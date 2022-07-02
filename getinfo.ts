// Run `gulp dist-install` to generate 'pdfjs-dist' npm package files.
// const pdfjsLib = require("pdfjs-dist/legacy/build/pdf.js");

import { getDocument, PDFDocumentProxy } from "pdfjs-dist";
import { TextItem } from "pdfjs-dist/types/src/display/api";

import { finished } from "stream";

import * as fs from "fs";

import * as arrow from "apache-arrow";
// The arrow docs are fairly sparse generated TypeScript documentation.
// The best documentation to dive into is https://loaders.gl/arrowjs/docs/api-reference/record-batch-writer

const validMethods = ["Election Day", "Vote by Mail", "Total"];

const PRECINCT_COUNTY_DIGIT = 0;
const PRECINCT_CITY_DIGIT = 1;

const digitToCity: { [key: string]: City } = {
  "0": "Unincorporated",
  "1": "Santa Cruz",
  "2": "Capitola",
  "3": "Watsonville",
  "4": "Scotts Valley",
};

type City =
  | "Unincorporated"
  | "Unincorporated D1"
  | "Unincorporated D2"
  | "Unincorporated D3"
  | "Unincorporated D4"
  | "Unincorporated D5"
  | "Santa Cruz"
  | "Capitola"
  | "Watsonville"
  | "Scotts Valley"
  // Now for unicorporated but known areas
  | "Bonny Doon";

type District = "D1" | "D2" | "D3" | "D4" | "D5";

function assignCity(precinct: string): City {
  if (
    !precinct ||
    precinct.length < 1 ||
    !Object.keys(digitToCity).includes(precinct[PRECINCT_CITY_DIGIT])
  ) {
    throw new Error("Invalid precinct");
  }

  /*
  switch (precinct) {
    case "30030":
    case "30041":
    case "30042":
      return "Bonny Doon";
  }*/

  const city = digitToCity[precinct[PRECINCT_CITY_DIGIT]];
  if (city === "Unincorporated") {
    switch (precinct[PRECINCT_COUNTY_DIGIT]) {
      case "1":
        return "Unincorporated D1";
      case "2":
        return "Unincorporated D2";
      case "3":
        return "Unincorporated D3";
      case "4":
        return "Unincorporated D4";
      case "5":
        return "Unincorporated D5";
      default:
        return "Unincorporated";
    }
  } else {
    return city;
  }
}

function assignDistrict(precinct: string): District {
  if (!["1", "2", "3", "4", "5"].includes(precinct[PRECINCT_COUNTY_DIGIT])) {
    throw new Error(`Invalid district code in precinct: ${precinct}`);
  }
  return `D${precinct[PRECINCT_COUNTY_DIGIT]}` as District;
}

/**
 * Print out metadata about the PDF
 * @param doc
 */
async function printMetadata(doc: PDFDocumentProxy) {
  const numPages = doc.numPages;
  console.log("# Document Loaded");
  console.log("Number of Pages: " + numPages);
  console.log();

  const data = await doc.getMetadata();

  console.log("# Metadata Is Loaded");
  console.log("## Info");
  console.log(JSON.stringify(data.info, null, 2));
  console.log();
}

/**
 * A raw record is a single entry from the PDF before we post-process
 */
type RawRecord = {
  Precinct: string; // Categorical so we don't need to keep it as an integer
  City: City;
  District: District;
  Method: "Vote by Mail" | "Total" | "Election Day";
  "Registered Voters": number;
  "Ballots Cast": number;
  // irrelevant since it's the same as (BallotsCast / RegisteredVoters)
  // Turnout: string;
  "Measure C - YES": number;
  "Measure C - NO": number;
  "Measure D - YES": number;
  "Measure D - NO": number;
};

function assignRecordFromArray(record: Array<any>): RawRecord {
  if (record.length !== 9) {
    throw "Record incorrect length";
  }

  return {
    Precinct: record[0],
    City: assignCity(record[0]),
    District: assignDistrict(record[0]),
    Method: record[1],
    "Registered Voters": parseInt(record[2]),
    "Ballots Cast": parseInt(record[3]),
    // Turnout: record[4], irrelevant since it's the same as (BallotsCast / RegisteredVoters)
    "Measure C - YES": parseInt(record[5]),
    "Measure C - NO": parseInt(record[6]),
    "Measure D - YES": parseInt(record[7]),
    "Measure D - NO": parseInt(record[8]),
  };
}

/**
 * Pull out the records
 *
 * @param {import('pdfjs-dist').PDFDocumentProxy} doc
 */
async function extractRecords(doc: PDFDocumentProxy) {
  let records: string[][] = [];

  for (let pageNum = 131; pageNum <= 136; pageNum++) {
    const page = await doc.getPage(pageNum);
    // console.log("# Page " + pageNum);

    const content = await page.getTextContent();

    let record: string[] = [];

    content.items.forEach(function (item) {
      if (!item.hasOwnProperty("str")) {
        return;
      }

      const textItem = item as TextItem;

      record.push(textItem.str);
      if (textItem.hasEOL) {
        // Only append the record if it leads with a quintuple digit entry (the precinct)
        if (record.length > 2 && record[0].match(/\d\d\d\d\d/)) {
          records.push(record);
        }
        record = [];
      }
    });

    page.cleanup();
  }

  // All the records with empty fields that should have zeros instead. Thanks MS SQL /s
  const manuallyFixedRecords = [
    ["20470", "Vote by Mail", "41", "5", "12.20 %", "5", "0", "4", "1"],
    ["20470", "Total", "41", "5", "12.20 %", "5", "0", "4", "1"],
    ["23410", "Election Day", "193", "2", "1.04 %", "2", "0", "0", "2"],
    ["23660", "Election Day", "1", "1", "100.00 %", "1", "0", "1", "0"],
    ["23660", "Total", "1", "1", "100.00 %", "1", "0", "1", "0"],

    ["40010", "Election Day", "3", "1", "33.33 %", "0", "1", "0", "1"],
    ["40010", "Vote by Mail", "3", "1", "33.33 %", "1", "0", "1", "0"],
    ["40270", "Election Day", "16", "1", "6.25 %", "1", "0", "0", "1"],
    ["43510", "Election Day", "1701", "7", "0.41 %", "5", "1", "0", "6"],
    ["50111", "Election Day", "672", "13", "1.93 %", "8", "5", "0", "13"],
  ].map(assignRecordFromArray);

  const cleanerRecords = records
    .map((record) => {
      return record.filter((field) => field != "" && field != " ");
    })
    .map((record) => {
      if (record.length === 9) {
        return assignRecordFromArray(record);
      }

      if (!validMethods.includes(record[1])) {
        // super bad
        return record;
      }

      // We likely have a precinct that hasn't been collected
      if (record.length === 4 && record[2] === "0" && record[3] === "0") {
        return assignRecordFromArray([
          record[0],
          record[1],
          0,
          0,
          "0.00 %",
          NaN,
          NaN,
          NaN,
          NaN,
        ]);
      }

      // No one voted here
      if (record.length === 5 && record[4] === "0.00 %") {
        return assignRecordFromArray([
          record[0],
          record[1],
          record[2],
          record[3],
          "0.00 %",
          0,
          0,
          0,
          0,
        ]);
      }

      const fixedRecord = manuallyFixedRecords.find((fixedRecord) => {
        return (
          record[0] === fixedRecord.Precinct &&
          record[1] === fixedRecord.Method &&
          parseInt(record[2]) === fixedRecord["Registered Voters"] &&
          // If the record has updated since we last checked, this is the field that will likely change
          parseInt(record[3]) === fixedRecord["Ballots Cast"]
        );
      });

      if (fixedRecord) {
        return fixedRecord;
      }

      console.warn("record not cleaned up::::", record);

      return record;
    });

  // Only keep the formalized object records. Arrays are tossed.
  return cleanerRecords.filter((record) => !Array.isArray(record));
}

type FrameRecord = {
  Precinct: string;
  City: City;
  District: District;
  "Registered Voters": number;
  "Ballots Cast": number;
  "YES - Vote by Mail": number;
  "YES - Election Day": number;
  "YES - Total": number;
  "NO - Vote by Mail": number;
  "NO - Election Day": number;
  "NO - Total": number;
  "% of NO": number;
  "% of YES": number;
};

type Frame = {
  [key: string]: FrameRecord;
};

async function writeArrowIPC(records: Frame, filename: string) {
  // Write records out to arrow IPC format
  const vector = arrow.vectorFromArray(Object.values(records));
  const batch = new arrow.RecordBatch(
    new arrow.Schema(vector.type.children),
    vector.data[0]
  );

  const table = new arrow.Table(batch);

  const writer = arrow.RecordBatchFileWriter.writeAll(table);
  const result = writer.pipe(fs.createWriteStream(filename));

  await new Promise((resolve) =>
    finished(result, () => {
      writer.close();
      resolve("done");
    })
  );
}

// Loading file from file system into typed array
const pdfPath = process.argv[2] || "./District Canvass - 6-21-22.pdf";
getDocument(pdfPath)
  .promise.then(async function (doc) {
    // printMetadata(doc);
    const records = await extractRecords(doc);

    const measureDRecords: Frame = {};
    const measureCRecords: Frame = {};

    // First pass is breaking up Measure C and Measure D into separate Data Frames
    records.forEach((record) => {
      // Not a parsed record
      if (Array.isArray(record)) {
        return;
      }

      const splitDRecord: Partial<FrameRecord> = {
        Precinct: record.Precinct,
        "Registered Voters": record["Registered Voters"],
        "Ballots Cast": record["Ballots Cast"],
        City: record["City"],
        District: record["District"],
      };

      const splitCRecord: Partial<FrameRecord> = {
        Precinct: record.Precinct,
        "Registered Voters": record["Registered Voters"],
        "Ballots Cast": record["Ballots Cast"],
        City: record["City"],
        District: record["District"],
      };

      switch (record.Method) {
        case "Election Day":
          splitDRecord["YES - Election Day"] = record["Measure D - YES"];
          splitDRecord["NO - Election Day"] = record["Measure D - NO"];
          splitCRecord["YES - Election Day"] = record["Measure C - YES"];
          splitCRecord["NO - Election Day"] = record["Measure C - NO"];
          break;

        case "Vote by Mail":
          splitDRecord["YES - Vote by Mail"] = record["Measure D - YES"];
          splitCRecord["YES - Vote by Mail"] = record["Measure C - YES"];
          splitDRecord["NO - Vote by Mail"] = record["Measure D - NO"];
          splitCRecord["NO - Vote by Mail"] = record["Measure C - NO"];
          break;
        case "Total":
          splitDRecord["YES - Total"] = record["Measure D - YES"];
          splitCRecord["YES - Total"] = record["Measure C - YES"];
          splitDRecord["NO - Total"] = record["Measure D - NO"];
          splitCRecord["NO - Total"] = record["Measure C - NO"];
          break;
      }

      measureDRecords[record.Precinct] = Object.assign(
        {},
        measureDRecords[record.Precinct],
        splitDRecord
      );

      measureCRecords[record.Precinct] = Object.assign(
        {},
        measureCRecords[record.Precinct],
        splitCRecord
      );
    });

    for (let precinct in measureDRecords) {
      let record = measureDRecords[precinct];
      record["% of NO"] =
        record["NO - Total"] / (record["NO - Total"] + record["YES - Total"]);
      record["% of YES"] =
        record["YES - Total"] / (record["NO - Total"] + record["YES - Total"]);
    }

    for (let precinct in measureCRecords) {
      let record = measureCRecords[precinct];
      record["% of NO"] =
        record["NO - Total"] / (record["NO - Total"] + record["YES - Total"]);
      record["% of YES"] =
        record["YES - Total"] / (record["NO - Total"] + record["YES - Total"]);
    }

    await Promise.all([
      writeArrowIPC(measureCRecords, "measure-c.arrow"),
      writeArrowIPC(measureDRecords, "measure-d.arrow"),
    ]);
  })
  .catch((err) => {
    console.error(err);
  });
