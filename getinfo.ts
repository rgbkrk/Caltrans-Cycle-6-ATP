import { getDocument, PDFDocumentProxy } from "pdfjs-dist";
import { TextItem } from "pdfjs-dist/types/src/display/api";

import { finished } from "stream";

import * as fs from "fs";

import { writeFile } from "fs/promises";

import * as arrow from "apache-arrow";

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
  applicationID: string;
  applicationNumber: string;
  implementingAgencyName: string;
  projectName: string;
  receivedDate: string;
};

function assignRecordFromArray(record: Array<any>): RawRecord {
  if (record.length != 5) {
    if (
      record[0] === "13." &&
      record[1] === "8-San Bernardino County Transportation Authority (SBCTA)-1"
    ) {
      return assignRecordFromArray([
        "13.",
        "8-San Bernardino County Transportation Authority (SBCTA)-1",
        "San Bernardino County Transportation Authority (SBCTA)",
        "San Bernardino County Safe Routes to Schools Phase III Program",
        "6/6/2022",
      ]);
    }
    console.log(record.length);
    console.log(record);
    throw Error(`Incorrect record length for ${record.join(", ")}`);
  }

  return {
    applicationID: record[0],
    applicationNumber: record[1],
    implementingAgencyName: record[2],
    projectName: record[3],
    receivedDate: record[4],
  };
}

/**
 * Pull out the records
 *
 * @param {import('pdfjs-dist').PDFDocumentProxy} doc
 */
async function extractRecords(doc: PDFDocumentProxy) {
  let records: string[][] = [];

  for (let pageNum = 1; pageNum <= doc.numPages; pageNum++) {
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
        if (record.length > 3) {
          records.push(record);
        }
        record = [];
      }
    });

    page.cleanup();
  }

  // All the records with empty fields that should have zeros instead. Thanks MS SQL /s
  // const manuallyFixedRecords = [].map(assignRecordFromArray);

  const cleanerRecords = records
    .map((record) => {
      return record.filter((field) => field != "" && field != " ");
    })
    .map((record) => {
      return assignRecordFromArray(record);
    })
    .filter((record) => {
      // Drop the header
      return record.applicationID !== "No.";
    });

  // Only keep the formalized object records. Arrays are tossed.
  return cleanerRecords.filter((record) => !Array.isArray(record));
}

type FrameRecord = {};

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
const pdfPath = process.argv[2] || "./AppsRecdToDate-20220628.pdf";
getDocument(pdfPath)
  .promise.then(async function (doc) {
    await printMetadata(doc);
    const records = await extractRecords(doc);

    writeFile(
      "./ATP-Cycle-6-Applications.json",
      JSON.stringify(records, null, 2)
    );
  })
  .catch((err) => {
    console.error(err);
  });
