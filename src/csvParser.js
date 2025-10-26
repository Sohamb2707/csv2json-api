// import { createReadStream } from "fs";
// import { EventEmitter } from "events";

// export function streamCsvRows(filePath) {
//   const emitter = new EventEmitter();
//   const stream = createReadStream(filePath, { encoding: "utf8" });

//   let buf = "";
//   let row = [];
//   let field = "";
//   let inQuotes = false;
//   let i = 0;

//   function pushField() {
//     row.push(field);
//     field = "";
//   }
//   function pushRow() {
//     // trim BOM on very first header cell if present
//     if (row.length && row[0].charCodeAt(0) === 0xfeff) {
//       row[0] = row[0].slice(1);
//     }
//     emitter.emit("row", row);
//     row = [];
//   }

//   stream.on("data", chunk => {
//     buf += chunk;
//     for (i = 0; i < buf.length; i++) {
//       const ch = buf[i];

//       if (inQuotes) {
//         if (ch === '"') {
//           const next = buf[i + 1];
//           if (next === '"') { // escaped quote
//             field += '"';
//             i++;
//           } else {
//             inQuotes = false;
//           }
//         } else {
//           field += ch;
//         }
//       } else {
//         if (ch === '"') {
//           inQuotes = true;
//         } else if (ch === ",") {
//           pushField();
//         } else if (ch === "\n") {
//           pushField(); pushRow();
//         } else if (ch === "\r") {
//           // ignore; handle \r\n by skipping \r
//         } else {
//           field += ch;
//         }
//       }
//     }
//     // keep remainder (possibly an unfinished field) in buf
//     buf = "";
//   });

//   stream.on("end", () => {
//     if (field.length > 0 || row.length > 0) {
//       pushField(); pushRow();
//     }
//     emitter.emit("end");
//   });

//   stream.on("error", (e) => emitter.emit("error", e));
//   return emitter;
// }

import { createReadStream } from "fs";
import { EventEmitter } from "events";

/**
 * Streams CSV rows as arrays of strings.
 * Supports: commas, quotes, escaped quotes (""), CR/LF newlines.
 * No third-party libs.
 */
export function streamCsvRows(filePath) {
  const emitter = new EventEmitter();
  const stream = createReadStream(filePath, { encoding: "utf8" });

  let buf = "";
  let row = [];
  let field = "";
  let inQuotes = false;

  emitter.pause  = () => stream.pause();
  emitter.resume = () => stream.resume();

  function pushField() { row.push(field); field = ""; }
  function pushRow() {
    if (row.length && row[0] && row[0].charCodeAt(0) === 0xfeff) {
      row[0] = row[0].slice(1); // strip BOM
    }
    emitter.emit("row", row);
    row = [];
  }

  stream.on("data", chunk => {
    buf += chunk;
    for (let i = 0; i < buf.length; i++) {
      const ch = buf[i];
      if (inQuotes) {
        if (ch === '"') {
          if (buf[i + 1] === '"') { field += '"'; i++; } // escaped quote
          else inQuotes = false;
        } else field += ch;
      } else {
        if (ch === '"') inQuotes = true;
        else if (ch === ",") pushField();
        else if (ch === "\n") { pushField(); pushRow(); }
        else if (ch === "\r") { /* ignore */ }
        else field += ch;
      }
    }
    buf = "";
  });

  stream.on("end", () => {
    if (field.length > 0 || row.length > 0) { pushField(); pushRow(); }
    emitter.emit("end");
  });

  stream.on("error", (e) => emitter.emit("error", e));
  return emitter;
}
