// import dotenv from "dotenv";
// dotenv.config();
// import { streamCsvRows } from "./csvParser.js";
// import { buildNestedObject } from "./dotJson.js";
// import { insertUsersBatch, pool } from "./db.js";
// import { createReadStream, existsSync } from "fs";

// const CSV_PATH = process.env.CSV_PATH;
// const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);

// function trimOrNull(s) {
//   if (s == null) return null;
//   const t = String(s).trim();
//   return t.length ? t : null;
// }

// export async function importCsvAndReport() {
//   if (!existsSync(CSV_PATH)) throw new Error(`CSV not found at ${CSV_PATH}`);

//   const csv = streamCsvRows(CSV_PATH);

//   let headers = null;
//   let batch = [];
//   let total = 0;

//   return new Promise((resolve, reject) => {
//     csv.on("row", async (arr) => {
//       if (!headers) {
//         headers = arr.map(h => h.trim());
//         return;
//       }

//       // Build flat map record
//       const flat = {};
//       for (let i = 0; i < headers.length; i++) {
//         const key = headers[i];
//         if (!key) continue;
//         flat[key] = trimOrNull(arr[i] ?? "");
//       }

//       // Validate mandatory
//       const firstName = flat["name.firstName"] || "";
//       const lastName  = flat["name.lastName"] || "";
//       const ageStr    = flat["age"] || "";
//       if (!firstName || !lastName || !ageStr) return; // skip invalid

//       // Build nested for everything
//       const nested = buildNestedObject(flat);

//       // Map to table columns
//       const fullName = `${firstName} ${lastName}`.trim();
//       const age = Number(ageStr);

//       // Extract address.* into address json
//       const address = nested.address || null;

//       // Remove mandatory + address subtree from additional
//       const additional = structuredClone(nested);
//       // delete mandatory
//       if (additional.name) {
//         if (additional.name.firstName) delete additional.name.firstName;
//         if (additional.name.lastName) delete additional.name.lastName;
//         if (Object.keys(additional.name).length === 0) delete additional.name;
//       }
//       delete additional.age;
//       delete additional.address;

//       const cleanedAdditional = Object.keys(additional).length ? additional : null;

//       batch.push({ name: fullName.replace(/\s+/g, " "), age, address, additional_info: cleanedAdditional });

//       if (batch.length >= BATCH_SIZE) {
//         csv.pause?.(); // in case underlying emitter supports pause
//         try {
//           await insertUsersBatch(batch);
//           total += batch.length;
//           batch = [];
//           csv.resume?.();
//         } catch (e) { reject(e); }
//       }
//     });

//     csv.on("end", async () => {
//       try {
//         if (batch.length) {
//           await insertUsersBatch(batch);
//           total += batch.length;
//         }
//         console.log(`Imported ${total} users.`);
//         const dist = await queryAgeDistribution();
//         printAgeReport(dist);
//         await pool.end();
//         resolve({ total, dist });
//       } catch (e) {
//         reject(e);
//       }
//     });

//     csv.on("error", (e) => reject(e));
//   });
// }

// async function queryAgeDistribution() {
//   // total and each bucket count
//   const sql = `
//     WITH t AS (
//       SELECT
//         CASE
//           WHEN age < 20 THEN '< 20'
//           WHEN age >= 20 AND age <= 40 THEN '20 to 40'
//           WHEN age > 40 AND age <= 60 THEN '40 to 60'
//           ELSE '> 60'
//         END AS bucket
//       FROM public.users
//     )
//     SELECT bucket, COUNT(*)::float AS cnt
//     FROM t
//     GROUP BY bucket
//   `;
//   const totalSql = `SELECT COUNT(*)::float AS total FROM public.users`;
//   const [rowsRes, totalRes] = await Promise.all([pool.query(sql), pool.query(totalSql)]);
//   const total = Number(totalRes.rows[0].total) || 0;
//   const out = { "< 20": 0, "20 to 40": 0, "40 to 60": 0, "> 60": 0 };
//   for (const r of rowsRes.rows) out[r.bucket] = Number(r.cnt) || 0;
//   // convert to %
//   for (const k of Object.keys(out)) {
//     out[k] = total ? Math.round((out[k] / total) * 100) : 0;
//   }
//   return out;
// }

// function printAgeReport(dist) {
//   console.log("Age-Group % Distribution");
//   console.log(`< 20     ${dist["< 20"]}`);
//   console.log(`20 to 40 ${dist["20 to 40"]}`);
//   console.log(`40 to 60 ${dist["40 to 60"]}`);
//   console.log(`> 60     ${dist["> 60"]}`);
// }

import dotenv from "dotenv";
dotenv.config();
import { streamCsvRows } from "./csvParser.js";
import { buildNestedObject } from "./dotJson.js";
import { insertUsersBatch, pool } from "./db.js";
import { existsSync } from "fs";

const CSV_PATH   = process.env.CSV_PATH;
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);

function trimOrNull(s) {
  if (s == null) return null;
  const t = String(s).trim();
  return t.length ? t : null;
}

export async function importCsvAndReport() {
  if (!existsSync(CSV_PATH)) throw new Error(`CSV not found at ${CSV_PATH}`);

  const csv = streamCsvRows(CSV_PATH);
  let headers = null;
  let batch = [];
  let total = 0;

  return new Promise((resolve, reject) => {
    csv.on("row", async (arr) => {
      if (!headers) { headers = arr.map(h => h.trim()); return; }

      const flat = {};
      for (let i = 0; i < headers.length; i++) {
        const key = headers[i];
        if (!key) continue;
        flat[key] = trimOrNull(arr[i] ?? "");
      }

      const firstName = flat["name.firstName"] || "";
      const lastName  = flat["name.lastName"] || "";
      const ageStr    = flat["age"] || "";
      if (!firstName || !lastName || !ageStr) return; // skip invalid

      const nested   = buildNestedObject(flat);
      const fullName = `${firstName} ${lastName}`.replace(/\s+/g, " ").trim();
      const age      = Number(ageStr);

      const address  = nested.address || null;

      // additional_info = everything except mandatory + address subtree
      const additional = structuredClone(nested);
      if (additional.name) {
        delete additional.name.firstName;
        delete additional.name.lastName;
        if (Object.keys(additional.name).length === 0) delete additional.name;
      }
      delete additional.age;
      delete additional.address;

      const cleanedAdditional = Object.keys(additional).length ? additional : null;

      batch.push({ name: fullName, age, address, additional_info: cleanedAdditional });

      if (batch.length >= BATCH_SIZE) {
        csv.pause();
        try {
          await insertUsersBatch(batch);
          total += batch.length;
          batch = [];
          csv.resume();
        } catch (e) { reject(e); }
      }
    });

    csv.on("end", async () => {
      try {
        if (batch.length) {
          await insertUsersBatch(batch);
          total += batch.length;
        }
        console.log(`Imported ${total} users.`);
        const dist = await queryAgeDistribution();
        printAgeReport(dist);
        await pool.end();
        resolve({ total, dist });
      } catch (e) { reject(e); }
    });

    csv.on("error", (e) => reject(e));
  });
}

export async function queryAgeDistribution() {
  const sql = `
    WITH t AS (
      SELECT CASE
        WHEN age < 20 THEN '< 20'
        WHEN age >= 20 AND age <= 40 THEN '20 to 40'
        WHEN age > 40 AND age <= 60 THEN '40 to 60'
        ELSE '> 60' END AS bucket
      FROM public.users
    )
    SELECT bucket, COUNT(*)::float AS cnt
    FROM t
    GROUP BY bucket
  `;
  const totalSql = `SELECT COUNT(*)::float AS total FROM public.users`;
  const [rowsRes, totalRes] = await Promise.all([
    pool.query(sql), pool.query(totalSql)
  ]);
  const total = Number(totalRes.rows[0].total) || 0;
  const out = { "< 20": 0, "20 to 40": 0, "40 to 60": 0, "> 60": 0 };
  for (const r of rowsRes.rows) out[r.bucket] = Number(r.cnt) || 0;
  for (const k of Object.keys(out)) out[k] = total ? Math.round((out[k] / total) * 100) : 0;
  return out;
}

function printAgeReport(dist) {
  console.log("Age-Group % Distribution");
  console.log(`< 20     ${dist["< 20"]}`);
  console.log(`20 to 40 ${dist["20 to 40"]}`);
  console.log(`40 to 60 ${dist["40 to 60"]}`);
  console.log(`> 60     ${dist["> 60"]}`);
}
