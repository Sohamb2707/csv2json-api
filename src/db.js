// import pkg from "pg";
// import dotenv from "dotenv";
// dotenv.config();
// const { Pool } = pkg;

// export const pool = new Pool({
//   host: process.env.PGHOST,
//   port: Number(process.env.PGPORT),
//   user: process.env.PGUSER,
//   password: process.env.PGPASSWORD,
//   database: process.env.PGDATABASE,
//   max: 10
// });

// export async function insertUsersBatch(rows) {
//   // rows: array of { name, age, address, additional_info }
//   if (!rows.length) return;

//   // build parameterized multi-row insert
//   const cols = ['"name"', "age", "address", "additional_info"];
//   const values = [];
//   const params = [];
//   let p = 1;

//   for (const r of rows) {
//     params.push(`($${p++}, $${p++}, $${p++}::jsonb, $${p++}::jsonb)`);
//     values.push(r.name);
//     values.push(r.age);
//     values.push(r.address ? JSON.stringify(r.address) : null);
//     values.push(r.additional_info ? JSON.stringify(r.additional_info) : null);
//   }

//   const sql = `INSERT INTO public.users (${cols.join(",")}) VALUES ${params.join(",")}`;
//   await pool.query(sql, values);
// }

import pkg from "pg";
import dotenv from "dotenv";
dotenv.config();

const { Pool } = pkg;

export const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT),
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  max: 10
});

export async function insertUsersBatch(rows) {
  if (!rows.length) return;
  const cols = ['"name"', "age", "address", "additional_info"];
  const values = [];
  const tuples = [];
  let p = 1;

  for (const r of rows) {
    tuples.push(`($${p++}, $${p++}, $${p++}::jsonb, $${p++}::jsonb)`);
    values.push(r.name);
    values.push(r.age);
    values.push(r.address ? JSON.stringify(r.address) : null);
    values.push(r.additional_info ? JSON.stringify(r.additional_info) : null);
  }

  const sql = `INSERT INTO public.users (${cols.join(",")}) VALUES ${tuples.join(",")}`;
  await pool.query(sql, values);
}
