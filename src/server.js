// import express from "express";
// import dotenv from "dotenv";
// dotenv.config();
// import { importCsvAndReport } from "./importer.js";
// import { pool } from "./db.js";

// const app = express();
// const PORT = Number(process.env.PORT || 3000);

// app.post("/import", async (req, res) => {
//   try {
//     const result = await importCsvAndReport();
//     res.json({ ok: true, imported: result.total, distribution: result.dist });
//   } catch (e) {
//     console.error(e);
//     res.status(500).json({ ok: false, error: String(e.message || e) });
//   }
// });

// app.get("/report", async (req, res) => {
//   try {
//     const dist = await (await import("./importer.js")).queryAgeDistribution?.();
//     // queryAgeDistribution is file-local; if you prefer, expose a dedicated function.
//     res.json(dist);
//   } catch (e) {
//     res.status(500).json({ error: String(e.message || e) });
//   } finally {
//     // do not pool.end() here; server keeps running
//   }
// });
// // in src/server.js, before app.listen(...)
// app.get("/", (_req, res) => {
//   res.type("html").send(`
//     <h1>CSV → JSON → Postgres API</h1>
//     <ul>
//       <li><code>POST /import</code> – import CSV from <code>CSV_PATH</code></li>
//       <li><code>GET /report</code> – age-group % distribution</li>
//     </ul>
//   `);
// });

// app.listen(PORT, () => {
//   console.log(`Server running on http://localhost:${PORT}`);
// });

import express from "express";
import dotenv from "dotenv";
dotenv.config();

import { importCsvAndReport, queryAgeDistribution } from "./importer.js";

const app  = express();
const PORT = Number(process.env.PORT || 3001);

app.get("/", (_req, res) => {
  res.type("html").send(`
    <h1>CSV → JSON → Postgres API</h1>
    <ul>
      <li><b>POST</b> /import – import CSV from <code>CSV_PATH</code></li>
      <li><b>GET</b> /report – age-group % distribution</li>
    </ul>
  `);
});

app.post("/import", async (_req, res) => {
  try {
    const result = await importCsvAndReport();
    res.json({ ok: true, imported: result.total, distribution: result.dist });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.get("/report", async (_req, res) => {
  try {
    const dist = await queryAgeDistribution();
    res.json(dist);
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
