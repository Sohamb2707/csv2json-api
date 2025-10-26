export function setByPath(obj, path, value) {
  const parts = path.split(".");
  let cur = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i].trim();
    if (!p) continue;
    if (typeof cur[p] !== "object" || cur[p] === null) cur[p] = {};
    cur = cur[p];
  }
  cur[parts[parts.length - 1].trim()] = value;
}

export function buildNestedObject(flatRecord) {
  const root = {};
  for (const [k, v] of Object.entries(flatRecord)) {
    if (k.includes(".")) setByPath(root, k, v);
    else root[k] = v;
  }
  return root;
}

