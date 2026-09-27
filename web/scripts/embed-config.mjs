// Embed connectors/*.yaml, schemas/*/v*.yaml and the Python files the page quotes into
// src/sim/config.generated.ts so the browser port reads the shipped configuration and the
// shipped source byte for byte. `--check` exits 1 when the committed module no longer
// matches the files; `npm run embed` rewrites it.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..", "..");
const out = resolve(here, "..", "src", "sim", "config.generated.ts");

const yamlFiles = (dir) => readdirSync(dir).filter((f) => f.endsWith(".yaml")).sort();

const connectors = {};
for (const file of yamlFiles(join(root, "connectors"))) {
  connectors[file.replace(/\.yaml$/, "")] = readFileSync(join(root, "connectors", file), "utf8");
}
const schemas = {};
for (const name of readdirSync(join(root, "schemas")).sort()) {
  const dir = join(root, "schemas", name);
  const versions = {};
  for (const file of yamlFiles(dir)) {
    const match = /^v(\d+)\.yaml$/.exec(file);
    if (match) versions[Number(match[1])] = readFileSync(join(dir, file), "utf8");
  }
  schemas[name] = versions;
}

const SOURCE_FILES = ["conduit/adapters/base.py"];
const sources = {};
for (const file of SOURCE_FILES) sources[file] = readFileSync(join(root, file), "utf8");

const text = [
  "// Written by scripts/embed-config.mjs from connectors/*.yaml and schemas/*/v*.yaml.",
  "// Do not edit: run `npm run embed` after changing a YAML; the build checks the two match.",
  "",
  "/** connectors/<name>.yaml, byte for byte. */",
  `export const CONNECTOR_YAML: Record<string, string> = ${JSON.stringify(connectors, null, 2)};`,
  "",
  "/** schemas/<name>/v<N>.yaml, byte for byte, keyed by version. */",
  `export const SCHEMA_YAML: Record<string, Record<number, string>> = ${JSON.stringify(schemas, null, 2)};`,
  "",
  "/** Python files the page quotes, byte for byte, keyed by repository path. */",
  `export const SOURCE_PY: Record<string, string> = ${JSON.stringify(sources, null, 2)};`,
  "",
].join("\n");

if (process.argv.includes("--check")) {
  let current = "";
  try {
    current = readFileSync(out, "utf8");
  } catch {
    current = "";
  }
  if (current !== text) {
    console.error(`${out} does not match connectors/ and schemas/: run npm run embed`);
    process.exit(1);
  }
  console.log("config.generated.ts matches connectors/ and schemas/");
} else {
  writeFileSync(out, text);
  console.log(`wrote ${out}`);
}
