// Slice a class out of an embedded Python file so the page can quote the shipped source
// verbatim instead of carrying a hand-written paraphrase of it.

/**
 * The `class <name>` statement and its body, up to the next top-level statement.
 * Throws when the class is absent, so a rename in the Python breaks the page loudly
 * rather than silently showing nothing.
 */
export function pythonClass(source: string, name: string): string {
  const lines = source.split("\n");
  const start = lines.findIndex((line) => new RegExp(`^class ${name}\\b`).test(line));
  if (start === -1) throw new Error(`class ${name} is not in the embedded source`);
  let end = lines.length;
  for (let i = start + 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (line.trim() !== "" && !/^[ \t]/.test(line)) {
      end = i;
      break;
    }
  }
  return lines.slice(start, end).join("\n").replace(/\s+$/, "");
}
