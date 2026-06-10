import { mkdir, writeFile } from 'node:fs/promises';

/** Writes the CommonJS package marker required because the root package is ESM. */
async function main() {
  await mkdir(new URL('../lib-cjs/', import.meta.url), { recursive: true });
  await writeFile(new URL('../lib-cjs/package.json', import.meta.url), `${JSON.stringify({ type: 'commonjs' }, null, 2)}\n`);
}

await main();
