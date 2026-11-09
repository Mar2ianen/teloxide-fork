// Run from a widget-sandbox v0.1.1 checkout with its dependencies installed:
// pnpm exec tsx /path/to/teloxide/docs/bot-api-10.4/check-widget-sandbox.mts
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { deflateSync } from 'node:zlib';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const html = fs.readFileSync(path.join(root, 'crates/teloxide/examples/widgets/status.html'), 'utf8');
assert(Buffer.byteLength(html) <= 32768);
assert(deflateSync(html, { level: 9 }).byteLength <= 4096);
const { runWidget } = await import(pathToFileURL(path.resolve('packages/core/scripts/sandbox-node.ts')).href);
for (const theme of ['light', 'dark']) {
  const result = await runWidget(html, {
    theme,
    probes: ["(document.getElementById('refresh').click(), document.getElementById('status').textContent)"],
  });
  assert.deepEqual(result.diags, []);
  assert(!result.logs.some((entry: { level: string }) => entry.level === 'error'));
  assert(result.renderOps.some((op: { op: string }) => op.op === 'el'));
  assert(result.actions.some((action: { kind: string }) => action.kind === 'ready'));
  // SDK v1 predates Bot API widget queries: verify the explicit upgrade path.
  assert.equal(result.probes[0], 'Please update Telegram to use widget requests.');
  console.log(`${theme}: HTML executes, validated render ops, ready, request fallback OK`);
}
console.log(`HTML: ${Buffer.byteLength(html)} bytes; zlib level 9: ${deflateSync(html, { level: 9 }).byteLength} bytes`);
