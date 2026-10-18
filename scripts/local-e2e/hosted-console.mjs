// Serves the staged hosted console (web/apps/console/dist-cloudflare, from
// `LOAMS_ENGINE_URL=http://127.0.0.1:18080 node deploy/stage.mjs`) the way its
// Cloudflare deploy does: files with their MIME types and _headers, anything
// else answered by the SPA fallback (index.html). This is the web edition the
// owner's users get (console.loams.dev), pointed at the local engine.
//
//   node scripts/local-e2e/hosted-console.mjs [dir] [port]
import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize } from 'node:path';

const dir = process.argv[2] ?? new URL('../../web/apps/console/dist-cloudflare', import.meta.url).pathname;
const port = Number(process.argv[3] ?? 5192);
const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.mjs': 'text/javascript',
  '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml', '.png': 'image/png',
  '.woff2': 'font/woff2', '.map': 'application/json', '.wasm': 'application/wasm', '.txt': 'text/plain' };
createServer((req, res) => {
  const path = normalize(decodeURIComponent(new URL(req.url, 'http://x').pathname)).replace(/^(\.\.[/\\])+/, '');
  let file = join(dir, path);
  if (existsSync(file) && statSync(file).isDirectory()) file = join(file, 'index.html');
  if (!existsSync(file)) file = join(dir, 'index.html');
  res.writeHead(200, { 'content-type': types[extname(file)] ?? 'application/octet-stream',
    'x-content-type-options': 'nosniff', 'cache-control': 'no-store' });
  createReadStream(file).pipe(res);
}).listen(port, '127.0.0.1', () => console.log(`hosted console: http://127.0.0.1:${port}/ (${dir})`));
