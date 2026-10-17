// The web edition's Studio needs an access token for `loams-agentd serve
// --remote`. A release daemon takes them from a real issuer; for local testing
// a debug daemon takes them from a local JWKS file (LOAMS_AGENTD_TEST_ISSUER,
// as web/apps/console/test/e2e/fixtures/agentd.ts does). This writes
// <dir>/jwks.json (once) and <dir>/token: an EdDSA JWT for subject local-e2e,
// audience loams-agentd, valid 7 days.
//
//   node scripts/local-e2e/agentd-token.mjs DIR
import { createPrivateKey, generateKeyPairSync, sign } from 'node:crypto';
import { chmodSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2];
if (!dir) throw new Error('usage: agentd-token.mjs DIR');
const keyFile = join(dir, 'issuer-key.pem');
const kid = 'local-e2e-1';
if (!existsSync(keyFile)) {
  const { privateKey, publicKey } = generateKeyPairSync('ed25519');
  writeFileSync(keyFile, privateKey.export({ format: 'pem', type: 'pkcs8' }), { mode: 0o600 });
  const jwk = publicKey.export({ format: 'jwk' });
  writeFileSync(join(dir, 'jwks.json'), JSON.stringify({ keys: [{ ...jwk, kid, alg: 'EdDSA', use: 'sig' }] }));
}
const key = createPrivateKey(readFileSync(keyFile));
const b64 = (o) => Buffer.from(typeof o === 'string' ? o : JSON.stringify(o)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const head = b64({ alg: 'EdDSA', typ: 'JWT', kid });
const body = b64({
  iss: 'http://127.0.0.1:9/application/o/loams-e2e/',
  aud: 'loams-agentd',
  sub: 'local-e2e',
  iat: now,
  nbf: now - 5,
  exp: now + 7 * 24 * 3600,
});
const sig = sign(null, Buffer.from(`${head}.${body}`), key).toString('base64url');
writeFileSync(join(dir, 'token'), `${head}.${body}.${sig}\n`, { mode: 0o600 });
chmodSync(join(dir, 'token'), 0o600);
