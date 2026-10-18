// electron-builder custom macOS signer (`mac.sign` in electron-builder.config.cjs), 0.1.0 release fix B9.
//
// The app bundles loams-agentd and loams in Contents/Resources/bin with SHA256SUMS, which
// `loams-agentd install-runtime` checks before it copies them into the per-user runtime. Signing
// rewrites both binaries (an ad-hoc signature too), so the sums written by fetch-engine.mjs would no
// longer match and turning background agents on would fail. So:
//   1. sign the whole app as electron-builder would (@electron/osx-sign, through app-builder-lib's
//      wrapper with its retries): every Mach-O, the two binaries included, with the same identity,
//      hardened runtime, entitlements and timestamp;
//   2. write SHA256SUMS again from the signed binaries;
//   3. sign the app bundle alone once more, which reseals its resources over the new SHA256SUMS
//      (nested code keeps the signatures from step 1).
// electron-builder notarizes after this function returns, so notarization sees the final bundle.
const { createRequire } = require("node:module");
const { existsSync } = require("node:fs");
const { join } = require("node:path");
const { writeBinSums } = require("./bin-sums.cjs");

/** app-builder-lib's `sign` (osx-sign with retries), resolved through electron-builder. */
function builderSign() {
	const fromBuilder = createRequire(require.resolve("electron-builder"));
	const fromLib = createRequire(fromBuilder.resolve("app-builder-lib/package.json"));
	return fromLib("./out/codeSign/macCodeSign.js").sign;
}

/** The options of the second pass: only the app bundle itself is signed (exported for tests). */
function resealOptions(opts) {
	return { ...opts, binaries: undefined, ignore: (file) => file !== opts.app };
}

/** The bundle's resources/bin directory (exported for tests). */
function binDir(app) {
	return join(app, "Contents", "Resources", "bin");
}

/**
 * The three passes with `sign` as the signer (exported for tests). electron-builder calls the
 * exported function as `(opts, packager)`, so the signer cannot be its second parameter.
 * @param {import('@electron/osx-sign').SignOptions} opts what electron-builder would sign with
 * @param {(o: object) => Promise<void>} sign
 */
async function signWith(opts, sign) {
	await sign(opts);
	const dir = binDir(opts.app);
	if (!existsSync(dir)) return;
	const sums = writeBinSums(dir, "darwin");
	if (sums === null) return; // no daemon bundled: no SHA256SUMS to keep in step
	console.log(`mac-sign: SHA256SUMS of the signed binaries\n${sums}`);
	await sign(resealOptions(opts));
}

/** electron-builder's `mac.sign` hook: `(opts, packager)`; the packager is not used. */
async function macSign(opts) {
	await signWith(opts, builderSign());
}

module.exports = macSign;
module.exports.default = macSign;
module.exports.macSign = macSign;
module.exports.signWith = signWith;
module.exports.resealOptions = resealOptions;
module.exports.binDir = binDir;
