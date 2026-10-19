// SHA256SUMS for the bundled binaries in resources/bin (0.1.0 release fix B9): what
// `loams-agentd install-runtime --from resources/bin` checks (crates/loams-agentd/src/runtime_install.rs)
// before it copies loams-agentd and loams into the per-user runtime directory.
//
// The sums must describe the binaries as they ship. Code signing rewrites them, so the file is written
// again after every signing step:
//   fetch-engine.mjs     after the copy and strip (Linux ships these bytes as they are);
//   mac-sign.cjs         macOS: after codesign signed the two binaries, before the app is sealed;
//   the release workflow Windows: after SignPath's Authenticode signatures, before the installer.
//
// CLI: node scripts/bin-sums.cjs <dir> [win32|darwin|linux]   (the platform defaults to this one)
const { createHash } = require("node:crypto");
const { existsSync, readFileSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");

/** The bundled binaries' file names on `platform`, the daemon first. */
function binaryNames(platform = process.platform) {
	const sfx = platform === "win32" ? ".exe" : "";
	return [`loams-agentd${sfx}`, `loams${sfx}`];
}

/**
 * The SHA256SUMS text for the binaries present in `dir` (sha256sum format, sorted by name), or null
 * when the daemon is not there: without it there is nothing for install-runtime to install.
 */
function binSums(dir, platform = process.platform) {
	const names = binaryNames(platform);
	if (!existsSync(join(dir, names[0]))) return null;
	return names
		.filter((n) => existsSync(join(dir, n)))
		.sort()
		.map((n) => `${createHash("sha256").update(readFileSync(join(dir, n))).digest("hex")}  ${n}\n`)
		.join("");
}

/** Writes `<dir>/SHA256SUMS`; returns its text, or null (and writes nothing) without the daemon. */
function writeBinSums(dir, platform = process.platform) {
	const text = binSums(dir, platform);
	if (text !== null) writeFileSync(join(dir, "SHA256SUMS"), text);
	return text;
}

module.exports = { binaryNames, binSums, writeBinSums };

if (require.main === module) {
	const [dir, platform] = process.argv.slice(2);
	if (!dir) {
		console.error("usage: node scripts/bin-sums.cjs <dir> [win32|darwin|linux]");
		process.exit(2);
	}
	const text = writeBinSums(dir, platform || process.platform);
	if (text === null) {
		console.error(`${dir}: no ${binaryNames(platform || process.platform)[0]}; SHA256SUMS not written`);
		process.exit(1);
	}
	process.stdout.write(text);
}
