// Adapted from dataelement/dsh-desktop (MIT), src/main/runtime/harness-runtime.ts.
import { createServer } from "node:net";

function reserveOne(): Promise<{ port: number; close: () => Promise<void> }> {
	return new Promise((resolve, reject) => {
		const s = createServer();
		s.once("error", reject);
		s.listen(0, "127.0.0.1", () => {
			const a = s.address();
			if (!a || typeof a === "string") {
				s.close();
				reject(new Error("no port"));
				return;
			}
			resolve({
				port: a.port,
				close: () => new Promise<void>((r) => s.close(() => r())),
			});
		});
	});
}

/** n distinct free loopback ports. All are held open until every one is picked, then released. */
export async function reservePorts(n: number): Promise<number[]> {
	const held: Awaited<ReturnType<typeof reserveOne>>[] = [];
	try {
		for (let i = 0; i < n; i++) held.push(await reserveOne());
		return held.map((h) => h.port);
	} finally {
		await Promise.all(held.map((h) => h.close()));
	}
}

/**
 * The address Live's own listener binds when the engine is not given `--live-listen`
 * (`LIVE_LISTEN_DEFAULT` in crates/loams/src/main.rs).
 */
export const LIVE_DEFAULT_PORT = 7710;

/** Whether a loopback port can be bound right now. */
export function portFree(port: number): Promise<boolean> {
	return new Promise((resolve) => {
		const s = createServer();
		s.once("error", () => resolve(false));
		s.listen(port, "127.0.0.1", () => s.close(() => resolve(true)));
	});
}
