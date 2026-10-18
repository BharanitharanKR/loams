import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";
import { protocol, type Session } from "electron";
import type { EngineState, ServerEntry } from "../../shared/contracts";
import { isAgentdPath } from "../agentd/allowlist";
import type { AgentdTarget } from "../agentd/discover";
import { handleAgentdRequest } from "./agentd";
import { upstream } from "./inflight";
import { isProxied, proxyRequest } from "./proxy";
import { routeRequest } from "./route";
import { ensureCspMeta, resolveStatic } from "./static";

export const APP_SCHEME = "loams-app";

/**
 * Answers a request itself when the active server is the local engine, or
 * returns null. `forward` sends the request on to the engine, for an answer the
 * shim only amends (local-shim.ts).
 */
export type LocalShim = (
	pathname: string,
	req: Request,
	forward: () => Promise<Response>,
) => Promise<Response> | null;

export interface AppProtocolDeps {
	distRoot: string;
	activeServer: () => ServerEntry;
	/** The engine supervisor's state, for the durable and live listeners. */
	engineState: () => EngineState;
	/** Answers a few endpoints itself when the active server is the local engine. */
	localShim: LocalShim;
	/** The local daemon's Connect listener and token (SD1 Task 5), or null. */
	agentd: () => AgentdTarget | null;
}

/** Must run before app ready. */
export function registerAppScheme(): void {
	protocol.registerSchemesAsPrivileged([
		{
			scheme: APP_SCHEME,
			privileges: {
				standard: true,
				secure: true,
				supportFetchAPI: true,
				corsEnabled: false,
				stream: true,
			},
		},
	]);
}

const text = (status: number, body: string) =>
	new Response(body, {
		status,
		headers: { "content-type": "text/plain; charset=utf-8" },
	});

/** The console entry pages; each carries the console CSP as a `<meta>`. */
const CONSOLE_PAGES = new Set(["index.html", "cordis.html"]);

async function serveFile(file: string, mime: string): Promise<Response> {
	try {
		if (!(await stat(file)).isFile()) return text(404, "not found");
		const headers = {
			"content-type": mime,
			"cache-control": "no-cache",
			"x-content-type-options": "nosniff",
		};
		if (CONSOLE_PAGES.has(basename(file)))
			return new Response(ensureCspMeta(await readFile(file, "utf8")), {
				headers,
			});
		return new Response(new Uint8Array(await readFile(file)), { headers });
	} catch {
		return text(404, "not found");
	}
}

export function installAppProtocol(ses: Session, deps: AppProtocolDeps): void {
	ses.protocol.handle(APP_SCHEME, async (req) => {
		const url = new URL(req.url);
		if (url.hostname !== "console") return text(404, "not found");

		// Before the server routes: `/loams.` is otherwise the active server's.
		// Node's fetch, not `ses.fetch`: the daemon's loopback listener is plain
		// HTTP/1.1, where Chromium's network stack allows six connections per host,
		// and Studio keeps six Watch* server streams open. Through the session the
		// seventh request (AddRepo, a thread's turn, ...) queued behind them for
		// ever. The daemon needs no cookie or session state (they are dropped).
		if (isAgentdPath(url.pathname))
			return handleAgentdRequest(req, deps.agentd, (r) =>
				globalThis.fetch(r as Request),
			);

		if (isProxied(url.pathname)) {
			const server = deps.activeServer();
			const route = routeRequest(url.pathname, server, deps.engineState());
			if (route.kind === "reject")
				return new Response(
					JSON.stringify({
						code: route.body.code,
						message: route.body.message,
					}),
					{
						status: route.status,
						headers: { "content-type": "application/json" },
					},
				);
			const forward = () =>
				proxyRequest(
					req,
					route.target,
					(r) =>
						upstream.run((signal) => ses.fetch(r as Request, { signal })),
					route.pathname,
				);
			try {
				if (server.kind === "local") {
					const shimmed = deps.localShim(url.pathname, req, forward);
					if (shimmed) return await shimmed;
				}
				return await forward();
			} catch (e) {
				return new Response(
					JSON.stringify({
						code: "unreachable",
						message: `cannot reach ${server.name}: ${(e as Error).message}`,
					}),
					{ status: 502, headers: { "content-type": "application/json" } },
				);
			}
		}

		const r = resolveStatic(deps.distRoot, url);
		if (!r) return text(404, "not found");
		if ("spaFallback" in r)
			return serveFile(r.spaFallback, "text/html; charset=utf-8");
		return serveFile(r.file, r.mime);
	});
}
