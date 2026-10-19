// loams-app://console/pg-control/* (PG2 R9.1): the renderer's way to the
// Postgres control plane. `loams pg-control` serves loams.postgres.v1 and its
// operations' reads on its own listener, not on the engine's API port, so the
// `/loams.` route to the active server cannot reach it. Main strips the
// prefix, drops the renderer's Origin, Cookie and Authorization, and adds the
// API key only main holds. Pure: no electron import.
import type { PgControlTarget } from "../pgcontrol/target";
import { proxyAgentdRequest } from "./proxy";

export const PG_CONTROL_PREFIX = "/pg-control/";

/**
 * Set on the answers main makes itself when pg-control cannot be reached, so
 * the page can tell "no control plane here" from a refusal pg-control sent.
 */
export const PG_CONTROL_STATUS_HEADER = "x-loams-pg-control";

const POSTGRES = "/loams.postgres.v1.PostgresService/";
/** pg-control serves only these of loams.operations.v1 (R9.1). */
const OPERATIONS = new Set([
	"/loams.operations.v1.OperationsService/GetOperation",
	"/loams.operations.v1.OperationsService/ListOperations",
]);

export function isPgControlPath(pathname: string): boolean {
	return pathname.startsWith(PG_CONTROL_PREFIX);
}

/** The upstream path for a renderer path, or null when it is not one of pg-control's. */
export function pgControlUpstreamPath(pathname: string): string | null {
	if (!isPgControlPath(pathname)) return null;
	const rest = pathname.slice(PG_CONTROL_PREFIX.length - 1);
	if (rest.startsWith(POSTGRES) && /^[A-Za-z]+$/.test(rest.slice(POSTGRES.length)))
		return rest;
	return OPERATIONS.has(rest) ? rest : null;
}

const connectError = (
	status: number,
	code: string,
	message: string,
	marker?: string,
): Response =>
	new Response(JSON.stringify({ code, message }), {
		status,
		headers: {
			"content-type": "application/json",
			...(marker ? { [PG_CONTROL_STATUS_HEADER]: marker } : {}),
		},
	});

export async function handlePgControlRequest(
	req: Request,
	target: () => PgControlTarget,
	fetchImpl: typeof fetch,
): Promise<Response> {
	const upstream = pgControlUpstreamPath(new URL(req.url).pathname);
	if (!upstream)
		return connectError(
			404,
			"unimplemented",
			"This is not a Postgres control plane method.",
		);
	const t = target();
	try {
		return await proxyAgentdRequest(
			req,
			t.url,
			t.apiKey ? `Bearer ${t.apiKey}` : "",
			fetchImpl,
			upstream,
		);
	} catch (e) {
		return connectError(
			503,
			"unavailable",
			`No Postgres control plane answered at ${t.url}: ${(e as Error).message}`,
			"unreachable",
		);
	}
}
