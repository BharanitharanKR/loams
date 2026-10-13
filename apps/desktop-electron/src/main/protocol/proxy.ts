const PREFIXES = [
	"/durable/",
	"/api/",
	"/v1/",
	"/loams.",
	"/grpc.health.",
	"/.well-known/",
	"/health",
	"/ready",
];

export function isProxied(pathname: string): boolean {
	return PREFIXES.some((p) =>
		p.endsWith("/") || p.endsWith(".")
			? pathname.startsWith(p)
			: pathname === p || pathname.startsWith(`${p}/`),
	);
}

/** Hop-by-hop headers (RFC 9110 §7.6.1) never forwarded by a proxy. */
const HOP_BY_HOP = [
	"connection",
	"keep-alive",
	"proxy-authenticate",
	"proxy-authorization",
	"proxy-connection",
	"te",
	"trailer",
	"upgrade",
];

/**
 * `session.fetch` hands back a decoded body, so the upstream framing headers
 * (content-encoding/-length, transfer-encoding) would describe bytes we no
 * longer serve. Drop them with the hop-by-hop set, and whatever `Connection` names.
 */
function stripFraming(h: Headers): void {
	const named = (h.get("connection") ?? "")
		.split(",")
		.map((x) => x.trim().toLowerCase())
		.filter(Boolean);
	for (const k of [
		"content-encoding",
		"content-length",
		"transfer-encoding",
		...HOP_BY_HOP,
		...named,
	])
		h.delete(k);
	for (const k of [...h.keys()]) if (k.startsWith("proxy-")) h.delete(k);
}

const isHtml = (ct: string | null): boolean =>
	/^\s*(text\/html|application\/xhtml\+xml)\b/i.test(ct ?? "");

/**
 * Forwards a loams-app://console request to the active server's origin.
 * Redirects are returned as-is (never followed), cookie Domain attributes are
 * dropped so cookies bind to the loams-app host. The upstream CSP is kept; an
 * HTML response also gets `sandbox` (it would otherwise run in the console origin),
 * and every response gets `nosniff`.
 */
export async function proxyRequest(
	req: Request,
	target: string,
	fetchImpl: typeof fetch,
	/** Replaces the request path (the query string is kept). */
	pathname?: string,
): Promise<Response> {
	const src = new URL(req.url);
	const dest = new URL(target);
	const headers = new Headers(req.headers);
	headers.delete("origin");
	headers.delete("referer");
	headers.set("origin", dest.origin);
	const hasBody = req.method !== "GET" && req.method !== "HEAD";
	const out = new Request(
		`${dest.origin}${pathname ?? src.pathname}${src.search}`,
		{
			method: req.method,
			headers,
			body: hasBody ? req.body : undefined,
			redirect: "manual",
			duplex: "half",
		},
	);
	const res = await fetchImpl(out);
	const rh = new Headers(res.headers);
	stripFraming(rh);
	if (isHtml(rh.get("content-type")))
		rh.append("content-security-policy", "sandbox");
	rh.set("x-content-type-options", "nosniff");
	const cookies = res.headers.getSetCookie();
	if (cookies.length > 0) {
		rh.delete("set-cookie");
		for (const c of cookies)
			rh.append("set-cookie", c.replace(/;\s*Domain=[^;]*/gi, ""));
	}
	return new Response(res.body, {
		status: res.status,
		statusText: res.statusText,
		headers: rh,
	});
}

/** Request headers never forwarded to the daemon: the renderer's own identity
 * and credentials, and the hop-by-hop set. */
const AGENTD_DROPPED_REQUEST = [
	"origin",
	"referer",
	"cookie",
	"authorization",
	"host",
	...HOP_BY_HOP,
];

/** Response headers a renderer never gets from the daemon. */
const AGENTD_DROPPED_RESPONSE = [
	"set-cookie",
	"www-authenticate",
	"authorization",
];

/**
 * Forwards a loams-app://console/loams.agentd.v1.* request to the daemon's
 * loopback Connect listener (SD1 Task 5, §53 §6.2). It drops the renderer's
 * `Origin`, `Cookie`, `Referer` and `Authorization` (the listener refuses any
 * `Origin`), adds `Authorization: Bearer <token>`, which only main holds, and
 * returns the upstream body as the response body, so a server stream reaches
 * the renderer frame by frame. A response header that carries the token is
 * dropped, so the token cannot reach the renderer even if echoed.
 */
export async function proxyAgentdRequest(
	req: Request,
	target: string,
	/** `Bearer <token>`. */
	authorization: string,
	fetchImpl: typeof fetch,
): Promise<Response> {
	const src = new URL(req.url);
	const dest = new URL(target);
	const headers = new Headers(req.headers);
	for (const k of AGENTD_DROPPED_REQUEST) headers.delete(k);
	headers.set("authorization", authorization);
	const hasBody = req.method !== "GET" && req.method !== "HEAD";
	const res = await fetchImpl(
		new Request(`${dest.origin}${src.pathname}${src.search}`, {
			method: req.method,
			headers,
			body: hasBody ? req.body : undefined,
			redirect: "manual",
			duplex: "half",
		}),
	);
	const secret = authorization.replace(/^Bearer /i, "");
	const rh = new Headers(res.headers);
	stripFraming(rh);
	for (const k of AGENTD_DROPPED_RESPONSE) rh.delete(k);
	for (const [k, v] of [...rh.entries()])
		if (secret && v.includes(secret)) rh.delete(k);
	rh.set("x-content-type-options", "nosniff");
	return new Response(res.body, {
		status: res.status,
		statusText: res.statusText,
		headers: rh,
	});
}
