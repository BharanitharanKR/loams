// The factory parity fixture (SF1 amendment F1; design §50 §13): each TypeScript op is run
// through its real adapter against a scripted upstream, and what it sent and returned is
// recorded in crates/loams-agentd-factory/tests/fixtures/parity.json. The daemon's Rust port
// replays the same upstream and must send the same requests and return the same values.
//
// `LOAMS_UPDATE_FACTORY_PARITY=1 pnpm exec vitest run test/factory-parity.test.ts` writes it;
// otherwise this test fails when the committed fixture is stale.

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { Context } from "cordis";
import { afterEach, expect, it, vi } from "vitest";
import type { FactoryAppId } from "../src/shared/contracts";
import { FACTORY_APPS } from "../src/main/factory/apps";
import { classifyError } from "../src/main/factory/host";
import { OPS, ParamError, UnsupportedError } from "../src/main/factory/ops";

const FIXTURE = resolve(
	__dirname,
	"../../../crates/loams-agentd-factory/tests/fixtures/parity.json",
);
/** Every case runs at this instant (Langfuse's daily window is relative to now). */
const NOW = "2026-10-19T12:00:00.000Z";

interface Route {
	method?: string;
	/** The path (no query) this answers. */
	path: string;
	status?: number;
	body?: unknown;
	/** Reply with a network error instead (fetch rejects). */
	network?: boolean;
	/** Response headers. */
	headers?: Record<string, string>;
}
interface Case {
	name: string;
	app: FactoryAppId;
	url: string;
	fields: Record<string, string>;
	op: string;
	params: Record<string, unknown>;
	routes: Route[];
}
interface Sent {
	method: string;
	path: string;
	query: [string, string][];
	headers: string[];
	body?: unknown;
}

const iso = (d: string) => `${d}T10:00:00Z`;
const ok = (body: unknown, extra: Partial<Route> = {}) => ({ body, ...extra });

const FORGEJO = { url: "http://forgejo.test", fields: { token: "tok-F" } };
const ZULIP = {
	url: "http://zulip.test",
	fields: { email: "bot@example.com", apiKey: "key-Z" },
};
const PLANE = {
	url: "http://plan-api.test",
	fields: { apiKey: "itp_key", projectKey: "FAC" },
};
const GLITCHTIP = { url: "http://gt.test", fields: { token: "tok-G" } };
const OPENPANEL = {
	url: "http://op.test",
	fields: {
		clientId: "0195f2ae-7c1a-4c2b-9f3d-4e5a6b7c8d9e",
		clientSecret: "sec-O",
		projectId: "shop",
	},
};
const MATOMO = {
	url: "http://matomo.test",
	fields: { apiToken: "tok-M", idSite: "1" },
};
const LANGFUSE = {
	url: "http://lf.test",
	fields: { publicKey: "pk-lf-1", secretKey: "sk-lf-1" },
};

const repo = {
	full_name: "sfux/shop",
	description: "Demo shop",
	stars_count: 1,
	forks_count: 0,
	open_issues_count: 2,
	updated_at: iso("2026-10-18"),
	html_url: "http://forgejo.test/sfux/shop",
};
const fIssue = (n: number, extra: Record<string, unknown> = {}) => ({
	id: 100 + n,
	number: n,
	title: `Issue ${n}`,
	state: "open",
	html_url: `http://forgejo.test/sfux/shop/issues/${n}`,
	updated_at: iso("2026-10-17"),
	...extra,
});
const zMsg = (id: number) => ({
	id,
	sender_full_name: "Sfux Dev",
	timestamp: 1_792_000_000 + id,
	subject: "checkout",
	content: `<p>message ${id}</p>`,
});
const gtIssue = (id: string, status = "unresolved") => ({
	id,
	title: "ZeroDivisionError: division by zero",
	level: "error",
	count: "3",
	lastSeen: iso("2026-10-18"),
	permalink: `http://gt.test/factory/issues/${id}`,
	status,
});

const CASES: Case[] = [
	// Forgejo
	{
		name: "forgejo.version",
		...FORGEJO,
		app: "forgejo",
		op: "version",
		params: {},
		routes: [{ path: "/api/v1/version", ...ok({ version: "16.0.5" }) }],
	},
	{
		name: "forgejo.health",
		...FORGEJO,
		app: "forgejo",
		op: "health",
		params: {},
		routes: [{ path: "/api/v1/version", ...ok({ version: "16.0.5" }) }],
	},
	{
		name: "forgejo.health.auth_failed",
		...FORGEJO,
		app: "forgejo",
		op: "health",
		params: {},
		routes: [
			{
				path: "/api/v1/version",
				status: 401,
				body: { message: "token is required" },
			},
		],
	},
	{
		name: "forgejo.repos",
		...FORGEJO,
		app: "forgejo",
		op: "repos",
		params: { limit: 20 },
		routes: [
			{ path: "/api/v1/repos/search", ...ok({ ok: true, data: [repo] }) },
		],
	},
	{
		name: "forgejo.repos.query_page_and_clamp",
		...FORGEJO,
		app: "forgejo",
		op: "repos",
		params: { q: "shop & co", page: 2, limit: 500 },
		routes: [{ path: "/api/v1/repos/search", ...ok({ ok: true, data: [] }) }],
	},
	{
		name: "forgejo.issues",
		...FORGEJO,
		app: "forgejo",
		op: "issues",
		params: { limit: 20 },
		routes: [
			{ path: "/api/v1/repos/issues/search", ...ok([fIssue(1), fIssue(3)]) },
		],
	},
	{
		name: "forgejo.pulls",
		...FORGEJO,
		app: "forgejo",
		op: "issues",
		params: { type: "pulls", owner: "sfux", repo: "shop", limit: 50 },
		routes: [
			{
				path: "/api/v1/repos/sfux/shop/pulls",
				...ok([fIssue(2, { head: { ref: "fac-42-fix" } })]),
			},
		],
	},
	{
		name: "forgejo.pulls.needs_owner",
		...FORGEJO,
		app: "forgejo",
		op: "issues",
		params: { type: "pulls" },
		routes: [],
	},
	{
		name: "forgejo.repos.upstream_error",
		...FORGEJO,
		app: "forgejo",
		op: "repos",
		params: {},
		routes: [
			{ path: "/api/v1/repos/search", status: 500, body: { message: "boom" } },
		],
	},
	{
		name: "forgejo.repos.unreachable",
		...FORGEJO,
		app: "forgejo",
		op: "repos",
		params: {},
		routes: [{ path: "/api/v1/repos/search", network: true }],
	},
	{
		name: "forgejo.unknown_param",
		...FORGEJO,
		app: "forgejo",
		op: "repos",
		params: { evil: 1 },
		routes: [],
	},
	// Zulip
	{
		name: "zulip.streams",
		...ZULIP,
		app: "zulip",
		op: "streams",
		params: {},
		routes: [
			{
				path: "/api/v1/streams",
				...ok({
					result: "success",
					msg: "",
					streams: [
						{
							stream_id: 9,
							name: "factory",
							description: "Factory signals",
							invite_only: false,
						},
						{ stream_id: 10, name: "ops", description: "", invite_only: true },
					],
				}),
			},
		],
	},
	{
		name: "zulip.messages",
		...ZULIP,
		app: "zulip",
		op: "messages",
		params: { channel: "factory", limit: 20 },
		routes: [
			{
				path: "/api/v1/messages",
				...ok({
					result: "success",
					msg: "",
					anchor: 3,
					found_newest: true,
					found_oldest: true,
					found_anchor: false,
					history_limited: false,
					messages: [zMsg(1), zMsg(2)],
				}),
			},
		],
	},
	{
		name: "zulip.server",
		...ZULIP,
		app: "zulip",
		op: "server",
		params: {},
		routes: [
			{
				path: "/api/v1/users/me",
				...ok({
					result: "success",
					msg: "",
					full_name: "Sfux Dev",
					email: "user8@example.com",
					user_id: 8,
				}),
			},
		],
	},
	{
		name: "zulip.health.auth_failed",
		...ZULIP,
		app: "zulip",
		op: "health",
		params: {},
		routes: [
			{
				path: "/api/v1/users/me",
				status: 401,
				body: {
					result: "error",
					msg: "Invalid API key",
					code: "INVALID_API_KEY",
				},
			},
		],
	},
	// Plane (ItsAPlan)
	{
		name: "plane.stats",
		...PLANE,
		app: "plane",
		op: "stats",
		params: {},
		routes: [
			{
				path: "/projects/FAC/analytics/stats",
				...ok({
					open: 2,
					inProgress: 1,
					backlog: 0,
					overdue: 0,
					unassigned: 2,
					closedLast7d: 0,
				}),
			},
		],
	},
	{
		name: "plane.issues",
		...PLANE,
		app: "plane",
		op: "issues",
		params: { limit: 20 },
		routes: [
			{
				path: "/projects/FAC/issues",
				...ok([
					{
						id: 2,
						sequenceNumber: 2,
						identifier: "FAC-2",
						title: "Add retry to webhook sender",
						columnId: 3,
						typeId: null,
						priority: "medium",
						labelIds: [],
						archived: false,
					},
				]),
			},
		],
	},
	{
		name: "plane.health",
		...PLANE,
		app: "plane",
		op: "health",
		params: {},
		routes: [
			{ path: "/", ...ok({ name: "It's a Plan api", status: "ok" }) },
			{ path: "/me", ...ok({ authenticated: true, user: { id: "u1" } }) },
		],
	},
	{
		name: "plane.health.not_authenticated",
		...PLANE,
		app: "plane",
		op: "health",
		params: {},
		routes: [
			{ path: "/", ...ok({ name: "It's a Plan api", status: "ok" }) },
			{ path: "/me", ...ok({ authenticated: false }) },
		],
	},
	{
		name: "plane.stats.no_project_key",
		url: PLANE.url,
		fields: { apiKey: "itp_key" },
		app: "plane",
		op: "stats",
		params: {},
		routes: [],
	},
	// GlitchTip
	{
		name: "glitchtip.organizations",
		...GLITCHTIP,
		app: "glitchtip",
		op: "organizations",
		params: {},
		routes: [
			{
				path: "/api/0/organizations/",
				headers: {
					"X-Hits": "2",
					"X-Max-Hits": "2",
					Link: '<http://gt.test/api/0/x/?cursor=a>; rel="previous"; results="false"; cursor="a", <http://gt.test/api/0/x/?cursor=b>; rel="next"; results="false"; cursor="b"',
				},
				...ok([{ id: 1, slug: "factory", name: "Factory" }]),
			},
		],
	},
	{
		name: "glitchtip.issues",
		...GLITCHTIP,
		app: "glitchtip",
		op: "issues",
		params: { orgSlug: "factory", limit: 20, sort: "-last_seen" },
		routes: [
			{
				path: "/api/0/organizations/factory/issues/",
				headers: {
					"X-Hits": "2",
					"X-Max-Hits": "2",
					Link: '<http://gt.test/api/0/x/?cursor=a>; rel="previous"; results="false"; cursor="a", <http://gt.test/api/0/x/?cursor=b>; rel="next"; results="false"; cursor="b"',
				},
				...ok([gtIssue("1"), gtIssue("2", "resolved")]),
			},
		],
	},
	{
		name: "glitchtip.latest_event",
		...GLITCHTIP,
		app: "glitchtip",
		op: "latestEvent",
		params: { issueId: "1" },
		routes: [
			{
				path: "/api/0/issues/1/events/latest/",
				...ok({
					culprit: "shop/cart.py in total",
					release: { version: "shop@1.4.2" },
					entries: [
						{
							type: "exception",
							data: {
								values: [
									{
										type: "ZeroDivisionError",
										value: "division by zero",
										stacktrace: {
											frames: [
												{
													filename: "shop/app.py",
													function: "handler",
													lineNo: 10,
												},
												{
													filename: "shop/cart.py",
													function: "total",
													lineNo: 42,
												},
											],
										},
									},
								],
							},
						},
					],
				}),
			},
		],
	},
	{
		name: "glitchtip.health",
		...GLITCHTIP,
		app: "glitchtip",
		op: "health",
		params: {},
		routes: [
			{
				path: "/api/0/",
				...ok({
					version: "6.2.6",
					auth: { scopes: ["org:read"] },
					user: { id: 1 },
				}),
			},
		],
	},
	{
		name: "glitchtip.health.no_auth",
		...GLITCHTIP,
		app: "glitchtip",
		op: "health",
		params: {},
		routes: [
			{ path: "/api/0/", ...ok({ version: "6.2.6", auth: null, user: null }) },
		],
	},
	// OpenPanel
	{
		name: "openpanel.insights",
		...OPENPANEL,
		app: "openpanel",
		op: "insights",
		params: { limit: 10 },
		routes: [
			{
				path: "/api/insights/shop/overview",
				...ok({
					summary: {
						unique_visitors: 1,
						total_sessions: 1,
						total_screen_views: 4,
					},
					series: [
						{
							date: "2026-10-18T00:00:00.000Z",
							unique_visitors: 0,
							total_sessions: 0,
						},
						{
							date: "2026-10-19T00:00:00.000Z",
							unique_visitors: 1,
							total_sessions: 1,
						},
					],
				}),
			},
			{
				path: "/api/insights/shop/pages/top",
				...ok([
					{
						origin: "http://127.0.0.1",
						path: "/cart",
						sessions: 1,
						pageviews: 2,
					},
				]),
			},
		],
	},
	{
		name: "openpanel.health",
		...OPENPANEL,
		app: "openpanel",
		op: "health",
		params: {},
		routes: [
			{
				path: "/api/healthcheck",
				...ok({ ready: true, redis: true, db: true, ch: true }),
			},
			{ path: "/api/insights/shop/live", ...ok({ visitors: 1 }) },
		],
	},
	// Matomo
	{
		name: "matomo.visits",
		...MATOMO,
		app: "matomo",
		op: "visits",
		params: {},
		routes: [
			{
				path: "/index.php",
				...ok({
					"2026-10-18": { nb_visits: 3, nb_uniq_visitors: 2, bounce_rate: 0.5 },
					"2026-10-19": [],
				}),
			},
		],
	},
	{
		name: "matomo.pages",
		...MATOMO,
		app: "matomo",
		op: "pages",
		params: { limit: 5 },
		routes: [
			{
				path: "/index.php",
				...ok([{ label: "/cart", nb_hits: 4, nb_visits: 2 }]),
			},
		],
	},
	{
		name: "matomo.health",
		...MATOMO,
		app: "matomo",
		op: "health",
		params: {},
		routes: [{ path: "/index.php", ...ok({ value: "5.5.0" }) }],
	},
	{
		name: "matomo.health.bad_token",
		...MATOMO,
		app: "matomo",
		op: "health",
		params: {},
		routes: [
			{
				path: "/index.php",
				...ok({
					result: "error",
					message: "You must be logged in to access this functionality.",
				}),
			},
		],
	},
	// Langfuse
	{
		name: "langfuse.traces",
		...LANGFUSE,
		app: "langfuse",
		op: "traces",
		params: { limit: 20 },
		routes: [
			{
				path: "/api/public/v2/observations",
				...ok({
					data: [
						{
							id: "obs1",
							traceId: "t1",
							name: "checkout-agent",
							startTime: iso("2026-10-19"),
							level: "DEFAULT",
							totalCost: 0.25,
						},
					],
					meta: {},
				}),
			},
		],
	},
	{
		name: "langfuse.traces.legacy_fallback",
		...LANGFUSE,
		app: "langfuse",
		op: "traces",
		params: { limit: 20 },
		routes: [
			{
				path: "/api/public/v2/observations",
				status: 404,
				body: { message: "v4 only" },
			},
			{
				path: "/api/public/traces",
				...ok({
					data: [
						{
							id: "t1",
							name: "checkout-agent",
							timestamp: iso("2026-10-19"),
							totalCost: 0.5,
						},
					],
					meta: { page: 1, limit: 20, totalItems: 1, totalPages: 1 },
				}),
			},
		],
	},
	{
		name: "langfuse.traces.neither",
		...LANGFUSE,
		app: "langfuse",
		op: "traces",
		params: {},
		routes: [
			{
				path: "/api/public/v2/observations",
				status: 404,
				body: { message: "no" },
			},
			{ path: "/api/public/traces", status: 404, body: { message: "no" } },
		],
	},
	{
		name: "langfuse.daily",
		...LANGFUSE,
		app: "langfuse",
		op: "daily",
		params: {},
		routes: [
			{
				path: "/api/public/metrics/daily",
				...ok({
					data: [
						{
							date: "2026-10-19",
							countTraces: 1,
							countObservations: 2,
							totalCost: 0.5,
							usage: [],
						},
					],
					meta: {},
				}),
			},
		],
	},
	{
		name: "langfuse.daily.v2_metrics",
		...LANGFUSE,
		app: "langfuse",
		op: "daily",
		params: {},
		routes: [
			{
				path: "/api/public/metrics/daily",
				status: 404,
				body: { message: "no" },
			},
			{
				path: "/api/public/v2/metrics",
				...ok({
					data: [
						{
							time_dimension: "2026-10-18",
							count_count: 0,
							sum_totalCost: null,
						},
						{
							time_dimension: "2026-10-19",
							count_count: 2,
							sum_totalCost: 0.5,
						},
					],
				}),
			},
		],
	},
	{
		name: "langfuse.health",
		...LANGFUSE,
		app: "langfuse",
		op: "health",
		params: {},
		routes: [
			{
				path: "/api/public/health",
				...ok({ status: "OK", version: "4.50.0" }),
			},
			{ path: "/api/public/v2/observations", ...ok({ data: [], meta: {} }) },
		],
	},
	{
		name: "langfuse.health.bad_keys",
		...LANGFUSE,
		app: "langfuse",
		op: "health",
		params: {},
		routes: [
			{
				path: "/api/public/health",
				...ok({ status: "OK", version: "4.50.0" }),
			},
			{
				path: "/api/public/v2/observations",
				status: 401,
				body: { message: "Invalid credentials" },
			},
		],
	},
];

async function run(c: Case) {
	const sent: Sent[] = [];
	vi.stubGlobal(
		"fetch",
		vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
			const u = new URL(String(input instanceof Request ? input.url : input));
			const method = (init?.method ?? "GET").toUpperCase();
			const headers = [...new Headers(init?.headers).keys()].sort();
			let body: unknown;
			if (typeof init?.body === "string") {
				try {
					body = JSON.parse(init.body);
				} catch {
					body = Object.fromEntries(new URLSearchParams(init.body));
				}
			} else if (init?.body instanceof URLSearchParams) {
				body = Object.fromEntries(init.body);
			}
			const query = [...u.searchParams.entries()];
			sent.push({
				method,
				path: u.pathname,
				query,
				headers,
				...(body === undefined ? {} : { body }),
			});
			const route = c.routes.find(
				(r) => r.path === u.pathname && (r.method ?? "GET") === method,
			);
			if (!route) return new Response("no route", { status: 599 });
			if (route.network) throw new TypeError("fetch failed");
			return new Response(JSON.stringify(route.body ?? null), {
				status: route.status ?? 200,
				headers: { "content-type": "application/json", ...route.headers },
			});
		}),
	);
	const def = FACTORY_APPS[c.app];
	const op = OPS[c.app][c.op];
	if (!op || !def.adapter) throw new Error(`no op ${c.app}.${c.op}`);
	const plain: Record<string, string> = {};
	for (const f of def.credentialFields)
		if (!f.secret && c.fields[f.key]) plain[f.key] = c.fields[f.key] ?? "";
	const parsed = op.params.safeParse(c.params);
	if (!parsed.success)
		return { sent, result: { ok: false, code: "bad_params" } };
	const Ctor = await def.adapter();
	let adapter:
		| Record<string, (...a: unknown[]) => Promise<unknown>>
		| undefined;
	const ctx = new Context();
	try {
		await ctx.plugin((child: Context) => {
			adapter = new Ctor(child, def.configFrom(c.url, c.fields));
		});
		if (!adapter) throw new Error("adapter failed to start");
		const value = await op.run(adapter as never, parsed.data as never, {
			fields: plain,
		});
		return {
			sent,
			result: { ok: true, value: JSON.parse(JSON.stringify(value)) },
		};
	} catch (e) {
		const code =
			e instanceof ParamError
				? "bad_params"
				: e instanceof UnsupportedError
					? "unsupported"
					: classifyError(e);
		return { sent, result: { ok: false, code } };
	}
}

afterEach(() => {
	vi.unstubAllGlobals();
	vi.useRealTimers();
});

it("factory_parity_fixture_is_fresh", async () => {
	vi.useFakeTimers({ toFake: ["Date"] });
	vi.setSystemTime(new Date(NOW));
	const out = [];
	for (const c of CASES) {
		const { sent, result } = await run(c);
		out.push({
			name: c.name,
			app: c.app,
			url: c.url,
			fields: c.fields,
			op: c.op,
			params: c.params,
			routes: c.routes,
			expect: { requests: sent, result },
		});
	}
	const text = `${JSON.stringify({ now: NOW, cases: out }, null, 2)}\n`;
	if (process.env.LOAMS_UPDATE_FACTORY_PARITY === "1") {
		writeFileSync(FIXTURE, text);
		return;
	}
	expect(existsSync(FIXTURE)).toBe(true);
	expect(readFileSync(FIXTURE, "utf8")).toBe(text);
});
