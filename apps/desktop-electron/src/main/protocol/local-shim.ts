import {
	create,
	type DescMessage,
	fromBinary,
	fromJson,
	type JsonValue,
	type MessageShape,
	toBinary,
	toJson,
} from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { admin, auth, instance } from "@loams/proto";
import type { LocalShim } from "./handler.electron";

export interface LocalShimInfo {
	version: string;
	username: string;
}

const GET_INSTANCE = "/loams.instance.v1.InstanceService/GetInstance";
const AUTH = "/loams.auth.v1.AuthService/";
const ADMIN = "/loams.admin.v1.";

/** Connect's error body; the HTTP status follows the code (Connect protocol). */
const connectError = (
	status: number,
	code: string,
	message: string,
): Response =>
	new Response(JSON.stringify({ code, message }), {
		status,
		headers: { "content-type": "application/json" },
	});

const NO_CONTROL_PLANE =
	"This needs a Loams control plane. Add a server in Servers.";

/** Binary (`application/proto`) or JSON (`application/json`), as the request was. */
function isBinary(contentType: string | null): boolean {
	return (contentType ?? "").split(";")[0]?.trim() === "application/proto";
}

function encode<D extends DescMessage>(
	schema: D,
	message: MessageShape<D>,
	binary: boolean,
	headers?: Headers,
): Response {
	const out = new Headers(headers);
	out.delete("content-length");
	out.delete("content-encoding");
	out.set("content-type", binary ? "application/proto" : "application/json");
	return new Response(
		binary
			? toBinary(schema, message)
			: JSON.stringify(toJson(schema, message)),
		{ status: 200, headers: out },
	);
}

/**
 * The local engine has no control plane, so the console's own API
 * (`loams.auth.v1`, `loams.admin.v1`; API2 Task 10) is answered here, over
 * Connect, for the local edition:
 *
 * - `AuthService/GetSession` answers the person at this computer, in the org
 *   "This computer"; `ListProviders` answers that there is no sign-in.
 * - `InstanceService/GetInstance` goes to the engine, and its answer gains the
 *   `local` and `desktop` features and `loams.auth.v1`, which this shim serves,
 *   and loses `setup_required` (there is nothing to set up here).
 * - Every other auth or admin RPC is `unimplemented`: add a server for those.
 *
 * Pure: no electron. `kind` is consulted per request so a remote server is
 * never shimmed. Returns null when the request is not the shim's.
 */
export function createLocalShim(
	info: LocalShimInfo,
	kind: () => string,
	now: () => number = Date.now,
): LocalShim {
	const session = () => {
		const t = now();
		return create(auth.SessionSchema, {
			user: {
				id: "local",
				name: info.username,
				createdAt: timestampFromMs(t),
				lastSeenAt: timestampFromMs(t),
			},
			org: {
				id: "local",
				name: "This computer",
				slug: "local",
				createdAt: timestampFromMs(t),
			},
			role: admin.OrgRole.OWNER,
			csrfToken: "local",
			expiresAt: timestampFromMs(t + 12 * 3600_000),
		});
	};

	return (pathname, req, forward) => {
		if (kind() !== "local") return null;
		const binary = isBinary(req.headers.get("content-type"));
		if (pathname === GET_INSTANCE && req.method === "POST") {
			return forward().then(async (res) => {
				const encoding = res.headers.get("content-encoding");
				if (!res.ok || (encoding && encoding !== "identity")) return res;
				const fromProto = isBinary(res.headers.get("content-type"));
				const answer = fromProto
					? fromBinary(
							instance.GetInstanceResponseSchema,
							new Uint8Array(await res.arrayBuffer()),
						)
					: fromJson(
							instance.GetInstanceResponseSchema,
							(await res.json()) as JsonValue,
						);
				answer.features = { ...answer.features, local: true, desktop: true };
				// The person at this computer is the owner of "This computer": no
				// first-run setup, whatever the engine's own principal store says.
				answer.setupRequired = false;
				if (!answer.apiVersions.includes("loams.auth.v1"))
					answer.apiVersions = [...answer.apiVersions, "loams.auth.v1"];
				return encode(
					instance.GetInstanceResponseSchema,
					answer,
					fromProto,
					res.headers,
				);
			});
		}
		if (pathname.startsWith(AUTH) && req.method === "POST") {
			const method = pathname.slice(AUTH.length);
			if (method === "GetSession")
				return Promise.resolve(
					encode(
						auth.GetSessionResponseSchema,
						create(auth.GetSessionResponseSchema, { session: session() }),
						binary,
					),
				);
			if (method === "ListProviders")
				return Promise.resolve(
					encode(
						auth.ListProvidersResponseSchema,
						create(auth.ListProvidersResponseSchema, {}),
						binary,
					),
				);
		}
		if (pathname.startsWith(AUTH) || pathname.startsWith(ADMIN))
			return Promise.resolve(
				connectError(501, "unimplemented", NO_CONTROL_PLANE),
			);
		return null;
	};
}
