// Wire types of loams-agentd (DD1 Shared contracts, §50 §5.4, §8). Hand-written
// until Task 9's ts-rs output (`web/packages/agentd-client/src/gen`) lands; the
// names and fields match the Rust types so the swap is an import change.

/** `loams_agentd_proto::rpc::AGENTD_PROTOCOL`. */
export const AGENTD_PROTOCOL = 1;

export type ClientKind = "electron-main" | "cli" | "mcp";
export type DaemonMode = "service" | "child";
export type Role = "owner" | "mcp";

export interface Hello {
	client: ClientKind;
	clientVersion: string;
	protocol: number;
}

export interface HelloReply {
	protocol: number;
	version: string;
	buildSha: string;
	mode: DaemonMode;
	pid: number;
	startedAtMs: number;
	role: Role;
	features: string[];
}

export type EngineState =
	| { state: "stopped" }
	| { state: "starting"; attempt: number }
	| {
			state: "ready";
			url: string;
			esUrl: string;
			flightUrl: string;
			durableUrl: string;
			liveUrl?: string;
			pid: number;
	  }
	| { state: "failed"; reason: string; logPath: string };

export interface DaemonStatus {
	mode: DaemonMode;
	draining: boolean;
	runningTurns: number;
	pendingApprovals: number;
	engine: EngineState;
	version: string;
}

/** `<userData>/agentd/agentd.json`, written by the daemon after it binds (§50 §5.4). */
export interface Discovery {
	protocol: number;
	version: string;
	pid: number;
	port: number;
	startedAtMs: number;
	mode: DaemonMode;
}

/** A session row as `ListSessions` returns it (the fork's surviving method). */
export interface SessionSummary {
	id: string;
	title: string;
	harness: string;
	state: "running" | "waiting_approval" | "idle" | "interrupted" | "failed";
	updatedAtMs: number;
	pendingApprovals: number;
	resumable: boolean;
}

/** The `{err}` reasons of the Shared contracts; also `AgentdError.code`. */
export type AgentdErrorCode =
	| "unauthenticated"
	| "forbidden_role"
	| "protocol_mismatch"
	| "agentd_draining"
	| "unknown_method"
	| "bad_params"
	| "key_required"
	| "unknown_provider"
	| "invalid_url"
	| "tool_unavailable_remote_server"
	| "not_found"
	| "engine_not_ready"
	| "keyring_unavailable";

export class AgentdError extends Error {
	constructor(
		readonly code: AgentdErrorCode | "unavailable",
		message: string,
	) {
		super(message);
		this.name = "AgentdError";
	}
}

/** `ListRepos`, `AddRepo`, `CloneRepo` (Studio's projects, §53 §5.2): a repository on the daemon's host. */
export interface Repo {
	path: string;
	name: string;
	defaultBranch: string | null;
}

/** The unary methods this client uses, with params and results (a subset of Task 9's table). */
export interface UnaryMethods {
	DaemonStatus: { params: Record<string, never>; result: DaemonStatus };
	ListSessions: { params: Record<string, never>; result: SessionSummary[] };
	AttachSession: { params: { sessionId: string }; result: { sessionId: string } };
	Drain: { params: { reason: "stop" | "upgrade" | "quit"; timeoutMs: number }; result: null };
	Shutdown: { params: Record<string, never>; result: null };
	ListRepos: { params: Record<string, never>; result: Repo[] };
	AddRepo: { params: { path: string }; result: Repo };
	CloneRepo: { params: { url: string }; result: Repo };
}
export type UnaryMethod = keyof UnaryMethods;
export type Params<M extends UnaryMethod> = UnaryMethods[M]["params"];
export type Result<M extends UnaryMethod> = UnaryMethods[M]["result"];

export interface StreamMethods {
	WatchDaemonStatus: { params: Record<string, never>; item: DaemonStatus };
}
export type StreamMethod = keyof StreamMethods;
export type StreamParams<M extends StreamMethod> = StreamMethods[M]["params"];
export type Item<M extends StreamMethod> = StreamMethods[M]["item"];
