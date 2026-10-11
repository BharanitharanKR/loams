// The Node-side client for Electron main (DD1 Task 10). Main is the only holder
// of the daemon's token: the renderer reaches the daemon through the named IPC
// channels in ipc.electron.ts, never through this object.
//
// The WebSocket implementation is `@loams/agentd-client` (Task 10); until it
// lands, `createMockAgentdClient` (mock.ts) stands in behind this interface.
import type {
	HelloReply,
	Item,
	Params,
	Result,
	StreamMethod,
	StreamParams,
	UnaryMethod,
} from "./types";

export interface Subscription {
	done: Promise<void>;
	cancel(): void;
}

export interface AgentdClient {
	readonly hello: HelloReply;
	call<M extends UnaryMethod>(
		method: M,
		params: Params<M>,
		opts?: { signal?: AbortSignal; timeoutMs?: number },
	): Promise<Result<M>>;
	subscribe<M extends StreamMethod>(
		method: M,
		params: StreamParams<M>,
		onItem: (i: Item<M>) => void,
	): Subscription;
	on(event: "disconnected" | "reconnected", cb: () => void): () => void;
	close(): Promise<void>;
}

export interface ConnectOptions {
	/** `<userData>` (the client reads `agentd/agentd.json` and `agentd/agentd.token`). */
	dataDir: string;
	clientVersion: string;
	signal?: AbortSignal;
}

export type ConnectFn = (opts: ConnectOptions) => Promise<AgentdClient>;

/**
 * The bearer token, held so it cannot leak by accident: it is not enumerable,
 * and `toString`, `toJSON` and Node's inspect all print `[redacted]`.
 * Only `bearer()` returns it, for the upgrade request's header.
 */
export class AgentdToken {
	readonly #value: string;
	constructor(value: string) {
		this.#value = value.trim();
	}
	/** `Authorization` header value. */
	bearer(): string {
		return `Bearer ${this.#value}`;
	}
	toString(): string {
		return "[redacted]";
	}
	toJSON(): string {
		return "[redacted]";
	}
	[Symbol.for("nodejs.util.inspect.custom")](): string {
		return "AgentdToken [redacted]";
	}
}

/** Headers for the WebSocket upgrade (§50 §7): the bearer token, and never an `Origin`. */
export function upgradeHeaders(token: AgentdToken): Record<string, string> {
	return { authorization: token.bearer() };
}
