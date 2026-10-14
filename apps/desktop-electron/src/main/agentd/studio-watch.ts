// Main's own watch of the daemon for Loams Studio's tray block and OS notifications
// (SD1 Task 28; §53 §7, §50 §5.5). It calls the daemon's loopback `loams.agentd.v1`
// listener directly, with the token only main holds, re-reading the discovery file
// and the token at each (re)open (T5-5). No electron import: index.ts wires it.
//
// Four server streams, each reopened with backoff: `WatchThreads` (titles),
// `WatchSessions` (which threads are working), `WatchApprovals` (G17) and
// `WatchNotifications` (G19). A watch the daemon answers `unimplemented` (a gap whose
// task has not landed) is treated as absent and not retried.
import {
	Code,
	ConnectError,
	createClient,
	type Transport,
} from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import {
	ApprovalService,
	type Notification,
	NotificationKind,
	NotificationService,
	SessionStatus,
	ThreadService,
} from "@loams/proto/agentd";
import {
	EMPTY_STUDIO,
	type StudioNotice,
	type StudioNoticeKind,
	type StudioSnapshot,
} from "../shell/studio-tray-model";
import type { AgentdTarget } from "./discover";

export interface StudioWatchDeps {
	/** The running daemon's listener and token; null while none runs. */
	target: () => AgentdTarget | null;
	fetch?: typeof fetch;
	now: () => number;
	onSnapshot: (s: StudioSnapshot) => void;
	onNotice: (n: StudioNotice) => void;
	/** The first retry delay (doubles to 30 s); also the poll while no daemon runs. */
	retryMs?: number;
}

export interface StudioWatch {
	stop(): void;
}

const KIND: Partial<Record<NotificationKind, StudioNoticeKind>> = {
	[NotificationKind.TURN_DONE]: "turn_done",
	[NotificationKind.APPROVAL]: "approval",
	[NotificationKind.QUESTION]: "question",
	[NotificationKind.ERROR]: "error",
	[NotificationKind.GROUP_READY]: "group_ready",
	[NotificationKind.CHECK_FAILED]: "check_failed",
};

function noticeOf(n: Notification): StudioNotice | undefined {
	const kind = KIND[n.kind];
	if (!kind) return undefined;
	return {
		id: n.id,
		kind,
		chatId: n.chatId,
		groupId: n.groupId,
		title: n.title,
		createdAtMs: Number(n.createdAtMs),
	};
}

const MAX_SEEN = 400;
const MAX_DELAY = 30_000;

export function watchStudio(deps: StudioWatchDeps): StudioWatch {
	const abort = new AbortController();
	const signal = abort.signal;
	const doFetch =
		deps.fetch ?? ((input, init) => globalThis.fetch(input, init));
	const first = deps.retryMs ?? 2_000;
	const since = deps.now();
	const seen = new Set<string>();

	let threads = new Map<string, string>();
	let working = new Set<string>();
	let approvals: { id: string; chatId: string }[] = [];
	let connected = false;
	const publish = () => {
		if (signal.aborted) return;
		deps.onSnapshot(
			connected ? { connected, threads, working, approvals } : EMPTY_STUDIO,
		);
	};

	const sleep = (ms: number) =>
		new Promise<void>((resolve) => {
			if (signal.aborted) return resolve();
			const t = setTimeout(done, ms);
			function done() {
				clearTimeout(t);
				signal.removeEventListener("abort", done);
				resolve();
			}
			signal.addEventListener("abort", done);
		});

	const transportFor = (t: AgentdTarget): Transport =>
		createConnectTransport({
			baseUrl: t.url,
			useBinaryFormat: true,
			// The token goes on main's own requests to the loopback listener only.
			fetch: (input, init) => {
				const headers = new Headers(init?.headers);
				headers.set("authorization", t.token.bearer());
				return doFetch(input, { ...init, headers });
			},
		});

	/** Runs one watch until stopped; `run` returns when the stream ends. */
	const loop = async (
		run: (transport: Transport) => Promise<void>,
		onLost: () => void,
	): Promise<void> => {
		let delay = first;
		while (!signal.aborted) {
			const t = deps.target();
			if (!t) {
				onLost();
				await sleep(first);
				continue;
			}
			try {
				await run(transportFor(t));
				delay = first;
			} catch (e) {
				if (signal.aborted) return;
				if (ConnectError.from(e).code === Code.Unimplemented) {
					onLost();
					return;
				}
				onLost();
			}
			await sleep(delay);
			delay = Math.min(delay * 2, MAX_DELAY);
		}
	};

	void loop(
		async (transport) => {
			for await (const r of createClient(ThreadService, transport).watchThreads(
				{},
				{ signal },
			)) {
				threads = new Map(
					r.chats.filter((c) => !c.archived).map((c) => [c.id, c.title ?? ""]),
				);
				connected = true;
				publish();
			}
		},
		() => {
			if (!connected) return;
			connected = false;
			threads = new Map();
			working = new Set();
			approvals = [];
			publish();
		},
	);
	void loop(
		async (transport) => {
			for await (const r of createClient(
				ThreadService,
				transport,
			).watchSessions({}, { signal })) {
				working = new Set(
					r.sessions
						.filter((s) => s.status === SessionStatus.WORKING)
						.map((s) => s.chatId),
				);
				publish();
			}
		},
		() => {
			working = new Set();
		},
	);
	void loop(
		async (transport) => {
			for await (const r of createClient(
				ApprovalService,
				transport,
			).watchApprovals({}, { signal })) {
				approvals = r.pending.map((a) => ({ id: a.id, chatId: a.chatId }));
				publish();
			}
		},
		() => {
			approvals = [];
		},
	);
	void loop(
		async (transport) => {
			const client = createClient(NotificationService, transport);
			for await (const r of client.watchNotifications({}, { signal })) {
				const n = r.notification && noticeOf(r.notification);
				// The daemon replays its last 200 on each open: pass each one on once, and
				// none from before this watch began.
				if (!n || seen.has(n.id) || n.createdAtMs < since) continue;
				seen.add(n.id);
				if (seen.size > MAX_SEEN)
					seen.delete(seen.values().next().value as string);
				deps.onNotice(n);
			}
		},
		() => undefined,
	);

	return {
		stop: () => abort.abort(),
	};
}
