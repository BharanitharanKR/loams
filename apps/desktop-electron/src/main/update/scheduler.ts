import { CHECK_INTERVAL_MS, firstCheckDelayMs } from "./policy";

export interface SchedulerDeps {
	check: () => void;
	/** Subscribes to the computer waking up; returns the unsubscribe. */
	onResume: (cb: () => void) => () => void;
	rand?: () => number;
	setTimeout?: typeof setTimeout;
	setInterval?: typeof setInterval;
	clearTimeout?: typeof clearTimeout;
	clearInterval?: typeof clearInterval;
}

export interface CheckScheduler {
	/** Starts (true) or stops (false) the automatic checks; idempotent. */
	setEnabled(enabled: boolean): void;
	running(): boolean;
}

/**
 * The automatic update checks: one shortly after start, one every 6 hours and one on
 * wake. While stopped nothing is scheduled and no wake listener is held, so no
 * automatic request can happen. A manual check never goes through here.
 */
export function createCheckScheduler(d: SchedulerDeps): CheckScheduler {
	const st = d.setTimeout ?? setTimeout;
	const si = d.setInterval ?? setInterval;
	const ct = d.clearTimeout ?? clearTimeout;
	const ci = d.clearInterval ?? clearInterval;
	const rand = d.rand ?? Math.random;
	let handles: { first: ReturnType<typeof setTimeout>; every: ReturnType<typeof setInterval>; off: () => void } | undefined;
	return {
		running: () => handles !== undefined,
		setEnabled(enabled) {
			if (enabled === (handles !== undefined)) return;
			if (!enabled) {
				if (handles) {
					ct(handles.first);
					ci(handles.every);
					handles.off();
				}
				handles = undefined;
				return;
			}
			handles = {
				first: st(() => d.check(), firstCheckDelayMs(rand())),
				every: si(() => d.check(), CHECK_INTERVAL_MS),
				off: d.onResume(() => d.check()),
			};
		},
	};
}
