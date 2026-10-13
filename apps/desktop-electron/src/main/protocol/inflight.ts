// The upstream requests the app protocol has in flight, so leaving a server can stop them first.
// Without this, a console request still running against the server being left can complete after
// its cookies were cleared, and Chromium stores that response's Set-Cookie again: the old session
// is back in the jar (I4), and since cookies ignore the port it would reach 127.0.0.1:<engine>.

export class InFlight {
	private readonly running = new Map<AbortController, Promise<unknown>>();

	/** Runs `start` with a signal that `abortAll` fires; the result is passed through. */
	run<T>(start: (signal: AbortSignal) => Promise<T>): Promise<T> {
		const ctl = new AbortController();
		const p = start(ctl.signal);
		const settled = p.then(
			() => undefined,
			() => undefined,
		);
		this.running.set(ctl, settled);
		void settled.then(() => this.running.delete(ctl));
		return p;
	}

	/**
	 * Aborts every running request and resolves once each has settled. A request whose response
	 * headers had already arrived settles at once, and its cookies were stored before it did, so
	 * a cookie clear that follows this sees them.
	 */
	async abortAll(): Promise<void> {
		const pending = [...this.running];
		for (const [ctl] of pending) ctl.abort();
		await Promise.all(pending.map(([, settled]) => settled));
	}

	get size(): number {
		return this.running.size;
	}
}

/** The app protocol's upstream requests (handler.electron.ts) and the server switch (ipc.electron.ts). */
export const upstream = new InFlight();
