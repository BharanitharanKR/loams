// The tray's background-agents block (DD1 Task 12, §50 §5.5). Pure: tray.electron.ts
// appends these items to trayModel()'s once the daemon ships (Task 11).
import type { AgentdStatus } from "../../shared/contracts";
import type { TrayItem } from "./tray-model";

const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? "" : "s"}`;

/** "Background agents: running · 2 turns · 1 approval". */
export function agentdStatusLabel(s: AgentdStatus): string {
	switch (s.state) {
		case "running": {
			const parts = [s.draining ? "stopping" : "running"];
			if (s.runningTurns > 0) parts.push(plural(s.runningTurns, "turn"));
			if (s.pendingApprovals > 0)
				parts.push(plural(s.pendingApprovals, "approval"));
			return `Background agents: ${parts.join(" · ")}`;
		}
		case "stopped":
			return "Background agents: stopped";
		default:
			return "Background agents: unavailable";
	}
}

/** Approvals waiting in the daemon; the tray badge adds these. */
export function agentdBadge(s: AgentdStatus): number {
	return s.state === "running" ? Math.max(0, s.pendingApprovals) : 0;
}

/**
 * Items: the status line (disabled, informational), "Stop background agents"
 * (only while running and not already draining), and the quit label, which
 * says agents keep running in service mode.
 */
export function agentdTrayItems(s: AgentdStatus): TrayItem[] {
	const running = s.state === "running";
	const service = running && s.mode === "service";
	return [
		{ id: "agentd-status", label: agentdStatusLabel(s), enabled: false },
		{
			id: "agentd-stop",
			label: "Stop background agents",
			enabled: running && !s.draining,
		},
		{
			id: "quit",
			label: service
				? "Quit Loams Desktop (agents keep running)"
				: "Quit Loams Desktop",
			enabled: true,
		},
	];
}
