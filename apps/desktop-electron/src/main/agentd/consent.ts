// Background-mode consent (§50 §5.5, D785): pure, so it is unit-testable.
// `daemon.background` is `ask` until the user answers the dialog; the answer is
// stored and never asked again (changeable in Settings).
import type { AgentdBackground } from "../../shared/contracts";

export const BACKGROUND_KEY = "daemon.background";

/** A stored value, or `ask` for anything missing or unknown. */
export function parseBackground(v: unknown): AgentdBackground {
	return v === "on" || v === "off" ? v : "ask";
}

/** Whether starting the daemon must first ask the user. */
export function needsConsent(bg: AgentdBackground): boolean {
	return bg === "ask";
}

/** The daemon mode a choice runs in; `ask` runs nothing until answered. */
export function modeFor(bg: AgentdBackground): "service" | "child" | null {
	if (bg === "on") return "service";
	if (bg === "off") return "child";
	return null;
}

/** Validates a renderer-supplied answer; null rejects it. */
export function parseAnswer(v: unknown): "on" | "off" | null {
	return v === "on" || v === "off" ? v : null;
}

/** The explicit-stop guard: the renderer must say the user confirmed. */
export function stopConfirmed(opts: unknown): boolean {
	return (
		!!opts &&
		typeof opts === "object" &&
		(opts as { confirmed?: unknown }).confirmed === true
	);
}
