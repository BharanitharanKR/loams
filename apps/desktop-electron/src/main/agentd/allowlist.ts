// The loams.agentd.v1 methods the renderer may call through the protocol proxy
// (design §53 §6.2, SD1 Task 5). The list is generated from the proto
// descriptors joined with the daemon's ndjson method table (a method is on it
// when its ndjson twin is `renderer: true`), by loams-agentd-connect; see
// allowlist.gen.ts.
import { AGENTD_RENDERER_METHODS } from "./allowlist.gen";

/** Every loams.agentd.v1 path starts with this. */
export const AGENTD_PREFIX = "/loams.agentd.v1.";

/** Whether a loams-app://console path is for the daemon's Connect surface. */
export function isAgentdPath(pathname: string): boolean {
	return pathname.startsWith(AGENTD_PREFIX);
}

/** Whether the renderer may call this method: an exact `/<service>/<method>` match. */
export function rendererMayCall(pathname: string): boolean {
	return AGENTD_RENDERER_METHODS.has(pathname);
}
