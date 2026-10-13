// loams-app://console/loams.agentd.v1.* (design §53 §6.2, D908; SD1 Task 5):
// the renderer's way to the local daemon's Connect surface. Pure: no electron
// import, so the tests drive it with a fake or a real fetch.
import type { AgentdTarget } from "../agentd/discover";
import { proxyAgentdRequest } from "./proxy";
import { routeAgentd } from "./route";

const jsonError = (status: number, code: string, message: string): Response =>
	new Response(JSON.stringify({ code, message }), {
		status,
		headers: { "content-type": "application/json" },
	});

/**
 * Checks the method against the renderer allowlist (403 `forbidden_method`),
 * finds the daemon (503 `agentd_unavailable`), and forwards with the token
 * only main holds. The token is read only for an allowed method.
 */
export async function handleAgentdRequest(
	req: Request,
	agentd: () => AgentdTarget | null,
	fetchImpl: typeof fetch,
): Promise<Response> {
	let target: AgentdTarget | null = null;
	const route = routeAgentd(new URL(req.url).pathname, () => {
		target = agentd();
		return target?.url ?? null;
	});
	if (route.kind === "reject")
		return jsonError(route.status, route.body.code, route.body.message);
	const found = target as AgentdTarget | null;
	if (!found)
		return jsonError(
			503,
			"agentd_unavailable",
			"Background agents are not running.",
		);
	try {
		return await proxyAgentdRequest(
			req,
			route.target,
			found.token.bearer(),
			fetchImpl,
		);
	} catch {
		return jsonError(503, "agentd_unavailable", "The daemon did not answer.");
	}
}
