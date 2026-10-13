import { describe, expect, it } from "vitest";
import type { ConnectFn } from "../src/main/agentd/client";
import { createMockAgentdClient, defaultFixture } from "../src/main/agentd/mock";
import { AgentdService } from "../src/main/agentd/service";
import { AgentdError } from "../src/main/agentd/types";
import type { AgentdBackground } from "../src/shared/contracts";

function setup(
	init: {
		bg?: AgentdBackground;
		running?: boolean;
		mode?: "service" | "child";
		stopServiceFails?: number;
	} = {},
) {
	let stopFailures = init.stopServiceFails ?? 0;
	let bg: AgentdBackground = init.bg ?? "ask";
	let up = init.running ?? false;
	const log: string[] = [];
	const client = createMockAgentdClient({ ...defaultFixture(1_000_000), mode: init.mode ?? "child" });
	const connect: ConnectFn = async () => {
		if (!up) throw new AgentdError("unavailable", "down");
		return client;
	};
	const svc = new AgentdService({
		connect,
		userData: "/data",
		agentdDir: "/data/agentd",
		clientVersion: "0.1.0",
		readBackground: () => bg,
		writeBackground: (v) => {
			bg = v;
			log.push(`bg:${v}`);
		},
		launch: async (mode) => {
			log.push(`launch:${mode}`);
			up = true;
		},
		stopService: async () => {
			log.push("service.stop");
			if (stopFailures > 0) {
				stopFailures--;
				throw new Error("systemctl failed");
			}
		},
	});
	return { svc, client, log };
}

describe("agentd service", () => {
	it("stopped_when_no_daemon", async () => {
		const { svc } = setup();
		await svc.attach();
		expect(svc.status()).toEqual({ state: "stopped", dataDir: "/data/agentd" });
		expect((await svc.sessions()).ok).toBe(false);
	});

	it("start_refused_until_consent", async () => {
		const { svc, log } = setup({ bg: "ask" });
		const r = await svc.start();
		expect(r).toMatchObject({ ok: false, code: "consent_required" });
		expect(log).toEqual([]);
		expect(svc.setBackground("on")).toEqual({ ok: true, value: undefined });
		expect((await svc.start()).ok).toBe(true);
		expect(log).toEqual(["bg:on", "launch:service"]);
		expect(svc.status().state).toBe("running");
	});

	it("status_has_no_token_fields", async () => {
		const { svc } = setup({ running: true });
		await svc.attach();
		await Promise.resolve();
		const s = svc.status();
		expect(s).toMatchObject({ state: "running", version: "0.2.1", dataDir: "/data/agentd" });
		expect(JSON.stringify(s).toLowerCase()).not.toContain("token");
	});

	it("lists_and_reattaches_sessions", async () => {
		const { svc } = setup({ running: true });
		await svc.attach();
		const list = await svc.sessions();
		expect(list.ok && list.value.length).toBe(4);
		expect(await svc.reattach("s_01hq7y")).toEqual({
			ok: true,
			value: { route: "/agent/s_01hq7y" },
		});
		expect(await svc.reattach("s_01hq5k")).toMatchObject({ ok: false, code: "bad_params" });
		expect(await svc.reattach("../etc")).toMatchObject({ ok: false, code: "bad_params" });
	});

	it("stop_requires_confirmation", async () => {
		const { svc, client } = setup({ running: true });
		await svc.attach();
		expect(await svc.stop({})).toMatchObject({ ok: false, code: "consent_required" });
		expect(client.calls).not.toContain("Drain");
	});

	it("stop_child_mode_drains_then_shuts_down", async () => {
		const { svc, client, log } = setup({ running: true, mode: "child" });
		await svc.attach();
		expect((await svc.stop({ confirmed: true })).ok).toBe(true);
		const i = client.calls.indexOf("Drain");
		expect(i).toBeGreaterThanOrEqual(0);
		expect(client.calls.indexOf("Shutdown")).toBeGreaterThan(i);
		expect(log).not.toContain("service.stop");
		expect(svc.status().state).toBe("stopped");
	});

	it("stop_background_agents_drains_first_in_service_mode", async () => {
		const { svc, client, log } = setup({ running: true, mode: "service" });
		await svc.attach();
		expect((await svc.stop({ confirmed: true })).ok).toBe(true);
		expect(client.calls).toContain("Drain");
		expect(client.calls).not.toContain("Shutdown");
		expect(log).toEqual(["service.stop"]);
	});

	it("failed_service_stop_keeps_the_daemon_for_a_retry", async () => {
		const { svc, log } = setup({ running: true, mode: "service", stopServiceFails: 1 });
		await svc.attach();
		expect(await svc.stop({ confirmed: true })).toMatchObject({
			ok: false,
			code: "service_stop_failed",
		});
		expect(svc.status().state).toBe("running");
		expect((await svc.stop({ confirmed: true })).ok).toBe(true);
		expect(log).toEqual(["service.stop", "service.stop"]);
		expect(svc.status().state).toBe("stopped");
	});

	it("status_updates_from_watch", async () => {
		const { svc, client } = setup({ running: true });
		const seen: number[] = [];
		svc.onStatus((s) => {
			if (s.state === "running") seen.push(s.pendingApprovals);
		});
		await svc.attach();
		await Promise.resolve();
		client.setSessions([]);
		expect(seen.at(-1)).toBe(0);
		expect(seen).toContain(1);
	});

	it("studio_repos_list_add_and_clone_through_the_daemon", async () => {
		const { svc, client } = setup({ running: true });
		expect((await svc.repos()).ok).toBe(false);
		await svc.attach();
		const list = await svc.repos();
		expect(list).toEqual({
			ok: true,
			value: [{ path: "/home/omar/src/loams", name: "loams", defaultBranch: "main" }],
		});
		const added = await svc.addRepo("/home/omar/src/site");
		expect(added).toEqual({
			ok: true,
			value: { path: "/home/omar/src/site", name: "site", defaultBranch: "main" },
		});
		const cloned = await svc.cloneRepo("https://git.example.com/acme/api.git");
		expect(cloned.ok && cloned.value.name).toBe("api");
		expect((await svc.repos()).ok && (await svc.repos())).toMatchObject({
			value: [{ name: "loams" }, { name: "site" }, { name: "api" }],
		});
		expect(client.calls).toEqual(
			expect.arrayContaining(["ListRepos", "AddRepo", "CloneRepo"]),
		);
	});

	it("studio_repo_params_are_checked_in_main", async () => {
		const { svc, client } = setup({ running: true });
		await svc.attach();
		for (const bad of [42, "", "relative/path", "/a\u0000b", "x".repeat(5000)])
			expect(await svc.addRepo(bad)).toMatchObject({ ok: false, code: "bad_params" });
		for (const bad of [
			"file:///etc",
			"ext::sh -c touch% /tmp/pwned",
			"-uhttps://x",
			"https://",
			"javascript:alert(1)",
			"/local/path",
			7,
		])
			expect(await svc.cloneRepo(bad)).toMatchObject({ ok: false, code: "invalid_url" });
		expect(client.calls).not.toContain("AddRepo");
		expect(client.calls).not.toContain("CloneRepo");
		expect((await svc.cloneRepo("git@git.example.com:acme/api.git")).ok).toBe(true);
		expect((await svc.cloneRepo("ssh://git@git.example.com/acme/web.git")).ok).toBe(true);
	});
});
