// Builds the AgentdService for main. Until the daemon ships (DD1 Task 11) and
// `@loams/agentd-client` lands (Task 10), a development build talks to the
// fixture-backed mock and a packaged build reports the daemon as stopped.
import { readSetting, writeSetting } from "../settings";
import type { AgentdClient, ConnectFn } from "./client";
import { BACKGROUND_KEY, parseBackground } from "./consent";
import { agentdDir } from "./discover";
import { createMockAgentdClient, defaultFixture } from "./mock";
import { AgentdService } from "./service";
import { AgentdError } from "./types";

export function createAgentdService(o: {
	userData: string;
	settingsFile: string;
	version: string;
	/** Use the mock daemon (development builds only). */
	mock: boolean;
}): AgentdService {
	let mockClient: AgentdClient | null = null;
	let mockMode: "service" | "child" | null = null;
	const connect: ConnectFn = async () => {
		if (o.mock && mockMode) {
			mockClient = createMockAgentdClient({ ...defaultFixture(), mode: mockMode });
			return mockClient;
		}
		throw new AgentdError("unavailable", "The daemon is not running");
	};
	return new AgentdService({
		connect,
		userData: o.userData,
		agentdDir: agentdDir(o.userData),
		clientVersion: o.version,
		readBackground: () =>
			parseBackground(readSetting<unknown>(o.settingsFile, BACKGROUND_KEY, "ask")),
		writeBackground: (v) => writeSetting(o.settingsFile, BACKGROUND_KEY, v),
		launch: async (mode) => {
			if (!o.mock)
				throw new AgentdError("unavailable", "The daemon is not bundled yet");
			mockMode = mode;
		},
		stopService: async () => {
			mockMode = null;
		},
	});
}
