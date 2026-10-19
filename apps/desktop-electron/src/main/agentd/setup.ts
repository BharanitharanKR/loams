// Builds the AgentdService for main. It attaches to a running loams-agentd over
// the daemon's `loams.agentd.v1` listener (connect-client.ts) and starts the
// bundled daemon when asked (launch.ts). The fixture-backed mock is opt-in for
// development: LOAMS_DESKTOP_AGENTD_MOCK=1 in an unpackaged build.
import { readSetting, writeSetting } from "../settings";
import type { AgentdClient, ConnectFn } from "./client";
import { connectAgentd } from "./connect-client";
import { BACKGROUND_KEY, parseBackground } from "./consent";
import { agentdDir } from "./discover";
import type { AgentdLauncher } from "./launch";
import { createMockAgentdClient, defaultFixture } from "./mock";
import { AgentdService } from "./service";
import { AgentdError } from "./types";

export function createAgentdService(o: {
	userData: string;
	settingsFile: string;
	version: string;
	/** Use the mock daemon (development builds only). */
	mock: boolean;
	/** Starts the bundled daemon; absent only with the mock. */
	launcher?: AgentdLauncher;
	connect?: ConnectFn;
}): AgentdService {
	const deps = {
		userData: o.userData,
		agentdDir: agentdDir(o.userData),
		clientVersion: o.version,
		readBackground: () =>
			parseBackground(readSetting<unknown>(o.settingsFile, BACKGROUND_KEY, "ask")),
		writeBackground: (v: "on" | "off") => writeSetting(o.settingsFile, BACKGROUND_KEY, v),
	};
	if (o.mock) {
		let mockClient: AgentdClient | null = null;
		let mockMode: "service" | "child" | null = null;
		return new AgentdService({
			...deps,
			connect: async () => {
				if (mockMode) {
					mockClient = createMockAgentdClient({ ...defaultFixture(), mode: mockMode });
					return mockClient;
				}
				throw new AgentdError("unavailable", "The daemon is not running");
			},
			launch: async (mode) => {
				mockMode = mode;
			},
			stopService: async () => {
				mockMode = null;
			},
		});
	}
	const launcher = o.launcher;
	return new AgentdService({
		...deps,
		connect: o.connect ?? connectAgentd(),
		launch: async (mode) => {
			if (!launcher) throw new AgentdError("unavailable", "loams-agentd is not bundled with this build");
			await launcher.launch(mode);
		},
		stopService: async () => {
			await launcher?.stopService();
		},
	});
}
