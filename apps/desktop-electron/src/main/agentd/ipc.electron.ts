import { ipcMain } from "electron";
import { CH } from "../../shared/contracts";
import { assertTrustedSender } from "../security/policy";
import { getMainWindow } from "../shell/main-window";
import type { AgentdService } from "./service";

/** The renderer's agentd channels. Every value crossing here is token-free. */
export function registerAgentdIpc(svc: AgentdService): () => void {
	ipcMain.handle(CH.agentdStatus, (e) => {
		assertTrustedSender(e);
		return svc.status();
	});
	ipcMain.handle(CH.agentdSessions, (e) => {
		assertTrustedSender(e);
		return svc.sessions();
	});
	ipcMain.handle(CH.agentdReattach, (e, id: unknown) => {
		assertTrustedSender(e);
		return svc.reattach(id);
	});
	ipcMain.handle(CH.agentdStart, (e) => {
		assertTrustedSender(e);
		return svc.start();
	});
	ipcMain.handle(CH.agentdStop, (e, opts: unknown) => {
		assertTrustedSender(e);
		return svc.stop(opts);
	});
	ipcMain.handle(CH.agentdBackgroundGet, (e) => {
		assertTrustedSender(e);
		return svc.background();
	});
	ipcMain.handle(CH.agentdBackgroundSet, (e, v: unknown) => {
		assertTrustedSender(e);
		return svc.setBackground(v);
	});
	const off = svc.onStatus((s) => {
		getMainWindow()?.webContents.send(CH.agentdEvent, s);
	});
	return () => {
		off();
		for (const ch of [
			CH.agentdStatus,
			CH.agentdSessions,
			CH.agentdReattach,
			CH.agentdStart,
			CH.agentdStop,
			CH.agentdBackgroundGet,
			CH.agentdBackgroundSet,
		])
			ipcMain.removeHandler(ch);
	};
}
