import type {
  AgentdBackground,
  AgentdSession,
  AgentdSessionState,
  AgentdStatus,
} from '@loams/desktop/contracts';
import type { Status } from '@loams/ui';
import { useCallback, useEffect, useState } from 'react';
import type { AgentdApi } from './mock.js';

/** The daemon's status: read once, then pushed by main. `null` while loading. */
export function useAgentdStatus(api: AgentdApi): AgentdStatus | null {
  const [s, setS] = useState<AgentdStatus | null>(null);
  useEffect(() => {
    let live = true;
    const off = api.onStatus((next) => live && setS(next));
    void api.status().then((first) => live && setS((cur) => cur ?? first));
    return () => {
      live = false;
      off();
    };
  }, [api]);
  return s;
}

export function useBackground(api: AgentdApi) {
  const [bg, setBg] = useState<AgentdBackground | null>(null);
  useEffect(() => {
    let live = true;
    void api.background.get().then((v) => live && setBg(v));
    return () => {
      live = false;
    };
  }, [api]);
  return [bg, setBg] as const;
}

export type SessionsState =
  | { kind: 'loading' }
  | { kind: 'ready'; sessions: AgentdSession[] }
  | { kind: 'error'; message: string };

/** Sessions, refetched whenever the daemon's state or counts change. */
export function useSessions(api: AgentdApi, status: AgentdStatus | null) {
  const [st, setSt] = useState<SessionsState>({ kind: 'loading' });
  const key =
    status?.state === 'running'
      ? `${status.pid}:${status.runningTurns}:${status.pendingApprovals}`
      : (status?.state ?? 'none');
  const reload = useCallback(async () => {
    const r = await api.sessions();
    setSt(r.ok ? { kind: 'ready', sessions: r.value } : { kind: 'error', message: r.message });
  }, [api]);
  useEffect(() => {
    if (key.startsWith('stopped') || key.startsWith('unavailable') || key === 'none') {
      setSt({ kind: 'ready', sessions: [] });
      return;
    }
    void reload();
  }, [key, reload]);
  return st;
}

export const STATE_LABEL: Record<AgentdStatus['state'], string> = {
  running: 'Running',
  stopped: 'Stopped',
  unavailable: 'Unavailable',
};
export const STATE_TONE: Record<AgentdStatus['state'], Status> = {
  running: 'done',
  stopped: 'neutral',
  unavailable: 'failed',
};
export const SESSION_LABEL: Record<AgentdSessionState, string> = {
  running: 'Working',
  waiting_approval: 'Waiting for approval',
  idle: 'Idle',
  interrupted: 'Interrupted',
  failed: 'Failed',
};
export const SESSION_TONE: Record<AgentdSessionState, Status> = {
  running: 'progress',
  waiting_approval: 'planned',
  idle: 'neutral',
  interrupted: 'planned',
  failed: 'failed',
};

/** "Background service" or "While the app is open". */
export function modeLabel(mode: 'service' | 'child'): string {
  return mode === 'service' ? 'Background service' : 'While the app is open';
}

export function backgroundLabel(bg: AgentdBackground): string {
  if (bg === 'on') return 'Keep running in the background';
  if (bg === 'off') return 'Only while the app is open';
  return 'Not chosen yet';
}
