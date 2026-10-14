// Settings › Studio (SD1 Task 15): the defaults a new thread starts from — harness
// and model, approval mode (Q803: supervised), parallel runs — and notifications.
// They are this device's preferences, kept under `loams.studio.settings`; the
// composer (Task 19) and run groups (Task 25) read them. The web edition also
// lists its agentd hosts (Hosts.tsx).

import type { StudioEditor, StudioEditorId, StudioNotifyPrefs } from '@loams/desktop/contracts';
import { Card, Checkbox, Field, Input, Notice, Select, Spinner } from '@loams/ui';
import { useEffect, useState } from 'react';
import type { StudioAgentd, StudioDesktop } from '../agentd.js';
import type { StudioKeys } from '../palette/commands.js';
import { Hosts } from './Hosts.js';
import { KeybindingsCard } from './Keybindings.js';

export const SETTINGS_KEY = 'loams.studio.settings';

export const HARNESSES = [
  { value: 'claude-code', label: 'Claude Code' },
  { value: 'codex', label: 'Codex' },
  { value: 'opencode', label: 'OpenCode' },
  { value: 'loams-bot', label: 'Loams agent' },
] as const;

/** §53 §8.1 (D911): `supervised` asks before each tool call; `full-access` never asks. */
export type ApprovalMode = 'supervised' | 'full-access';

export interface StudioSettings {
  harness: (typeof HARNESSES)[number]['value'];
  model: string;
  approvalMode: ApprovalMode;
  parallelRuns: number;
  notifyTurnFinished: boolean;
  notifyApprovalWaiting: boolean;
}

export const SETTINGS_DEFAULTS: StudioSettings = {
  harness: 'claude-code',
  model: '',
  approvalMode: 'supervised',
  parallelRuns: 2,
  notifyTurnFinished: true,
  notifyApprovalWaiting: true,
};

export function readSettings(): StudioSettings {
  try {
    const raw = JSON.parse(
      localStorage.getItem(SETTINGS_KEY) ?? 'null',
    ) as Partial<StudioSettings> | null;
    if (!raw || typeof raw !== 'object') return SETTINGS_DEFAULTS;
    const merged = { ...SETTINGS_DEFAULTS, ...raw };
    return {
      harness: HARNESSES.some((h) => h.value === merged.harness)
        ? merged.harness
        : SETTINGS_DEFAULTS.harness,
      model: typeof merged.model === 'string' ? merged.model.slice(0, 200) : '',
      approvalMode: ['supervised', 'full-access'].includes(merged.approvalMode)
        ? merged.approvalMode
        : SETTINGS_DEFAULTS.approvalMode,
      parallelRuns:
        Number.isInteger(merged.parallelRuns) &&
        merged.parallelRuns >= 2 &&
        merged.parallelRuns <= 6
          ? merged.parallelRuns
          : SETTINGS_DEFAULTS.parallelRuns,
      notifyTurnFinished: merged.notifyTurnFinished !== false,
      notifyApprovalWaiting: merged.notifyApprovalWaiting !== false,
    };
  } catch {
    return SETTINGS_DEFAULTS;
  }
}

function store(next: StudioSettings): void {
  try {
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(next));
  } catch {
    // Storage blocked: the choice lasts for this page.
  }
}

/** Changes this device's defaults (the palette's harness and model switches, SD1 Task 24). */
export function writeSettings(patch: Partial<StudioSettings>): StudioSettings {
  const next = { ...readSettings(), ...patch };
  store(next);
  return next;
}

function useSettings() {
  const [settings, setSettings] = useState(readSettings);
  const update = (patch: Partial<StudioSettings>) =>
    setSettings((prev) => {
      const next = { ...prev, ...patch };
      store(next);
      return next;
    });
  return [settings, update] as const;
}

const APPROVAL_LABEL: Record<ApprovalMode, string> = {
  supervised: 'Supervised: ask before each tool call',
  'full-access': 'Full access: never ask',
};

/** The notification choices as Electron main keeps them (SD1 Task 28). */
export function notifyPrefsOf(s: StudioSettings): StudioNotifyPrefs {
  return { turnDone: s.notifyTurnFinished, approval: s.notifyApprovalWaiting };
}

const EDITORS: { value: StudioEditorId; label: string }[] = [
  { value: 'vscode', label: 'VS Code' },
  { value: 'cursor', label: 'Cursor' },
  { value: 'zed', label: 'Zed' },
  { value: 'command', label: 'Another program…' },
];

/**
 * "Open in editor" (Q810): VS Code, Cursor or Zed through their URL schemes, or a
 * program picked in the OS file dialog. The choice is kept by Electron main.
 */
function EditorCard({ desktop }: { desktop: StudioDesktop }) {
  const [editor, setEditor] = useState<StudioEditor | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    desktop.editor().then(
      (e) => live && setEditor(e),
      () => live && setEditor({ editor: 'vscode' }),
    );
    return () => {
      live = false;
    };
  }, [desktop]);
  const choose = async (value: StudioEditorId) => {
    setError(null);
    if (value === 'command') {
      const r = await desktop.chooseEditorCommand();
      if (r.ok) setEditor(r.value);
      else if (r.code !== 'cancelled') setError(r.message);
      return;
    }
    const r = await desktop.setEditor(value);
    if (r.ok) setEditor({ editor: value });
    else setError(r.message);
  };
  return (
    <Card title="Editor">
      {editor ? (
        <Field
          label="Open threads in"
          hint={
            editor.editor === 'command' && editor.commandName
              ? `Runs ${editor.commandName} with the thread's folder.`
              : "Opens the thread's folder through the editor's link."
          }
        >
          {(props) => (
            <Select
              {...props}
              value={editor.editor}
              onChange={(e) => void choose(e.target.value as StudioEditorId)}
            >
              {EDITORS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.value === 'command' && editor.commandName ? editor.commandName : o.label}
                </option>
              ))}
            </Select>
          )}
        </Field>
      ) : (
        <Spinner label="Loading the editor choice" />
      )}
      {error && <Notice tone="danger" title={error} />}
    </Card>
  );
}

export function SettingsPage({ agentd, keys }: { agentd: StudioAgentd; keys?: StudioKeys }) {
  const [s, update] = useSettings();
  // Electron main shows the daemon's notifications (SD1 Task 28): it keeps a copy of
  // these two choices.
  const desktop = agentd.desktop;
  const { turnDone, approval } = notifyPrefsOf(s);
  useEffect(() => {
    void desktop?.setNotifications({ turnDone, approval }).catch(() => undefined);
  }, [desktop, turnDone, approval]);
  return (
    <div className="mx-auto flex max-w-[760px] flex-col gap-5 px-6 py-8">
      <header>
        <h1 className="m-0 text-[1.5rem] font-semibold text-ink">Studio settings</h1>
        <p className="m-0 mt-1 text-muted">Defaults for new threads on this device.</p>
      </header>
      {agentd.kind === 'web' && <Hosts agentd={agentd} />}
      <Card title="New threads">
        <div className="grid items-start gap-4 sm:grid-cols-2">
          <Field label="Harness">
            {(props) => (
              <Select
                {...props}
                value={s.harness}
                onChange={(e) => update({ harness: e.target.value as StudioSettings['harness'] })}
              >
                {HARNESSES.map((h) => (
                  <option key={h.value} value={h.value}>
                    {h.label}
                  </option>
                ))}
              </Select>
            )}
          </Field>
          <Field label="Model" hint="Empty uses the harness's own default.">
            {(props) => (
              <Input
                {...props}
                value={s.model}
                maxLength={200}
                placeholder="Harness default"
                onChange={(e) => update({ model: e.target.value })}
              />
            )}
          </Field>
          <Field label="Approvals" hint="Destructive git actions always ask, whatever this says.">
            {(props) => (
              <Select
                {...props}
                value={s.approvalMode}
                onChange={(e) => update({ approvalMode: e.target.value as ApprovalMode })}
              >
                {(Object.keys(APPROVAL_LABEL) as ApprovalMode[]).map((m) => (
                  <option key={m} value={m}>
                    {APPROVAL_LABEL[m]}
                  </option>
                ))}
              </Select>
            )}
          </Field>
          <Field label="Parallel runs" hint="How many runs a new run group starts with.">
            {(props) => (
              <Select
                {...props}
                value={String(s.parallelRuns)}
                onChange={(e) => update({ parallelRuns: Number(e.target.value) })}
              >
                {[2, 3, 4, 5, 6].map((n) => (
                  <option key={n} value={n}>
                    {n} runs
                  </option>
                ))}
              </Select>
            )}
          </Field>
        </div>
      </Card>
      {agentd.desktop && <EditorCard desktop={agentd.desktop} />}
      <Card title="Notifications">
        <div className="flex flex-col gap-2">
          {agentd.desktop && (
            <p className="m-0 text-sm text-muted">
              Shown as system notifications while Loams Desktop is in the background.
            </p>
          )}
          <Checkbox
            label="When a turn finishes"
            checked={s.notifyTurnFinished}
            onChange={(e) => update({ notifyTurnFinished: e.target.checked })}
          />
          <Checkbox
            label="When an approval is waiting"
            checked={s.notifyApprovalWaiting}
            onChange={(e) => update({ notifyApprovalWaiting: e.target.checked })}
          />
        </div>
      </Card>
      {keys && <KeybindingsCard store={keys.keybindings} platform={keys.platform} />}
    </div>
  );
}
