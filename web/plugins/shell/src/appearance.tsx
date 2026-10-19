// Appearance (design 59, D1074, D1076): the theme choice (System, Dark, Light) and the
// optional panel motion. The header menu and Settings › Appearance both use it.

import { MOTION_KEY, type ThemeChoice, type ThemeService } from '@loams/console-host';
import { Menu, MenuItem, MenuLabel, MenuSeparator, Segmented, Switch } from '@loams/ui';
import { Check, Monitor, Moon, Sun } from 'lucide-react';
import { useCallback, useState, useSyncExternalStore } from 'react';

const LABEL: Record<ThemeChoice, string> = { system: 'System', dark: 'Dark', light: 'Light' };
const CHOICES: readonly ThemeChoice[] = ['system', 'dark', 'light'];

export function useThemeState(theme: ThemeService): { choice: ThemeChoice; dark: boolean } {
  const subscribe = useCallback((l: () => void) => theme.subscribe(l), [theme]);
  const choice = useSyncExternalStore(subscribe, () => theme.choice);
  const dark = useSyncExternalStore(subscribe, () => theme.dark);
  return { choice, dark };
}

export function readMotion(): boolean {
  try {
    return localStorage.getItem(MOTION_KEY) === 'on';
  } catch {
    return false;
  }
}

export function setMotion(on: boolean): void {
  try {
    localStorage.setItem(MOTION_KEY, on ? 'on' : 'off');
  } catch {
    // Storage blocked: the setting holds for this page.
  }
  if (on) document.documentElement.dataset.motion = 'on';
  else delete document.documentElement.dataset.motion;
}

export function AppearanceMenu({ theme }: { theme: ThemeService }) {
  const { choice, dark } = useThemeState(theme);
  const [motion, setMotionState] = useState(readMotion);
  const Icon = choice === 'system' ? Monitor : dark ? Moon : Sun;
  return (
    <Menu
      label="Appearance"
      trigger={
        <button type="button" className="lc-round-btn" aria-label={`Theme: ${LABEL[choice]}`}>
          <Icon size={16} aria-hidden="true" />
        </button>
      }
    >
      <MenuLabel>Theme</MenuLabel>
      {CHOICES.map((c) => (
        <MenuItem key={c} onSelect={() => theme.set(c)}>
          <Check
            size={14}
            aria-hidden="true"
            style={{ visibility: c === choice ? 'visible' : 'hidden' }}
          />
          <span aria-current={c === choice ? 'true' : undefined}>{LABEL[c]}</span>
        </MenuItem>
      ))}
      <MenuSeparator />
      <MenuItem
        onSelect={() => {
          setMotion(!motion);
          setMotionState(!motion);
        }}
      >
        {motion ? 'Turn panel motion off' : 'Turn panel motion on'}
      </MenuItem>
    </Menu>
  );
}

export function AppearanceSection({ theme }: { theme: ThemeService }) {
  const { choice } = useThemeState(theme);
  const [motion, setMotionState] = useState(readMotion);
  return (
    <section className="grid justify-items-start gap-3" aria-labelledby="lc-appearance">
      <h2 id="lc-appearance" className="m-0 text-section font-semibold">
        Appearance
      </h2>
      <Segmented
        label="Theme"
        value={choice}
        onValueChange={(v) => theme.set(v as ThemeChoice)}
        options={CHOICES.map((c) => ({ value: c, label: LABEL[c] }))}
      />
      <Switch
        checked={motion}
        onCheckedChange={(on) => {
          setMotion(on);
          setMotionState(on);
        }}
        label="Animate panels"
      />
    </section>
  );
}
