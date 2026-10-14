import type { ComponentType, ReactNode } from 'react';

/** The selected environment, as slot props carry it. */
export interface EnvironmentRef {
  id: string;
  name: string;
  namespace: string;
  protected: boolean;
}

/**
 * The slot catalog (§37 §5.5). Plugins extend it by declaration merging:
 *
 *     declare module '@loams/slots' {
 *       interface SlotMap { 'my-plugin.panel': { kind: 'list'; props: { id: string } } }
 *     }
 *
 * Changing a slot's props is a breaking change of the console host.
 */
export interface SlotMap {
  root: { kind: 'single'; props: Record<string, never> };
  'console.nav': { kind: 'list'; props: { environment?: EnvironmentRef } };
  'console.page': {
    kind: 'keyed';
    props: { params: Record<string, string>; environment?: EnvironmentRef };
  };
  'console.settings.section': { kind: 'list'; props: Record<string, never> };
  'environment.overview.card': { kind: 'list'; props: { environment?: EnvironmentRef } };
  'approval.renderer': { kind: 'keyed'; props: { approvalId: string } };
  'shell.overlay': { kind: 'list'; props: Record<string, never> };
  /** A product nav entry; the shell reads its `meta` and renders the link itself. */
  'shell.nav.section': { kind: 'list'; props: Record<string, never> };
  /** The right dock (the agent panel); one plugin owns it. Empty hides the dock and its toggle. */
  'shell.dock.right': { kind: 'single'; props: Record<string, never> };
  /** The server switcher, in the header. */
  'shell.header.server': { kind: 'list'; props: Record<string, never> };
  /**
   * The sidebar's context above the nav: the project switcher and the open project's
   * pages (API2 Task 10). While it has an entry, it replaces the environment switcher.
   */
  'shell.nav.context': { kind: 'list'; props: Record<string, never> };
  /**
   * Commands for the shell's palette (§53 §4.5, §5.10; SD1 Task 24). An entry renders
   * nothing: while the palette is open it is mounted with the query and calls `publish`
   * with its commands (again whenever they change). Its mode is its `meta.mode`, else
   * its plugin's; the shell lists the current mode's commands first.
   */
  'palette.command': {
    kind: 'list';
    props: { query: string; publish: (commands: readonly PaletteCommand[]) => void };
  };
}

/** One row of the shell's command palette, as a `palette.command` entry publishes it. */
export interface PaletteCommand {
  /** Unique within its entry. */
  id: string;
  title: string;
  /** The heading it is listed under ("Threads", "Git"). */
  group?: string;
  /** Secondary text at the end of the row (a path, a project). */
  detail?: string;
  /** More words the query matches, after the title, in order of importance. */
  keywords?: string[];
  icon?: ReactNode;
  /** Its keyboard shortcut's keys, for example ['Ctrl', 'D']. */
  shortcut?: string[];
  /** When its subject last changed (threads); breaks ties between title matches. */
  recency?: number;
  /** Sorts after the other matches of its group. */
  secondary?: boolean;
  disabled?: boolean;
  /** Keep the palette open after running it. */
  keepOpen?: boolean;
  /** Runs the command. A thrown error or a rejection is shown as a toast. */
  run(): void | Promise<void>;
}

export type SlotName = keyof SlotMap;
export type SlotKind = SlotMap[SlotName]['kind'];
export type SlotProps<N extends SlotName> = SlotMap[N]['props'];

/** Slot components receive props only, never the cordis context (the harness rule). */
export type SlotComponent<N extends SlotName> = ComponentType<SlotProps<N>>;

/** Navigation and page metadata a slot entry may carry. */
export interface SlotMeta {
  /** A visible label, for example a nav item's text. */
  label?: string;
  /** Where a nav entry links to (a router path). */
  href?: string;
  /** The nav group, for example "Operate", or a `shell.nav.section` group ("Data"). */
  group?: string;
  /** A stable id for a `shell.nav.section` entry. */
  id?: string;
  /** An icon name the shell knows ("postgres", "live"); unknown names get a plain dot. */
  icon?: string;
  /**
   * The console mode a nav entry shows in (§53 §4.1, D901), for a plugin in both
   * modes. Without it the entry shows in each of its plugin's modes.
   */
  mode?: 'console' | 'studio';
}

export interface SlotSpec<N extends SlotName> {
  name: N;
  /** The registering plugin's id; shown by the error boundary. */
  plugin: string;
  /** Required for keyed slots: the key this entry answers. */
  key?: string;
  /** Lower renders first; ties keep registration order. */
  order?: number;
  meta?: SlotMeta;
}

export interface SlotEntry<N extends SlotName = SlotName> extends SlotSpec<N> {
  /** Unique per registration. */
  id: number;
  order: number;
  component: SlotComponent<N>;
}
