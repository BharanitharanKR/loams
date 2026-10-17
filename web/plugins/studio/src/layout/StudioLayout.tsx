// Studio's page column (design 59): the sidebar, the right panel and the terminal drawer are
// the shell's now (the sidebar's thread list, the inspector, the drawer). The layout wires
// Studio's commands to them and gives the page a scrolling column.

import type { PanelService } from '@loams/console-host';
import type { ReactNode } from 'react';
import { StudioCommands, useStudioCommand } from '../palette/commands.js';

const NO_COMMANDS = new StudioCommands();

export function StudioLayout({
  commands,
  inspector,
  drawer,
  children,
}: {
  /** Studio's command bus: the layout sends its panel and terminal commands to the shell. */
  commands?: StudioCommands;
  inspector?: PanelService | undefined;
  drawer?: PanelService | undefined;
  children: ReactNode;
}) {
  const bus = commands ?? NO_COMMANDS;
  useStudioCommand(
    bus,
    'rightPanel.toggle',
    commands && inspector ? () => inspector.toggle() : undefined,
  );
  useStudioCommand(
    bus,
    'rightPanel.show',
    commands && inspector ? () => inspector.open() : undefined,
  );
  useStudioCommand(
    bus,
    'terminal.toggle',
    commands && drawer ? () => drawer.toggle('studio.terminal') : undefined,
  );
  return (
    <div className="flex h-full min-h-0 flex-col bg-bg" data-studio-layout>
      <div className="min-h-0 flex-1 overflow-auto">{children}</div>
    </div>
  );
}
