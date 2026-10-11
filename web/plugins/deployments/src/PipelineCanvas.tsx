// One pipeline on a React Flow canvas (@xyflow/react 12, as in dashboard-ui's graph
// tile): read-only, fitted, themed from @loams/ui tokens, and keyboard navigable. Tab
// reaches the first stage; the arrow keys, Home and End move between stages in pipeline
// order; the buttons inside a stage are reachable with Tab from there.

import {
  Background,
  BackgroundVariant,
  type Edge,
  ReactFlow,
  ReactFlowProvider,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { onThemeChange } from '@loams/ui';
import { type KeyboardEvent, useCallback, useId, useRef, useSyncExternalStore } from 'react';
import type { StageNode } from './layout.js';
import { nodeTypes } from './StageNode.js';

function isDark() {
  return typeof document !== 'undefined' && document.documentElement.classList.contains('dark');
}

/** Follows the console's theme class on <html>, so the canvas chrome matches the tokens. */
export function useDarkTheme(): boolean {
  return useSyncExternalStore(
    (fn) => (typeof document === 'undefined' ? () => {} : onThemeChange(fn)),
    isDark,
    () => false,
  );
}

/** The id of the stage to move to from `current` for a key, or undefined if the key is not ours. */
export function nextStage(order: readonly string[], current: string | undefined, key: string) {
  if (order.length === 0) return undefined;
  const i = current === undefined ? -1 : order.indexOf(current);
  switch (key) {
    case 'ArrowRight':
    case 'ArrowDown':
      return order[Math.min(order.length - 1, i + 1)];
    case 'ArrowLeft':
    case 'ArrowUp':
      return order[Math.max(0, i - 1)];
    case 'Home':
      return order[0];
    case 'End':
      return order[order.length - 1];
    default:
      return undefined;
  }
}

export function PipelineCanvas({
  label,
  nodes,
  edges,
  height = 260,
}: {
  label: string;
  nodes: StageNode[];
  edges: Edge[];
  height?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const help = useId();
  const dark = useDarkTheme();
  const order = nodes.map((n) => n.id);

  const onKeyDown = useCallback(
    (e: KeyboardEvent<HTMLDivElement>) => {
      const nodeEl = (e.target as HTMLElement).closest<HTMLElement>('.react-flow__node');
      // Keys inside a stage's own buttons are theirs; only a focused stage moves.
      if (!nodeEl || e.target !== nodeEl) return;
      const target = nextStage(order, nodeEl.dataset.id, e.key);
      if (target === undefined) return;
      e.preventDefault();
      e.stopPropagation();
      ref.current?.querySelector<HTMLElement>(`.react-flow__node[data-id="${target}"]`)?.focus();
    },
    [order],
  );

  return (
    <div
      ref={ref}
      className="ld-canvas"
      role="group"
      aria-label={label}
      aria-describedby={help}
      onKeyDownCapture={onKeyDown}
      style={{ height }}
    >
      <p id={help} className="sr-only">
        Use the arrow keys to move between stages, Home and End for the first and last stage, and
        Tab to reach a stage's actions.
      </p>
      <ReactFlowProvider>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          colorMode={dark ? 'dark' : 'light'}
          fitView
          fitViewOptions={{ padding: 0.12 }}
          minZoom={0.4}
          maxZoom={1.5}
          nodesDraggable={false}
          nodesConnectable={false}
          nodesFocusable
          edgesFocusable={false}
          elementsSelectable
          panOnDrag
          zoomOnScroll={false}
          preventScrolling={false}
        >
          <Background variant={BackgroundVariant.Dots} gap={16} color="var(--op-rule)" />
        </ReactFlow>
      </ReactFlowProvider>
    </div>
  );
}
