// One service on the canvas: kind, name, a live status badge and a sparkline of the
// service's key metric, streamed through the client.

import { type SeriesPoint, Sparkline, useMetricStream } from '@loams/charts';
import { StatusTag } from '@loams/ui';
import { Handle, type Node, type NodeProps, Position } from '@xyflow/react';
import { createContext, memo, useCallback, useContext } from 'react';
import type { CanvasClient, Service } from './client.js';
import { kindInfo, STATUS_LABEL, STATUS_TONE } from './kinds.js';
import { NODE_HEIGHT, NODE_WIDTH } from './layout.js';

/** The sparkline's window. */
export const METRIC_WINDOW_MS = 2 * 60 * 1000;

export type ServiceNodeData = { service: Service };
export type ServiceFlowNode = Node<ServiceNodeData, 'service'>;

/** What every node needs from the page, without threading it through node data. */
export const CanvasContext = createContext<{ client: CanvasClient; projectId: string } | null>(
  null,
);

function useCanvas() {
  const c = useContext(CanvasContext);
  if (!c) throw new Error('ServiceNode outside a CanvasContext');
  return c;
}

/** The latest value of a metric, formatted for a node. */
export function latestValue(points: readonly SeriesPoint[], unit: string): string {
  const last = points.at(-1);
  if (!last) return '—';
  const v = Math.abs(last.v) >= 100 ? Math.round(last.v).toString() : last.v.toFixed(1);
  return unit.startsWith('/') || unit === '%' ? `${v}${unit}` : `${v} ${unit}`;
}

/** The node's accessible name: what a screen reader announces when focus lands on it. */
export function nodeLabel(s: Service): string {
  return `${kindInfo(s.kind).label} ${s.name}, ${STATUS_LABEL[s.status]}`;
}

export function useServiceMetric(serviceId: string): SeriesPoint[] {
  const { client, projectId } = useCanvas();
  const source = useCallback(
    () => client.metricStream(projectId, serviceId),
    [client, projectId, serviceId],
  );
  return useMetricStream(source, METRIC_WINDOW_MS);
}

function ServiceNodeView({ data, selected }: NodeProps<ServiceFlowNode>) {
  const s = data.service;
  const info = kindInfo(s.kind);
  const points = useServiceMetric(s.id);
  return (
    <div
      className={[
        'lc-canvas-node flex flex-col gap-2 border bg-surface p-3 text-ink',
        selected ? 'border-accent' : 'border-rule',
      ].join(' ')}
      style={{ width: NODE_WIDTH, height: NODE_HEIGHT }}
      data-status={s.status}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <div className="flex items-center gap-2">
        <span
          aria-hidden="true"
          className="inline-flex h-6 min-w-8 items-center justify-center bg-raised px-1 text-[10px] text-muted"
        >
          {info.glyph}
        </span>
        <div className="min-w-0 flex-1">
          <div className="truncate text-[11px] uppercase tracking-wide text-faint">
            {info.label}
          </div>
          <div className={/* mono-ok: id */ 'truncate font-mono text-sm'}>{s.name}</div>
        </div>
        <StatusTag status={STATUS_TONE[s.status]}>{STATUS_LABEL[s.status]}</StatusTag>
      </div>
      <div className="flex items-end gap-2">
        <div className="min-w-0 flex-1">
          <Sparkline
            points={points}
            height={32}
            ariaLabel={`${info.metric.name} for ${s.name}, last two minutes`}
          />
        </div>
        <div className="shrink-0 text-right">
          <div className="text-[10px] text-faint">{info.metric.name}</div>
          <div className="text-xs tabular-nums">{latestValue(points, info.metric.unit)}</div>
        </div>
      </div>
      <Handle type="source" position={Position.Right} isConnectable={false} />
    </div>
  );
}

export const ServiceNode = memo(ServiceNodeView);
