// One dashboard panel: a LiveChart (line, area, bar) or a stat tile with sparklines, over a
// metric's backfill and, when refresh is on, its live stream. In edit mode the panel is a
// focusable grid cell: arrows move it, Shift+arrows resize it, Delete removes it, Enter
// edits it, so the grid never needs a mouse.

import { LiveChart, Sparkline, useMetricStream } from '@loams/charts';
import { Button, cx } from '@loams/ui';
import type { Widget } from '@loams-core/types';
import { type KeyboardEvent, useCallback, useEffect, useMemo, useState } from 'react';
import { formatValue, metricDef } from './catalog.js';
import { failureText, type MetricsClient } from './client.js';
import type { Panel as PanelModel } from './spec.js';
import {
  backfillThenLive,
  latestBySeries,
  liveRows,
  type PanelSource,
  type Row,
  toRow,
} from './stream.js';

export type Nudge = { dx?: number; dy?: number; dw?: number; dh?: number };

/** The grid step a key asks for, or undefined for any other key. */
export function nudgeForKey(e: Pick<KeyboardEvent, 'key' | 'shiftKey'>): Nudge | undefined {
  switch (e.key) {
    case 'ArrowLeft':
      return e.shiftKey ? { dw: -1 } : { dx: -1 };
    case 'ArrowRight':
      return e.shiftKey ? { dw: 1 } : { dx: 1 };
    case 'ArrowUp':
      return e.shiftKey ? { dh: -1 } : { dy: -1 };
    case 'ArrowDown':
      return e.shiftKey ? { dh: 1 } : { dy: 1 };
    default:
      return undefined;
  }
}

const CHART_HEIGHT_PER_ROW = 92;
const KEYS =
  'ArrowLeft ArrowRight ArrowUp ArrowDown Shift+ArrowLeft Shift+ArrowRight Shift+ArrowUp Shift+ArrowDown Delete Enter';

export function Panel({
  panel,
  widget,
  rows,
  client,
  rangeMs,
  stepMs,
  intervalMs,
  tick,
  editing,
  onEdit,
  onRemove,
  onNudge,
}: {
  panel: PanelModel;
  widget: Widget;
  /** The panel's grid height, in rows: sizes the chart. */
  rows: number;
  client: MetricsClient;
  rangeMs: number;
  stepMs: number;
  /** 0: refresh is off. */
  intervalMs: number;
  /** Bumped by the page's Refresh button: reloads the backfill. */
  tick: number;
  editing: boolean;
  onEdit: () => void;
  onRemove: () => void;
  onNudge: (d: Nudge) => void;
}) {
  const def = metricDef(panel.metric);
  const source: PanelSource = useMemo(
    () => ({ metric: panel.metric, rangeMs, stepMs, intervalMs }),
    [panel.metric, rangeMs, stepMs, intervalMs],
  );
  const titleId = `mx-panel-${panel.id}`;
  const onKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (!editing || e.target !== e.currentTarget) return;
    const nudge = nudgeForKey(e);
    if (nudge) {
      e.preventDefault();
      onNudge(nudge);
    } else if (e.key === 'Delete') {
      e.preventDefault();
      onRemove();
    } else if (e.key === 'Enter') {
      e.preventDefault();
      onEdit();
    }
  };
  // Restart the panel's data whenever what it reads changes.
  const key = `${panel.metric}:${panel.type}:${rangeMs}:${stepMs}:${intervalMs}:${tick}`;
  const height = Math.max(80, rows * CHART_HEIGHT_PER_ROW - 64);

  return (
    <section
      aria-labelledby={titleId}
      aria-roledescription={editing ? 'movable panel' : undefined}
      aria-keyshortcuts={editing ? KEYS : undefined}
      tabIndex={editing ? 0 : undefined}
      onKeyDown={onKeyDown}
      data-panel={panel.id}
      className={cx(
        'loams-card flex h-full flex-col overflow-hidden',
        editing && 'outline-offset-2 focus-visible:outline-2 focus-visible:outline-accent',
      )}
    >
      <div
        className={cx(
          'loams-card-head flex items-center justify-between gap-2',
          editing && 'mx-drag cursor-move',
        )}
      >
        <div className="min-w-0">
          <h3 id={titleId} className="truncate">
            {panel.title}
          </h3>
          <span className="block truncate font-mono text-xs text-muted" title={def?.description}>
            {panel.metric}
          </span>
        </div>
        {editing && (
          <div className="flex shrink-0 gap-1">
            <Button size="sm" variant="quiet" onClick={onEdit} aria-label={`Edit ${panel.title}`}>
              Edit
            </Button>
            <Button
              size="sm"
              variant="quiet"
              onClick={onRemove}
              aria-label={`Remove ${panel.title}`}
            >
              Remove
            </Button>
          </div>
        )}
      </div>
      <div className="loams-card-body min-h-0 flex-1">
        {panel.type === 'stat' ? (
          <StatBody key={key} panel={panel} client={client} source={source} />
        ) : (
          <ChartBody
            key={key}
            panel={panel}
            widget={widget}
            client={client}
            source={source}
            height={height}
          />
        )}
      </div>
    </section>
  );
}

type Load =
  | { phase: 'loading' }
  | { phase: 'error'; message: string }
  | { phase: 'ready'; rows: Row[] };

function ChartBody({
  panel,
  widget,
  client,
  source,
  height,
}: {
  panel: PanelModel;
  widget: Widget;
  client: MetricsClient;
  source: PanelSource;
  height: number;
}) {
  const [load, setLoad] = useState<Load>({ phase: 'loading' });
  useEffect(() => {
    const ctl = new AbortController();
    client
      .query({ ...source, signal: ctl.signal })
      .then((points) => {
        if (!ctl.signal.aborted)
          setLoad({ phase: 'ready', rows: points.map((p) => toRow(p, panel.title)) });
      })
      .catch((e) => {
        if (!ctl.signal.aborted) setLoad({ phase: 'error', message: failureText(e) });
      });
    return () => ctl.abort();
  }, [client, source, panel.title]);
  const stream = useMemo(
    () => liveRows(client, source, panel.title),
    [client, source, panel.title],
  );

  if (load.phase === 'loading') {
    return (
      <p className="text-sm text-muted" role="status">
        Loading {panel.title}…
      </p>
    );
  }
  if (load.phase === 'error') {
    return (
      <p className="text-sm text-danger" role="alert">
        Could not load {panel.title}: {load.message}
      </p>
    );
  }
  const spec = widget.chartSpec;
  if (!spec) return null;
  return (
    <LiveChart
      spec={spec}
      data={load.rows}
      stream={stream}
      windowMs={source.rangeMs}
      height={height}
      ariaLabel={`${panel.title} (${panel.metric}), ${panel.type} chart`}
    />
  );
}

function StatBody({
  panel,
  client,
  source,
}: {
  panel: PanelModel;
  client: MetricsClient;
  source: PanelSource;
}) {
  const unit = metricDef(panel.metric)?.unit ?? 'count';
  const make = useCallback(() => backfillThenLive(client, source), [client, source]);
  const points = useMetricStream(make, source.rangeMs);
  const latest = latestBySeries(points);
  if (latest.length === 0) {
    return (
      <p className="text-sm text-muted" role="status">
        Loading {panel.title}…
      </p>
    );
  }
  return (
    <ul className="m-0 grid list-none gap-3 p-0 sm:grid-cols-[repeat(auto-fit,minmax(9rem,1fr))]">
      {latest.map(({ series, v }) => {
        const name = series ?? panel.title;
        return (
          <li key={name} className="flex min-w-0 flex-col gap-1">
            <span className="truncate text-xs text-muted">{name}</span>
            <strong className="font-mono text-2xl text-ink" aria-live="off">
              {formatValue(v, unit)}
            </strong>
            <Sparkline
              points={points.filter((p) => p.series === series)}
              height={32}
              ariaLabel={`${name} ${panel.title} over the range, now ${formatValue(v, unit)}`}
            />
          </li>
        );
      })}
    </ul>
  );
}
