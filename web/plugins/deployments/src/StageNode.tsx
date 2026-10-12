// The custom React Flow node for a deployment stage or a promotion step: label, state,
// a progress bar (animated only when motion is allowed), duration or actor, the logs
// action, and for promotion steps the env/secret key events (names only).

import { StatusTag } from '@loams/ui';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import { createContext, useContext } from 'react';
import type { StageNode as StageNodeType } from './layout.js';
import { STATE_LABEL, statusTone } from './model.js';
import type { StageKind } from './types.js';

export interface PipelineActions {
  openLogs?: (deploymentId: string, stage: StageKind) => void;
}

export const PipelineActionsContext = createContext<PipelineActions>({});

const SECRET_VERB = {
  promoted: 'promoted',
  rotated: 'rotated',
  added: 'added',
  removed: 'removed',
};

export function StageNode({ data, selected }: NodeProps<StageNodeType>) {
  const { openLogs } = useContext(PipelineActionsContext);
  const pct = Math.round(Math.max(0, Math.min(1, data.progress)) * 100);
  const running = data.state === 'running';
  return (
    <div
      className="ld-node"
      data-state={data.state}
      data-branch={data.branch ? '' : undefined}
      data-selected={selected ? '' : undefined}
      data-motion={data.reducedMotion ? 'reduce' : 'ok'}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <Handle type="source" position={Position.Right} isConnectable={false} />
      <Handle
        id="branch"
        type="source"
        position={Position.Bottom}
        isConnectable={false}
        style={{ opacity: 0 }}
      />
      <Handle
        id="branch-in"
        type="target"
        position={Position.Top}
        isConnectable={false}
        style={{ opacity: 0 }}
      />
      <div className="ld-node-head">
        <strong>{data.label}</strong>
        <StatusTag status={statusTone(data.state)}>{STATE_LABEL[data.state]}</StatusTag>
      </div>
      <div
        className="ld-progress"
        role="progressbar"
        aria-label={`${data.label} progress`}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={pct}
      >
        <span
          className={running ? 'ld-progress-fill ld-running' : 'ld-progress-fill'}
          style={{ width: `${pct}%` }}
        />
      </div>
      <div className="ld-node-foot">
        <span className="ld-muted">{data.detail ?? ' '}</span>
        {data.logs && openLogs && (
          <button
            type="button"
            className="ld-link nodrag nopan"
            onClick={() => {
              const l = data.logs;
              if (l) openLogs(l.deploymentId, l.stage);
            }}
            aria-label={`${data.label} logs`}
          >
            Logs
          </button>
        )}
      </div>
      {data.secrets && data.secrets.length > 0 && (
        <ul className="ld-keys" aria-label={`${data.label} env and secret changes`}>
          {data.secrets.map((e) => (
            <li key={e.id} data-kind={e.kind}>
              <code>{e.key}</code> {SECRET_VERB[e.kind]}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export const nodeTypes = { stage: StageNode };
