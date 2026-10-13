// `LiveAdminApi` over the engine's `loams.live.v1.LiveAdminService` (LV1 Task 10),
// which the main port serves behind its authentication (LV1 Task 12).
//
// The wire differs from the views' types in a few places, mapped here:
// - the enums are `DEPLOYMENT_STATE_*`, `FUNCTION_KIND_*`, `FUNCTION_VISIBILITY_*` and
//   `LOG_LEVEL_*`; the views use the short lowercase names;
// - an argument validator is the `Validator` oneof; the views use `{ kind, ... }`;
// - `ListDeployments` is paginated (AIP-158); the views get every page;
// - `TailLogs` streams batches of up to 500 lines with a `skipped` count; the views get
//   one line at a time, and a `{ skipped }` event when the ring overwrote lines;
// - timestamps are `uint64` milliseconds (bigint); the views use numbers.

import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import { errors, live, liveAdmin } from '@loams/proto';
import {
  DeployConflictError,
  type Deployment,
  type DeploymentState,
  type FunctionInfo,
  type LiveAdminApi,
  type LogLevel,
  type LogLine,
  type LogsSkipped,
  type Validator,
} from './admin.js';
import { toJs } from './value.js';

/**
 * One `ListDeployments` page. The engine keeps 50 deployments plus the active one and
 * its parent (LV1 T6-7), so the history is a page or two.
 */
export const PAGE_SIZE = 100;

/** What the page shows when the engine does not serve the admin service. */
export const ADMIN_UNSERVED =
  'This engine does not serve the Loams Live admin API: Live is off (--no-live), or the ' +
  'engine is older than LiveAdminService on its main port.';

const STATES: Record<liveAdmin.DeploymentState, DeploymentState> = {
  [liveAdmin.DeploymentState.UNSPECIFIED]: 'pending',
  [liveAdmin.DeploymentState.PENDING]: 'pending',
  [liveAdmin.DeploymentState.VALIDATING]: 'validating',
  [liveAdmin.DeploymentState.BACKFILLING]: 'backfilling',
  [liveAdmin.DeploymentState.ACTIVE]: 'active',
  [liveAdmin.DeploymentState.SUPERSEDED]: 'superseded',
  [liveAdmin.DeploymentState.FAILED]: 'failed',
  [liveAdmin.DeploymentState.DEGRADED]: 'degraded',
};

const LEVELS: Record<liveAdmin.LogLevel, LogLevel> = {
  [liveAdmin.LogLevel.UNSPECIFIED]: 'log',
  [liveAdmin.LogLevel.DEBUG]: 'debug',
  [liveAdmin.LogLevel.LOG]: 'log',
  [liveAdmin.LogLevel.INFO]: 'info',
  [liveAdmin.LogLevel.WARN]: 'warn',
  [liveAdmin.LogLevel.ERROR]: 'error',
};

const PRIMITIVES: Record<live.ValidatorPrimitive, Validator['kind']> = {
  [live.ValidatorPrimitive.UNSPECIFIED]: 'any',
  [live.ValidatorPrimitive.NULL]: 'null',
  [live.ValidatorPrimitive.INT64]: 'int64',
  [live.ValidatorPrimitive.FLOAT64]: 'float64',
  [live.ValidatorPrimitive.BOOLEAN]: 'boolean',
  [live.ValidatorPrimitive.STRING]: 'string',
  [live.ValidatorPrimitive.BYTES]: 'bytes',
  [live.ValidatorPrimitive.ANY]: 'any',
};

/** The wire `Deployment`. Its ids are ULIDs, `parent` and `failure_reason` empty when unset. */
export function toDeployment(d: liveAdmin.Deployment | undefined): Deployment {
  if (!d) throw new Error('The engine answered without a deployment.');
  return {
    id: d.id,
    bundleSha256: d.bundleSha256,
    createdAtMs: Number(d.createdAtMs),
    createdBy: d.createdBy,
    message: d.message,
    parent: d.parent || undefined,
    state: STATES[d.state] ?? 'pending',
    failureReason: d.failureReason || undefined,
  };
}

/** The wire `Validator` oneof; `undefined` when unset (the function declares no `args`). */
export function toValidator(v: live.Validator | undefined): Validator | undefined {
  const k = v?.kind;
  switch (k?.case) {
    case 'primitive':
      return { kind: PRIMITIVES[k.value] ?? 'any' } as Validator;
    case 'array':
      return { kind: 'array', items: toValidator(k.value) ?? { kind: 'any' } };
    case 'object':
      return {
        kind: 'object',
        fields: Object.fromEntries(
          Object.entries(k.value.fields).map(([name, f]) => [
            name,
            toValidator(f) ?? { kind: 'any' },
          ]),
        ),
      };
    case 'literal':
      return { kind: 'literal', value: toJs(k.value) };
    case 'union':
      return {
        kind: 'union',
        members: k.value.members.map((m) => toValidator(m) ?? { kind: 'any' }),
      };
    case 'optional':
      return { kind: 'optional', inner: toValidator(k.value) ?? { kind: 'any' } };
    case 'id':
      return { kind: 'id', table: k.value };
    default:
      return undefined;
  }
}

export function toFunction(f: liveAdmin.FunctionInfo): FunctionInfo {
  const args = toValidator(f.args);
  return {
    path: f.path,
    kind: f.kind === liveAdmin.FunctionKind.MUTATION ? 'mutation' : 'query',
    visibility: f.visibility === liveAdmin.FunctionVisibility.INTERNAL ? 'internal' : 'public',
    ...(args ? { args } : {}),
  };
}

export function toLogLine(e: liveAdmin.LogEntry): LogLine {
  return {
    ts: Number(e.tsMs),
    requestId: e.requestId,
    function: e.function,
    ...(e.deployment ? { deployment: e.deployment } : {}),
    level: LEVELS[e.level] ?? 'log',
    line: e.line,
    ...(e.truncated ? { truncated: true } : {}),
  };
}

/**
 * The views' error for a failed admin call: a lost compare-and-swap is
 * `DeployConflictError` (`FAILED_PRECONDITION`, reason `live_deploy_conflict`), an
 * engine without the service says so in words, anything else is the error itself.
 */
export function adminError(e: unknown): unknown {
  if (!(e instanceof ConnectError)) return e;
  const reason = e.findDetails(errors.ErrorInfoSchema)[0]?.reason;
  if (reason === 'live_deploy_conflict') return new DeployConflictError();
  if (e.code === Code.Unimplemented) return new Error(ADMIN_UNSERVED);
  return e;
}

async function call<T>(run: () => Promise<T>): Promise<T> {
  try {
    return await run();
  } catch (e) {
    throw adminError(e);
  }
}

export function createLiveAdminApi(transport: Transport): LiveAdminApi {
  const client = createClient(liveAdmin.LiveAdminService, transport);
  return {
    mocked: false,
    listDeployments: (signal) =>
      call(async () => {
        const all: Deployment[] = [];
        const seen = new Set<string>();
        let pageToken = '';
        do {
          const res = await client.listDeployments({ pageSize: PAGE_SIZE, pageToken }, { signal });
          all.push(...res.deployments.map(toDeployment));
          pageToken = res.nextPageToken;
          // A token seen before would loop forever: an engine bug, not a shorter history.
          if (pageToken && seen.has(pageToken)) {
            throw new Error(`The engine repeated the page token ${pageToken}.`);
          }
          seen.add(pageToken);
        } while (pageToken);
        return all;
      }),
    getDeployment: (id, signal) =>
      call(async () => {
        const res = await client.getDeployment({ id }, { signal });
        return {
          deployment: toDeployment(res.deployment),
          functions: res.functions.map(toFunction),
        };
      }),
    activateDeployment: (id, expectedActive) =>
      call(async () => {
        const res = await client.activateDeployment({ id, expectedActive });
        return toDeployment(res.deployment);
      }),
    async *tailLogs(req, signal): AsyncGenerator<LogLine | LogsSkipped> {
      const stream = client.tailLogs(
        {
          follow: req.follow,
          sinceMs: req.sinceMs === undefined ? undefined : BigInt(Math.max(0, req.sinceMs)),
          function: req.function,
        },
        { signal },
      );
      try {
        for await (const batch of stream) {
          if (batch.skipped > 0n) yield { skipped: Number(batch.skipped) };
          for (const e of batch.entries) yield toLogLine(e);
        }
      } catch (e) {
        throw adminError(e);
      }
    },
  };
}
