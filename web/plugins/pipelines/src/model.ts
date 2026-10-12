// The builder's own model: connector definitions, schemas, and the pipeline graph the
// canvas edits. The connector catalog follows CN2 (docs/plans/2026-10-10-cn2-connectors.md):
// one family per `loams-connector-<family>` crate, each connector declaring the
// directions it serves. There is no loams.flow proto yet, so these types are the
// client contract (client.ts) and the mock serves them from fixtures (fixtures.ts).

/** Arrow-flavoured logical types (CN2 Task 5: Arrow is the canonical schema). */
export type FieldType = 'string' | 'int' | 'float' | 'bool' | 'timestamp' | 'json' | 'bytes';

export const FIELD_TYPES: readonly FieldType[] = [
  'string',
  'int',
  'float',
  'bool',
  'timestamp',
  'json',
  'bytes',
];

export interface SchemaField {
  name: string;
  type: FieldType;
  /** A field the sink cannot write without. Ignored on output schemas. */
  required?: boolean;
}

/** A record schema. `open` means "any record": a sink that takes whatever arrives. */
export type Schema = { open: true } | { open?: false; fields: readonly SchemaField[] };

/** The connector families of CN2 (OR-6: one crate per family). */
export type Family =
  | 'kafka'
  | 'postgres'
  | 'mysql'
  | 'sqlserver'
  | 'objects'
  | 'kv'
  | 'search'
  | 'mongodb'
  | 'analytics'
  | 'brokers'
  | 'cloud'
  | 'http'
  | 'protocols';

export const FAMILY_LABEL: Record<Family, string> = {
  kafka: 'Kafka protocol',
  postgres: 'Postgres',
  mysql: 'MySQL',
  sqlserver: 'SQL Server',
  objects: 'Objects',
  kv: 'Key-value',
  search: 'Search and vector',
  mongodb: 'MongoDB',
  analytics: 'Analytics and lakehouse',
  brokers: 'Brokers',
  cloud: 'Cloud messaging',
  http: 'HTTP and SaaS',
  protocols: 'Protocols',
};

export type Direction = 'source' | 'sink';

/** One config field of a connector. A `secret` is write-only: the UI never shows it back. */
export interface ConfigField {
  key: string;
  label: string;
  kind: 'text' | 'number' | 'boolean' | 'select' | 'secret';
  required?: boolean;
  options?: readonly string[];
  hint?: string;
  placeholder?: string;
}

export interface ConnectorDef {
  /** The manifest id, for example `kafka` or `postgres-cdc`. */
  id: string;
  name: string;
  family: Family;
  directions: readonly Direction[];
  config: readonly ConfigField[];
  /** What the source emits. Absent for a sink-only connector. */
  emits?: Schema;
  /** What the sink accepts. Absent for a source-only connector. */
  accepts?: Schema;
  /** The manifest's delivery guarantee per direction (CN2 OR-7). */
  delivery?: Partial<Record<Direction, 'at_least_once' | 'effectively_once' | 'exactly_once'>>;
}

export type TransformKind = 'filter' | 'map' | 'schema';

export const TRANSFORM_LABEL: Record<TransformKind, string> = {
  filter: 'Filter',
  map: 'Map fields',
  schema: 'Schema mapping',
};

export const TRANSFORM_HINT: Record<TransformKind, string> = {
  filter: 'Keep the records that match an expression.',
  map: 'Rename, keep or drop fields.',
  schema: 'Cast fields to the types the sink expects.',
};

export type NodeKind = Direction | 'transform';

/** A config value. Secrets are never values here: see `PipelineNode.secrets`. */
export type ConfigValue = string | number | boolean;

/** A field mapping of a `map` transform: `to` takes `from`'s value; no `to` drops it. */
export interface FieldMapping {
  from: string;
  to: string;
}

export interface PipelineNode {
  id: string;
  kind: NodeKind;
  /** A connector id for a source or sink, a transform kind for a transform. */
  ref: string;
  label: string;
  position: { x: number; y: number };
  config: Record<string, ConfigValue>;
  /**
   * Which secret fields have a value, by key. The values themselves live outside the
   * graph (the page's write-only secret store) and only travel on deploy.
   */
  secrets: Record<string, boolean>;
  /** `map` transforms: the fields kept, in order, with their new names. */
  mappings?: readonly FieldMapping[];
  /** `schema` transforms: the target type per field. */
  casts?: Readonly<Record<string, FieldType>>;
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
}

export interface PipelineGraph {
  name: string;
  nodes: readonly PipelineNode[];
  edges: readonly PipelineEdge[];
}

export const emptyGraph = (name = 'untitled-pipeline'): PipelineGraph => ({
  name,
  nodes: [],
  edges: [],
});
