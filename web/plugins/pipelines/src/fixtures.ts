// The fixture catalog the mock client serves: a slice of the CN2 connector registry,
// at least one connector per family, with the config fields and schemas the builder
// needs. Field names follow the manifests' config schemas; no hosts, no credentials.

import type { ConfigField, ConnectorDef, Schema } from './model.js';

const secret = (key: string, label: string, required = true): ConfigField => ({
  key,
  label,
  kind: 'secret',
  required,
  hint: 'Write-only: stored in the secret store, never shown again.',
});
const text = (key: string, label: string, placeholder?: string, required = true): ConfigField => ({
  key,
  label,
  kind: 'text',
  required,
  placeholder,
});

const ORDERS: Schema = {
  fields: [
    { name: 'order_id', type: 'int' },
    { name: 'customer', type: 'string' },
    { name: 'amount', type: 'float' },
    { name: 'status', type: 'string' },
    { name: 'created_at', type: 'timestamp' },
  ],
};

const EVENTS: Schema = {
  fields: [
    { name: 'key', type: 'bytes' },
    { name: 'value', type: 'json' },
    { name: 'ts', type: 'timestamp' },
  ],
};

export const FIXTURE_CATALOG: readonly ConnectorDef[] = [
  {
    id: 'kafka',
    name: 'Kafka',
    family: 'kafka',
    directions: ['source', 'sink'],
    config: [
      text('brokers', 'Bootstrap servers', 'host:9092'),
      text('topic', 'Topic'),
      {
        key: 'sasl',
        label: 'SASL mechanism',
        kind: 'select',
        options: ['none', 'PLAIN', 'SCRAM-SHA-512'],
      },
      secret('password', 'SASL password', false),
    ],
    emits: EVENTS,
    accepts: {
      fields: [
        { name: 'key', type: 'bytes' },
        { name: 'value', type: 'json', required: true },
      ],
    },
    delivery: { source: 'effectively_once', sink: 'exactly_once' },
  },
  {
    id: 'redpanda',
    name: 'Redpanda',
    family: 'kafka',
    directions: ['source', 'sink'],
    config: [text('brokers', 'Bootstrap servers', 'host:9092'), text('topic', 'Topic')],
    emits: EVENTS,
    accepts: { fields: [{ name: 'value', type: 'json', required: true }] },
  },
  {
    id: 'postgres-cdc',
    name: 'Postgres CDC',
    family: 'postgres',
    directions: ['source'],
    config: [
      text('host', 'Host', 'db.internal'),
      { key: 'port', label: 'Port', kind: 'number', required: true, placeholder: '5432' },
      text('database', 'Database'),
      text('table', 'Table', 'public.orders'),
      text('user', 'User'),
      secret('password', 'Password'),
      text('slot', 'Replication slot', 'loams_slot', false),
    ],
    emits: ORDERS,
    delivery: { source: 'effectively_once' },
  },
  {
    id: 'postgres',
    name: 'Postgres',
    family: 'postgres',
    directions: ['sink'],
    config: [
      text('host', 'Host', 'db.internal'),
      text('database', 'Database'),
      text('table', 'Table'),
      text('user', 'User'),
      secret('password', 'Password'),
      {
        key: 'mode',
        label: 'Write mode',
        kind: 'select',
        options: ['insert', 'upsert'],
        required: true,
      },
    ],
    accepts: {
      fields: [
        { name: 'order_id', type: 'int', required: true },
        { name: 'customer', type: 'string' },
        { name: 'amount', type: 'float', required: true },
        { name: 'created_at', type: 'timestamp' },
      ],
    },
    delivery: { sink: 'exactly_once' },
  },
  {
    id: 'mysql-binlog',
    name: 'MySQL binlog',
    family: 'mysql',
    directions: ['source'],
    config: [
      text('host', 'Host'),
      text('table', 'Table'),
      text('user', 'User'),
      secret('password', 'Password'),
    ],
    emits: ORDERS,
  },
  {
    id: 'sqlserver-cdc',
    name: 'SQL Server CDC',
    family: 'sqlserver',
    directions: ['source'],
    config: [
      text('host', 'Host'),
      text('table', 'Table'),
      text('user', 'User'),
      secret('password', 'Password'),
    ],
    emits: ORDERS,
  },
  {
    id: 's3',
    name: 'S3',
    family: 'objects',
    directions: ['sink'],
    config: [
      text('bucket', 'Bucket'),
      text('prefix', 'Prefix', 'raw/', false),
      {
        key: 'format',
        label: 'Format',
        kind: 'select',
        options: ['parquet', 'jsonl'],
        required: true,
      },
      text('access_key_id', 'Access key id'),
      secret('secret_access_key', 'Secret access key'),
    ],
    accepts: { open: true },
    delivery: { sink: 'exactly_once' },
  },
  {
    id: 'redis-streams',
    name: 'Redis Streams',
    family: 'kv',
    directions: ['source', 'sink'],
    config: [
      text('address', 'Address', 'host:6379'),
      text('stream', 'Stream'),
      secret('password', 'Password', false),
    ],
    emits: {
      fields: [
        { name: 'id', type: 'string' },
        { name: 'fields', type: 'json' },
      ],
    },
    accepts: { fields: [{ name: 'fields', type: 'json', required: true }] },
  },
  {
    id: 'elasticsearch',
    name: 'Elasticsearch',
    family: 'search',
    directions: ['sink'],
    config: [
      text('index', 'Index'),
      text('endpoint', 'Endpoint', 'host:9200'),
      secret('api_key', 'API key'),
    ],
    accepts: {
      fields: [
        { name: 'order_id', type: 'string', required: true },
        { name: 'status', type: 'string' },
      ],
    },
  },
  {
    id: 'mongodb',
    name: 'MongoDB change streams',
    family: 'mongodb',
    directions: ['source', 'sink'],
    config: [
      text('database', 'Database'),
      text('collection', 'Collection'),
      secret('uri', 'Connection string'),
    ],
    emits: {
      fields: [
        { name: '_id', type: 'string' },
        { name: 'doc', type: 'json' },
      ],
    },
    accepts: { open: true },
  },
  {
    id: 'clickhouse',
    name: 'ClickHouse',
    family: 'analytics',
    directions: ['sink'],
    config: [
      text('endpoint', 'Endpoint', 'host:8123'),
      text('table', 'Table'),
      text('user', 'User'),
      secret('password', 'Password'),
    ],
    accepts: {
      fields: [
        { name: 'order_id', type: 'int', required: true },
        { name: 'amount', type: 'float', required: true },
        { name: 'created_at', type: 'timestamp', required: true },
      ],
    },
  },
  {
    id: 'iceberg',
    name: 'Iceberg',
    family: 'analytics',
    directions: ['sink'],
    config: [
      text('catalog', 'Catalog'),
      text('table', 'Table'),
      secret('token', 'Catalog token', false),
    ],
    accepts: { open: true },
  },
  {
    id: 'nats',
    name: 'NATS',
    family: 'brokers',
    directions: ['source', 'sink'],
    config: [text('subject', 'Subject'), secret('creds', 'Credentials', false)],
    emits: {
      fields: [
        { name: 'subject', type: 'string' },
        { name: 'data', type: 'bytes' },
      ],
    },
    accepts: { fields: [{ name: 'data', type: 'bytes', required: true }] },
  },
  {
    id: 'sqs',
    name: 'SQS',
    family: 'cloud',
    directions: ['source', 'sink'],
    config: [
      text('queue', 'Queue name'),
      text('region', 'Region'),
      secret('secret_access_key', 'Secret access key'),
    ],
    emits: {
      fields: [
        { name: 'message_id', type: 'string' },
        { name: 'body', type: 'string' },
      ],
    },
    accepts: { fields: [{ name: 'body', type: 'string', required: true }] },
  },
  {
    id: 'webhook',
    name: 'Webhook',
    family: 'http',
    directions: ['source', 'sink'],
    config: [text('path', 'Path', '/hooks/orders'), secret('signing_secret', 'Signing secret')],
    emits: { open: true },
    accepts: { open: true },
  },
  {
    id: 'otlp',
    name: 'OTLP',
    family: 'protocols',
    directions: ['source'],
    config: [
      {
        key: 'signal',
        label: 'Signal',
        kind: 'select',
        options: ['logs', 'metrics', 'traces'],
        required: true,
      },
    ],
    emits: {
      fields: [
        { name: 'resource', type: 'json' },
        { name: 'body', type: 'string' },
        { name: 'ts', type: 'timestamp' },
      ],
    },
  },
];
