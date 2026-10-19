// SDKP's `typescript_builders_match_corpus`.
//
// Every case in `sdks/fixtures/builders/` is driven through the public
// builders — `Schema`, `Filter`, `Search` — from its neutral spec, and the
// output is compared with the case's `expect` after both are decoded into the
// named message, so key order and number spelling do not matter and a field
// the message does not have does. The notation is in that directory's README.

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fromJson, toJson, type JsonObject, type JsonValue } from '@bufbuild/protobuf';
import { QuerySchema, SearchRequestSchema } from '@loams/proto/query';
import { describe, expect, it } from 'vitest';
import { Filter, type FilterJson } from '../src/builders/filter.js';
import { Schema } from '../src/builders/schema.js';
import { Search, Sort, dbsf, rrf, weighted } from '../src/builders/search.js';
import { InvalidArgumentError } from '../src/runtime/errors.js';
import { FIXTURES } from './helpers/server.js';

interface Call {
  op: string;
  args?: JsonValue[];
}

interface Case {
  name: string;
  builder: 'schema' | 'filter' | 'search';
  message: string;
  spec: unknown;
  expect: JsonObject;
}

const DIR = join(FIXTURES, 'builders');
const cases: Case[] = readdirSync(DIR)
  .filter((file) => file.endsWith('.json'))
  .sort()
  .map((file) => JSON.parse(readFileSync(join(DIR, file), 'utf8')) as Case);

const camel = (name: string) => name.replace(/_([a-z0-9])/g, (_m, c: string) => c.toUpperCase());

const isCall = (value: unknown): value is Call =>
  typeof value === 'object' && value !== null && !Array.isArray(value) && 'op' in value;

/** An options object, with its keys in TypeScript case. */
function options(value: JsonValue | undefined): Record<string, unknown> | undefined {
  if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
    return undefined;
  }
  return Object.fromEntries(Object.entries(value).map(([key, v]) => [camel(key), v]));
}

/** A call's arguments, with nested calls evaluated. */
function args(call: Call): unknown[] {
  return (call.args ?? []).map((arg) => (isCall(arg) ? evaluate(arg) : arg));
}

/** A filter, fusion or sort call. */
function evaluate(call: Call): JsonObject {
  const a = args(call);
  switch (call.op) {
    case 'rrf':
      return rrf(a[0] as number | undefined);
    case 'dbsf':
      return dbsf();
    case 'weighted':
      return weighted(...(a as number[]));
    case 'score':
      return Sort.score(a[0] as 'asc' | 'desc' | undefined);
    case 'pk':
      return Sort.pk(a[0] as 'asc' | 'desc' | undefined);
    case 'field':
      return Sort.field(a[0] as string, a[1] as 'asc' | 'desc' | undefined, a[2] as 'first' | 'last' | undefined);
    default:
      return filter(call.op, a);
  }
}

function filter(op: string, a: unknown[]): FilterJson {
  const verb = camel(op === 'phrase' ? 'phrase' : op);
  switch (verb) {
    case 'range':
      return Filter.range(a[0] as string, options(a[1] as JsonValue) ?? {});
    case 'match':
      return Filter.match(a[0] as string, a[1] as string, options(a[2] as JsonValue));
    case 'ids':
      return Filter.ids(...(a as Parameters<typeof Filter.ids>));
    case 'and':
    case 'or':
    case 'not':
    case 'filter':
      return (Filter[verb] as (...q: FilterJson[]) => FilterJson)(...(a as FilterJson[]));
    default: {
      const fn = (Filter as Record<string, unknown>)[verb];
      if (typeof fn !== 'function') {
        throw new Error(`no filter verb ${op}`);
      }
      return (fn as (...x: unknown[]) => FilterJson)(...a);
    }
  }
}

function schema(calls: Call[]): JsonObject {
  let builder = new Schema();
  for (const call of calls) {
    const a = args(call);
    const verb = camel(call.op) as keyof Schema;
    const fn = builder[verb] as unknown as (...x: unknown[]) => Schema;
    const last = a[a.length - 1];
    const opts = options(last as JsonValue);
    builder = fn.apply(builder, opts === undefined ? a : [...a.slice(0, -1), opts]);
  }
  return builder.toJson();
}

function search(spec: { on: string; calls: Call[] }): Search {
  let builder = Search.on(spec.on);
  for (const call of spec.calls) {
    const a = args(call);
    const verb = camel(call.op) as keyof Search;
    const fn = builder[verb] as unknown as (...x: unknown[]) => Search;
    if (typeof fn !== 'function') {
      throw new Error(`no search verb ${call.op}`);
    }
    // A trailing plain object is the call's options; nested calls are already
    // values, and `after`'s list is a list.
    const last = call.args?.[call.args.length - 1];
    const opts = !isCall(last) ? options(last) : undefined;
    builder = fn.apply(builder, opts === undefined ? a : [...a.slice(0, -1), opts]);
  }
  return builder;
}

describe('typescript_builders_match_corpus', () => {
  it('has the corpus', () => {
    expect(cases.map((c) => c.name)).toEqual(
      expect.arrayContaining([
        'schema_notes',
        'filter_term',
        'filter_bool_nested',
        'filter_range_open',
        'filter_ids',
        'search_text_filter_highlight_select',
        'search_vector_ann_params',
        'search_hybrid_rrf',
        'search_hybrid_dbsf',
        'search_hybrid_weighted',
        'search_after_page',
        'search_expand',
      ]),
    );
  });

  it.each(cases)('$name', (c) => {
    switch (c.builder) {
      case 'schema':
        expect(schema(c.spec as Call[])).toEqual(c.expect);
        return;
      case 'filter': {
        const built = filter((c.spec as Call).op, args(c.spec as Call));
        expect(toJson(QuerySchema, fromJson(QuerySchema, built))).toEqual(
          toJson(QuerySchema, fromJson(QuerySchema, c.expect)),
        );
        return;
      }
      case 'search': {
        const built = search(c.spec as { on: string; calls: Call[] }).build();
        expect(toJson(SearchRequestSchema, built)).toEqual(
          toJson(SearchRequestSchema, fromJson(SearchRequestSchema, c.expect)),
        );
        return;
      }
    }
  });

  it('refuses a weighted fusion with the wrong number of weights before any call', () => {
    const builder = Search.on('notes')
      .text('refund', { fields: ['body'] })
      .vector('embedding', [1, 0, 0, 0])
      .fuse(weighted(1));
    expect(() => builder.build()).toThrow(InvalidArgumentError);
    try {
      builder.build();
    } catch (error) {
      expect((error as InvalidArgumentError).metadata.field).toBe('fusion.weighted_sum.weights');
    }
  });

  it('is pure: a verb leaves the builder it was called on unchanged', () => {
    const base = Search.on('notes').text('refund', { fields: ['body'] });
    const limited = base.limit(2);
    expect(base.toJson().limit).toBeUndefined();
    expect(limited.toJson().limit).toBe(2);
  });
});
