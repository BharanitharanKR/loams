// SDKP's `typescript_consistency_token_merge` and
// `typescript_session_read_your_writes` (runtime contract R4 as amended).
//
// The first runs the token corpus, `sdks/fixtures/tokens.json`, which is the
// server's own `crates/loams-collection/src/token.rs` cases written down once
// for every SDK. The second drives the session store through the call path: a
// write's token goes into the session, and the next read whose request has a
// `consistency` field is sent `at_least = session` in that field.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DocumentService } from '@loams/proto/documents';
import { describe, expect, it } from 'vitest';
import { CallInvoker } from '../src/runtime/call.js';
import { ConsistencySession, ConsistencyToken } from '../src/runtime/consistency.js';
import { InvalidArgumentError } from '../src/runtime/errors.js';
import { GET, WRITE, messageTransport } from './helpers/app.js';
import { FIXTURES } from './helpers/server.js';

interface TokenCorpus {
  parse: { name: string; text: string; items: [string, number, string][] }[];
  merge: { name: string; a: string; b: string; expect: string }[];
  malformed: string[];
}

const corpus = JSON.parse(readFileSync(join(FIXTURES, 'tokens.json'), 'utf8')) as TokenCorpus;

describe('typescript_consistency_token_merge', () => {
  it.each(corpus.parse)('parses and prints $name', ({ text, items }) => {
    const token = ConsistencyToken.parse(text);
    expect(token.positions.map((p) => [p.stream.toString(), p.partition, p.offset.toString()])).toEqual(
      items,
    );
    expect(token.toString()).toBe(text);
  });

  it.each(corpus.merge)('merges $name', ({ a, b, expect: merged }) => {
    expect(ConsistencyToken.parse(a).merge(b).toString()).toBe(merged);
    // Merging is commutative once normalised.
    expect(ConsistencyToken.parse(b).merge(a).toString()).toBe(merged);
    expect(ConsistencyToken.parse(a).merge(b).equals(merged)).toBe(true);
  });

  it.each(corpus.malformed.map((text) => ({ text })))('refuses $text', ({ text }) => {
    expect(() => ConsistencyToken.parse(text)).toThrow(InvalidArgumentError);
    expect(ConsistencyToken.isValid(text)).toBe(false);
  });

  it('merges every token a session records', () => {
    const session = new ConsistencySession();
    for (const { a, b, expect: merged } of corpus.merge) {
      session.clear();
      session.record(a);
      session.record(b);
      expect(session.current()).toBe(merged === 'v1:' ? 'v1:' : merged);
    }
  });
});

describe('typescript_session_read_your_writes', () => {
  it('sends a write token as consistency.at_least on the next read', async () => {
    const session = new ConsistencySession();
    const { sent, transport } = messageTransport((method) =>
      method === 'WriteDocuments' ? { token: 'v1:s4/p0@12' } : { readToken: 'v1:s4/p0@12' },
    );
    const invoker = new CallInvoker(transport as never, undefined, 0, session, { namespace: 'app' });
    invoker.register('loams.collection.v1.DocumentService', DocumentService);

    await invoker.unary(WRITE, { collection: 'notes', ops: [] });
    expect(session.current()).toBe('v1:s4/p0@12');

    await invoker.unary(GET, { collection: 'notes', ids: [] });
    const read = sent[1]?.input as { consistency?: { atLeast?: string } };
    expect(read.consistency?.atLeast).toBe('v1:s4/p0@12');
    // The field carries it, so the header does not repeat it.
    expect(sent[1]?.headers['loams-consistency']).toBeUndefined();
  });

  it('merges two writes, and an explicit consistency wins', async () => {
    const session = new ConsistencySession();
    let n = 0;
    const { sent, transport } = messageTransport((method) => {
      if (method !== 'WriteDocuments') {
        return {};
      }
      n += 1;
      return { token: n === 1 ? 'v1:s4/p0@12' : 'v1:s4/p1@3' };
    });
    const invoker = new CallInvoker(transport as never, undefined, 0, session, { namespace: 'app' });
    invoker.register('loams.collection.v1.DocumentService', DocumentService);

    await invoker.unary(WRITE, { collection: 'notes', ops: [] });
    await invoker.unary(WRITE, { collection: 'notes', ops: [] });
    expect(session.current()).toBe('v1:s4/p0@12,s4/p1@3');

    await invoker.unary(GET, { collection: 'notes', ids: [], consistency: { freshness: 2 } });
    expect((sent[2]?.input as { consistency: unknown }).consistency).toEqual({ freshness: 2 });
  });

  it('reads the loams-consistency-token response header too', async () => {
    const session = new ConsistencySession();
    const transport = {
      unary(_m: unknown, _s: unknown, _t: unknown, _h: unknown, _input: unknown) {
        return Promise.resolve({
          message: {},
          header: new Headers({ 'loams-consistency-token': 'v1:s9/p0@1' }),
        }) as never;
      },
      stream() {
        throw new Error('no streams');
      },
    };
    const invoker = new CallInvoker(transport as never, undefined, 0, session, { namespace: 'app' });
    invoker.register('loams.collection.v1.DocumentService', DocumentService);
    await invoker.unary(WRITE, { collection: 'notes', ops: [] });
    expect(session.current()).toBe('v1:s9/p0@1');
  });

  it('opts a call out with session: false', async () => {
    const session = new ConsistencySession();
    session.record('v1:s1/p0@1');
    const { sent, transport } = messageTransport(() => ({ readToken: 'v1:s1/p0@9' }));
    const invoker = new CallInvoker(transport as never, undefined, 0, session, { namespace: 'app' });
    invoker.register('loams.collection.v1.DocumentService', DocumentService);
    await invoker.unary(GET, { collection: 'notes', ids: [] }, { consistency: { session: false } });
    expect((sent[0]?.input as { consistency?: unknown }).consistency).toBeUndefined();
    expect(session.current()).toBe('v1:s1/p0@1');
  });
});
