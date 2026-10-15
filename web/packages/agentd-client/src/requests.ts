// Typed request builders (design §50 §8.3). A request names a method of the
// generated table and carries that method's params type, so a call that
// compiles here parses on the daemon (`ts_requests_parse` checks the
// recorded set in Rust).

import type { MethodName, Params } from './gen/methods';

/**
 * One call: the `method` and `params` of a `{id, method, params}` frame.
 * Without a type argument it is the union over every method, so `method` and
 * `params` stay paired (a `Hello` cannot carry another method's params).
 */
export type Request<M extends MethodName = MethodName> = {
  [K in M]: { method: K; params: Params<K> };
}[M];

/** Build a request; `params` must be the method's params type. */
export function request<M extends MethodName>(method: M, params: Params<M>): Request<M> {
  return { method, params };
}
