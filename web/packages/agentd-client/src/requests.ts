// Typed request builders (design §50 §8.3). A request names a method of the
// generated table and carries that method's params type, so a call that
// compiles here parses on the daemon (`ts_requests_parse` checks the
// recorded set in Rust).

import type { MethodName, Params } from './gen/methods';

/** One call: the `method` and `params` of a `{id, method, params}` frame. */
export interface Request<M extends MethodName = MethodName> {
  method: M;
  params: Params<M>;
}

/** Build a request; `params` must be the method's params type. */
export function request<M extends MethodName>(method: M, params: Params<M>): Request<M> {
  return { method, params };
}
