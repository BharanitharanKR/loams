// @loams/agentd-client: the loams-agentd wire contract for TypeScript
// (design §50 §8). Everything here is safe for the renderer: generated
// types, the method table, typed request builders and the session reducer.
// The Node client for Electron main (`./client`) arrives with Task 10.

export * from './reducer';
export * from './requests';
export * from './types';
