// @loams/agentd-client: the loams-agentd wire contract for TypeScript
// (design §50 §8). Everything here is safe for the renderer: generated
// types, the method table, typed request builders and the session and
// transcript reducers. The Node client for Electron main (`./client`) arrives
// with DD1 Task 10; the console's Connect surface is `./connect` (SD1 Task 6).

export * from './reducer';
export * from './requests';
export * from './transcript';
export * from './types';
