// @loams/console-host/testing: an in-memory app-protos mock for tests and the
// console's demo mode.

// Wrap a component under test that reads server state the way the console does.
export { createQueryClient, ServerStateProvider } from '../root.js';
export {
  createMockControl,
  createMockTransport,
  type MockControl,
  type MockOptions,
} from './mock.js';
