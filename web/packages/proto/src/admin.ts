// `loams.admin.v1` in one module (API2 Task 7): the package is six proto files
// (common, org, project, agent, key and audit), and protoc-gen-es writes one
// TypeScript module per file. Every name is unique across the package, so
// they re-export together as `@loams/proto/admin`, which is the module the SDK
// facade imports the package from. Each file is also exported on its own path
// (`@loams/proto/admin/org`).

export * from './gen/loams/admin/v1/agent_pb.js';
export * from './gen/loams/admin/v1/audit_pb.js';
export * from './gen/loams/admin/v1/common_pb.js';
export * from './gen/loams/admin/v1/key_pb.js';
export * from './gen/loams/admin/v1/org_pb.js';
export * from './gen/loams/admin/v1/project_pb.js';
