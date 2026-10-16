// The module surface: one method per facade binding.
//
// This is the layer D606 says is generated. **For Swift it is the Q604
// hand-written fallback** (see `Facade.swift`'s header), and it is the file the
// facade renderer would replace: every method below resolves its binding from
// ``loamsBinding(module:call:)`` and hands it to the one ``CallInvoker``, so there
// is no RPC path and no retry class in this file. That is what makes annotating a
// proto enough to add an SDK method, and it is why ``ConformanceTests`` can
// assert that every binding in the catalogue has a method by checking that
// resolving each one succeeds.

import Foundation

/// Resolves a binding or fails with a message naming the module and the call.
///
/// The one place a missing binding is reported. A caller that asks for a call the
// catalogue does not have gets an `internal` failure naming both — which is a bug
/// in the facade, not in the caller's code, so `internal` is the honest code
/// (D611).
func loamsRequireBinding(module: String, call: String) throws -> CallBinding {
    guard let binding = loamsBinding(module: module, call: call) else {
        throw LoamsError.internalError(
            "",
            "loams: \(module).\(call) has no generated binding; the proto is missing the "
                + "loams.options.v1 annotation (Q604)"
        )
    }
    return binding
}

/// `loams.instance` — what this instance is, and who the caller is on it.
public struct InstanceModule: Sendable {
    let invoker: CallInvoker

    public init(invoker: CallInvoker) {
        self.invoker = invoker
    }

    /// What this instance is and which packages it serves.
    ///
    /// This is the first thing any client calls: it needs no credential, and its
    /// `services[]` is what feature detection reads (R5).
    public func getInstance(options: CallOptions = .init()) async throws -> GetInstanceResponse {
        let binding = try loamsRequireBinding(module: "instance", call: "GetInstance")
        return try await invoker.unary(
            binding: binding,
            request: GetInstanceRequest(),
            overrides: options
        )
    }

    /// Who the caller is on this instance.
    ///
    /// A loopback `loams dev` with auth off admits the caller as the
    /// `dev-loopback` principal; that answer is the recorded corpus's
    /// `instance_who_am_i_*` case.
    public func whoAmI(options: CallOptions = .init()) async throws -> WhoAmIResponse {
        let binding = try loamsRequireBinding(module: "instance", call: "WhoAmI")
        return try await invoker.unary(
            binding: binding,
            request: WhoAmIRequest(),
            overrides: options
        )
    }
}

/// `loams.live` — the live sync session half.
///
/// Its package is unstable, so its wire contract may still change (§44 §10.3).
/// `loams.live.v1` is **not served** in the standard variant, so every method here
/// answers `unimplemented` with `reason = feature_not_in_variant` — which is the
/// corpus's `live_query_*` and `live_watch` cases.
public struct LiveModule: Sendable {
    let invoker: CallInvoker

    public init(invoker: CallInvoker) {
        self.invoker = invoker
    }

    /// Changes the query set a watch is over.
    ///
    /// A mutation: retryable only once it carries an idempotency key, and its
    /// request has no `idempotency_key` field, so it is never keyed (R3).
    public func modifyQuerySet(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live", call: "ModifyQuerySet")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Watches a query set over a server stream.
    ///
    /// The result is an `AsyncSequence` (R7). Passing a `resume` turns on
    /// re-opening from the last applied cursor; without one, a broken stream is
    /// **reported** rather than spun on.
    ///
    ///     for try await transition in client.live.watch(request, resume: resume) {
    ///         apply(transition)
    ///     }
    public func watch(
        _ request: DynamicMessage,
        resume: StreamResume<DynamicMessage, DynamicMessage>? = nil
    ) async throws -> ResumableServerStream<DynamicMessage, DynamicMessage> {
        let binding = try loamsRequireBinding(module: "live", call: "Watch")
        var stream: ResumableServerStream<DynamicMessage, DynamicMessage> = try await invoker.serverStream(
            binding: binding,
            request: request
        )
        stream.policy = resume
        return stream
    }

    /// `loams.live.admin` — deploys, rollbacks, index status and function logs.
    ///
    /// A separate service in the same package (`LiveAdminService`), served on the
    /// engine's main port behind its authentication (LV1 Task 12).
    public var admin: LiveAdminModule { LiveAdminModule(invoker: invoker) }
}

/// `loams.live.admin` — Live administration: deploy, roll back, index status and
/// function logs (`loams.live.v1.LiveAdminService`, LV1 Task 10).
///
/// Its package is unstable, so its wire contract may still change (§44 §10.3).
/// It is the TypeScript and Rust SDKs' `live.admin` module, call for call. The
/// engine serves it on its main Connect port behind the same authentication as
/// every other service, so a client with authentication on needs a credential.
public struct LiveAdminModule: Sendable {
    let invoker: CallInvoker

    public init(invoker: CallInvoker) {
        self.invoker = invoker
    }

    /// Deploys an app bundle and its schema (`DeployRequest`: `bundle`, `schema`,
    /// `expected_active`, `message`, `dry_run`).
    ///
    /// Its request has **no** `idempotency_key` field, so R3 leaves it alone and it
    /// is never retried. Keying it would invent a field the schema does not have.
    /// A deploy that races another answers `live_deploy_conflict`.
    public func deploy(_ request: DynamicMessage, options: CallOptions = .init()) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "Deploy")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// One page of an app's deployments, newest first (AIP-158).
    ///
    /// Retry-safe. ``listAllDeployments(_:options:)`` follows the tokens for you.
    public func listDeployments(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "ListDeployments")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Every deployment, across every page, as the `deployments` items.
    public func listAllDeployments(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) -> AsyncThrowingStream<JSONValue, any Error> {
        let invoker = self.invoker
        let binding = loamsBinding(module: "live.admin", call: "ListDeployments")
            ?? CallBinding(
                module: "live.admin", name: "ListDeployments", protoName: "listDeployments",
                method: "ListDeployments", rpc: LoamsService.liveAdmin + "/ListDeployments",
                service: LoamsService.liveAdmin, package: "loams.live.v1",
                idempotency: .none, retry: .safe, streaming: .unary
            )
        let field = binding.pagination?.itemsField ?? "deployments"
        return loamsPaginate(
            binding: binding,
            fetch: { (page: DynamicMessage, options: CallOptions) async throws -> DynamicMessage in
                try await invoker.unary(binding: binding, request: page, overrides: options)
            },
            request: request,
            items: { loamsItems($0, field: field) },
            options: options
        )
    }

    /// One deployment by id. Retry-safe.
    public func getDeployment(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "GetDeployment")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Makes a deployment the active one: a rollback, or a roll forward.
    ///
    /// A mutation with no idempotency key, so it is never retried.
    public func activateDeployment(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "ActivateDeployment")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Where each index's backfill is. Retry-safe.
    public func getIndexStatus(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "GetIndexStatus")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Registers an app in the node's directory (`CreateAppRequest`).
    ///
    /// An existing name answers `live_app_exists`; a node without the app
    /// directory answers `live_app_disabled`. Never retried: it has no
    /// idempotency key. Address an app on other calls with ``CallOptions/liveApp(_:_:)``.
    public func createApp(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "CreateApp")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// One page of the node's apps (AIP-158: `apps`, `next_page_token`). Retry-safe.
    public func listApps(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "ListApps")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// One app by name. Retry-safe.
    public func getApp(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "GetApp")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Updates an app's settings. A mutation with no idempotency key, so it is never retried.
    public func updateApp(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "UpdateApp")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Removes an app from the directory. The server's own app (`--live-app`)
    /// cannot be deleted. Never retried.
    public func deleteApp(
        _ request: DynamicMessage,
        options: CallOptions = .init()
    ) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "live.admin", call: "DeleteApp")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Tails the app's function logs over a server stream (R7).
    public func tailLogs(
        _ request: DynamicMessage,
        resume: StreamResume<DynamicMessage, DynamicMessage>? = nil
    ) async throws -> ResumableServerStream<DynamicMessage, DynamicMessage> {
        let binding = try loamsRequireBinding(module: "live.admin", call: "TailLogs")
        var stream: ResumableServerStream<DynamicMessage, DynamicMessage> = try await invoker.serverStream(
            binding: binding,
            request: request
        )
        stream.policy = resume
        return stream
    }
}

/// `loams.tables` — the table half of `loams.live.v1` (design §44 §7.2).
///
/// A second facade name for the same RPCs, which is why
/// ``LoamsSystem/availableModule(_:)`` answers the same for `tables` and `live`.
public struct TablesModule: Sendable {
    let invoker: CallInvoker

    public init(invoker: CallInvoker) {
        self.invoker = invoker
    }

    /// Queries the live tables.
    public func query(_ request: DynamicMessage, options: CallOptions = .init()) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "tables", call: "Query")
        return try await invoker.unary(binding: binding, request: request, overrides: options)
    }

    /// Mutates the live tables.
    ///
    /// The one call whose request declares `idempotency_key`
    /// (``CallBinding/takesIdempotencyKey``), so R3 mints one before the first
    /// attempt and reuses it on every retry — which is what makes this the SDK's
    /// one retryable mutation.
    public func mutate(_ request: DynamicMessage, options: CallOptions = .init()) async throws -> DynamicMessage {
        let binding = try loamsRequireBinding(module: "tables", call: "Mutate")
        return try await invoker.unary(
            binding: binding,
            request: request,
            suppliedIdempotencyKey: options.idempotencyKey,
            overrides: options
        )
    }

    /// Deploys the live tables. **Moved:** this is `live.admin.deploy` now.
    ///
    /// LV1 Task 10 moved `Deploy` to `loams.live.v1.LiveAdminService` and the
    /// engine no longer serves `LiveService/Deploy`, so this forwards there. It is
    /// kept so code written against earlier previews keeps working; the TypeScript
    /// and Rust SDKs dropped it.
    @available(*, deprecated, renamed: "LiveAdminModule.deploy", message: "use client.live.admin.deploy")
    public func deploy(_ request: DynamicMessage, options: CallOptions = .init()) async throws -> DynamicMessage {
        try await LiveAdminModule(invoker: invoker).deploy(request, options: options)
    }
}