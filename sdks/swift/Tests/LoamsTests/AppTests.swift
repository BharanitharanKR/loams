// SDKP (docs/plans/2026-10-19-sdk-parity.md §5, L7): the runtime additions R4,
// R11–R14, the Schema, Filter and Search builders, the collection handle and
// the app modules, against the stub transport.

import Foundation
import XCTest
@testable import Loams

private let malformed = [
    "v2:s1/p0@1", "v1:s1/p0", "v1:s01/p0@1", "v1:s1/p0@1,", "", "v1", "s1/p0@1", "v1:,s1/p0@1",
    "v1:s1/p00@1", "v1:s1/p0@01", "v1:s1/p4294967296@1", "v1:s18446744073709551616/p0@1", "v1:s+1/p0@1",
    "v1:s1/p0@-1", "v1:s1p0@1", "v1:S1/P0@1", "v1: s1/p0@1", "v1:s1/p0@1@2", "v1:s/p0@1",
]

private func client(_ transport: StubTransport, namespace: String? = "app", session: Bool = false,
                    timeout: Double = loamsDefaultTimeout) throws -> Loams {
    try Loams(
        Options(endpoint: URL(string: "http://stub.invalid")!, maxRetries: 3, sessionConsistency: session,
                namespace: namespace, timeout: timeout),
        transport: transport
    )
}

/// A transport that never answers until it is cancelled.
private final class HangingTransport: HTTPTransport, @unchecked Sendable {
    let config = TransportConfig(endpoint: URL(string: "http://stub.invalid")!)
    private let lock = NSLock()
    private var seen: [WireRequest] = []
    var requests: [WireRequest] { lock.withLock { seen } }

    func unary(_ request: WireRequest, bearer: String?) async throws -> WireResponse {
        lock.withLock { seen.append(request) }
        try await Task.sleep(nanoseconds: 60_000_000_000)
        return .success(body: [:], trailers: [:])
    }

    func serverStream(_ request: WireRequest, bearer: String?) async throws -> WireStream {
        AsyncThrowingStream { $0.finish() }
    }
}

final class AppRuntimeTests: XCTestCase {
    func test_swift_consistency_token_merge() throws {
        let one = try ConsistencyToken.parse("v1:s7/p3@918274")
        XCTAssertEqual(one.positions, [.init(stream: 7, partition: 3, offset: 918_274)])
        XCTAssertEqual(one.description, "v1:s7/p3@918274")
        let max = "v1:s18446744073709551615/p4294967295@18446744073709551615"
        XCTAssertEqual(try ConsistencyToken.parse(max).description, max)
        XCTAssertEqual(try ConsistencyToken.parse("v1:"), .empty)

        let merged = try ConsistencyToken.parse("v1:s7/p3@10,s1/p0@5")
            .merge(try ConsistencyToken.parse("v1:s7/p3@4,s7/p1@2,s1/p0@9"))
        XCTAssertEqual(merged.description, "v1:s1/p0@9,s7/p1@2,s7/p3@10")
        XCTAssertEqual(merged.offset(stream: 7, partition: 3), 10)
        XCTAssertNil(merged.offset(stream: 1, partition: 1))
        XCTAssertEqual(
            ConsistencyToken([
                .init(stream: 2, partition: 0, offset: 1), .init(stream: 1, partition: 5, offset: 3),
                .init(stream: 2, partition: 0, offset: 8), .init(stream: 1, partition: 5, offset: 2),
            ]).description,
            "v1:s1/p5@3,s2/p0@8"
        )
        for bad in malformed {
            XCTAssertNil(ConsistencyToken.tryParse(bad), bad)
            XCTAssertThrowsError(try ConsistencyToken.parse(bad)) { error in
                XCTAssertEqual(LoamsError.code(of: error), .invalidArgument)
            }
        }
    }

    func test_swift_session_read_your_writes() async throws {
        let stub = StubTransport(replies: [
            .success(["token": .string("v1:s4/p0@7")]),
            .success(["token": .string("v1:s4/p1@2,s4/p0@3")]),
            .success(["documents": .array([]), "readToken": .string("v1:s4/p0@9")]),
            .success([:]),
        ])
        let loams = try client(stub, session: true)
        _ = try await loams.documents.upsert("notes", [Doc(1, ["title": .string("a")])])
        _ = try await loams.documents.delete("notes", ids: [2])
        let afterWrites = await loams.session?.current()
        XCTAssertEqual(afterWrites, "v1:s4/p0@7,s4/p1@2")

        _ = try await loams.documents.get("notes", ids: [1])
        XCTAssertEqual(stub.calls[2].body["consistency"], .object(["atLeast": .string("v1:s4/p0@7,s4/p1@2")]))
        let afterRead = await loams.session?.current()
        XCTAssertEqual(afterRead, "v1:s4/p0@9,s4/p1@2")

        _ = try await loams.documents.get("notes", ids: [1], consistency: .strong)
        XCTAssertEqual(stub.calls[3].body["consistency"], .object(["freshness": .string("FRESHNESS_STRONG")]))

        let plain = StubTransport(replies: [.success([:])])
        let off = try client(plain)
        _ = try await off.documents.get("notes", ids: [1])
        XCTAssertNil(plain.calls[0].body["consistency"])
    }

    func test_swift_default_namespace() async throws {
        let stub = StubTransport(replies: [.success(["name": .string("notes")])])
        let loams = try client(stub)
        _ = try await loams.collections.get("notes")
        _ = try await loams.collections.get("notes", namespace: "other")
        _ = try await loams.collections.get("notes", options: CallOptions(namespace: "per-call"))
        XCTAssertEqual(stub.calls.map { $0.body["namespace"]?.stringValue }, ["app", "other", "per-call"])

        let none = StubTransport(replies: [.success([:])])
        let bare = try client(none, namespace: nil)
        do {
            _ = try await bare.collections.get("notes")
            XCTFail("no namespace went out")
        } catch {
            XCTAssertEqual(LoamsError.code(of: error), .invalidArgument)
            XCTAssertEqual((error as? LoamsError)?.metadata["field"], "namespace")
        }
        XCTAssertEqual(none.calls.count, 0)
    }

    func test_swift_timeout_deadline() async throws {
        let hanging = HangingTransport()
        let loams = try Loams(
            Options(endpoint: URL(string: "http://stub.invalid")!, namespace: "app", timeout: 0.2),
            transport: hanging
        )
        let started = Date()
        do {
            _ = try await loams.collections.get("notes")
            XCTFail("the call outlived its deadline")
        } catch let error as LoamsError {
            XCTAssertEqual(error.code, .deadlineExceeded)
            XCTAssertEqual(error.unknownReason, loamsClientDeadlineExceeded)
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
        XCTAssertEqual(hanging.requests.count, 1, "a spent budget is not retried")
        let sent = Int(hanging.requests[0].headers["connect-timeout-ms"] ?? "") ?? -1
        XCTAssertTrue((1...200).contains(sent), "connect-timeout-ms was \(sent)")

        do {
            _ = try await loams.collections.get("notes", options: CallOptions(timeout: 0.05))
            XCTFail("the per-call deadline was ignored")
        } catch let error as LoamsError {
            XCTAssertEqual(error.metadata["timeout_ms"], "50")
        }
        XCTAssertEqual(loamsDefaultTimeout, 30)
    }

    func test_swift_write_batch_chunks() async throws {
        let docs = (0..<25).map { Doc(.uint(UInt64($0)), ["n": .int($0)]) }
        let stub = StubTransport(replies: [
            .success(["token": .string("v1:s1/p0@10")]),
            .failure(code: .unavailable, message: "restarting", info: nil),
            .success(["token": .string("v1:s1/p1@4")]),
            .success(["token": .string("v1:s1/p0@25")]),
        ])
        let loams = try client(stub)
        let result = try await loams.documents.writeBatch("notes", docs, chunkRows: 10)
        XCTAssertEqual(result.chunks, 3)
        XCTAssertEqual(result.ops, 25)
        XCTAssertEqual(result.token, "v1:s1/p0@25,s1/p1@4")
        let sizes = stub.calls.map { call -> Int in
            if case .array(let ops)? = call.body["ops"] { return ops.count }
            return 0
        }
        XCTAssertEqual(sizes, [10, 10, 10, 5])
        let keys = stub.idempotencyKeys
        XCTAssertEqual(keys.count, 4)
        XCTAssertEqual(keys[1], keys[2], "the retried chunk reuses its key")
        XCTAssertEqual(Set(keys).count, 3)
        XCTAssertEqual(loamsChunks(docs.map(\.op), rows: 10_000, bytes: 1).count, 25, "bytes cut a chunk")

        let failing = StubTransport(replies: [
            .success(["token": .string("v1:s1/p0@10")]),
            .failure(code: .invalidArgument, message: "bad doc", info: nil),
        ])
        let other = try client(failing)
        do {
            _ = try await other.documents.writeBatch("notes", docs, chunkRows: 10)
            XCTFail("a failed chunk did not stop the batch")
        } catch let error as BatchError {
            XCTAssertEqual(error.appliedChunks, 1)
            XCTAssertEqual(error.appliedOps, 10)
            XCTAssertEqual(error.token, "v1:s1/p0@10")
            XCTAssertEqual(error.reason, loamsBatchPartiallyApplied)
        }
        XCTAssertEqual(failing.calls.count, 2)
    }

    func test_swift_streams_subscribe_resumes() async throws {
        actor Script {
            var pages: [[StreamRecord]]
            var asked: [UInt64] = []
            var waits: [Int] = []
            init(_ pages: [[StreamRecord]]) { self.pages = pages }
            func fetch(_ offset: UInt64) -> [StreamRecord] {
                asked.append(offset)
                return pages.isEmpty ? [] : pages.removeFirst()
            }
            func wait(_ ms: Int) { waits.append(ms) }
        }
        @Sendable func record(_ offset: UInt64) -> StreamRecord {
            StreamRecord(partition: 0, offset: offset, value: Data("{\"n\":\(offset)}".utf8))
        }
        let script = Script([[record(0), record(1)], [], [], [record(2), record(3)]])
        let subscription = Subscription(from: 0, sleep: { await script.wait($0) }, fetch: { await script.fetch($0) })
        var seen: [UInt64] = []
        for try await item in subscription {
            seen.append(item.offset)
            if seen.count == 3 { break }
        }
        XCTAssertEqual(seen, [0, 1, 2])
        XCTAssertEqual(subscription.offset, 3)
        let asked = await script.asked
        XCTAssertEqual(asked, [0, 2, 2, 2])
        let waits = await script.waits
        XCTAssertEqual(waits, [100, 200])

        let resumed = Subscription(from: subscription.offset, sleep: { _ in }, fetch: { offset in
            offset == 3 ? [record(3)] : []
        })
        for try await item in resumed {
            XCTAssertEqual(item.offset, 3)
            break
        }
        XCTAssertEqual(resumed.offset, 4)
    }
}

final class AppBuilderTests: XCTestCase {
    private let embedding = [0.1, 0.2, 0.3, 0.4]
    private let textRefund: JSONValue = .object(["text": .object([
        "query": .object(["match": .object(["field": .string("body"), "text": .string("refund")])]),
        "k": .int(10),
    ])])

    func test_swift_builders_match_corpus() throws {
        let schema = Schema().text("title").text("body").keyword("tags").i64("n").vector("embedding", dim: 4).json
        let text: (String) -> JSONValue = { name in
            .object([
                "name": .string(name), "source_path": .string(name),
                "kind": .object(["text": .object(["analyzer": .string("standard"), "positions": .bool(true)])]),
                "indexed": .bool(true), "fast": .bool(false),
            ])
        }
        XCTAssertEqual(schema, .object([
            "fields": .array([
                text("title"), text("body"),
                .object(["name": .string("tags"), "source_path": .string("tags"), "kind": .string("keyword"),
                         "indexed": .bool(true), "fast": .bool(true)]),
                .object(["name": .string("n"), "source_path": .string("n"), "kind": .string("i64"),
                         "indexed": .bool(true), "fast": .bool(true)]),
            ]),
            "vectors": .array([.object(["name": .string("embedding"), "dim": .int(4), "distance": .string("cosine")])]),
            "sparse_vectors": .array([]),
            "dynamic": .string("ignore"),
            "max_fields": .int(1000),
        ]))

        XCTAssertEqual(Filter.eq("tags", "policy").json,
                       .object(["term": .object(["field": .string("tags"), "value": .string("policy")])]))
        XCTAssertEqual(
            (Filter.eq("tags", "policy") && (Filter.range("n", gte: 2) || !Filter.exists("archived"))).json,
            .object(["bool": .object(["must": .array([
                .object(["term": .object(["field": .string("tags"), "value": .string("policy")])]),
                .object(["bool": .object(["should": .array([
                    .object(["range": .object(["field": .string("n"), "gte": .int(2)])]),
                    .object(["bool": .object(["mustNot": .array([.object(["exists": .object(["field": .string("archived")])])])])]),
                ])])]),
            ])])])
        )
        XCTAssertEqual(Filter.range("n", gt: 1).json, .object(["range": .object(["field": .string("n"), "gt": .int(1)])]))
        XCTAssertEqual(Filter.ids([1, "k-str"]).json, .object(["ids": .array([.int(1), .string("k-str")])]))

        let text3 = Search.on("notes").text("refund", fields: ["body"]).filter(Filter.eq("tags", "policy"))
            .highlight("body").select(include: ["title"]).sort(.score(), .pk()).limit(2).json
        XCTAssertEqual(text3["retrievers"], .array([textRefund]))
        XCTAssertEqual(text3["sort"], .array([
            .object(["score": .object(["order": .string("SORT_ORDER_DESC")])]),
            .object(["pk": .object(["order": .string("SORT_ORDER_ASC")])]),
        ]))
        XCTAssertEqual(text3["select"], .object(["source": .object(["include": .array([.string("title")]), "exclude": .array([])])]))
        XCTAssertEqual(text3["highlight"], .object(["fields": .array([.object(["field": .string("body")])])]))
        XCTAssertEqual(text3["limit"], .int(2))

        let vector = Search.on("notes").vector("embedding", embedding, k: 3, params: ["ef": .int(64)]).limit(3).json
        XCTAssertEqual(vector["retrievers"], .array([.object(["vector": .object([
            "field": .string("embedding"), "query": .array(embedding.map { .number($0) }), "k": .int(3),
            "params": .object(["ef": .int(64)]),
        ])])]))

        let hybrid = { (fusion: Fusion) in
            Search.on("notes").text("refund", fields: ["body"]).vector("embedding", self.embedding).limit(3).fuse(fusion).json
        }
        XCTAssertEqual(hybrid(.rrf(k: 60))["fusion"], .object(["rrf": .object(["k": .int(60)])]))
        XCTAssertEqual(hybrid(.dbsf)["fusion"], .object(["dbsf": .object([:])]))
        XCTAssertEqual(hybrid(.weighted([0.7, 0.3]))["fusion"],
                       .object(["weightedSum": .object(["weights": .array([.number(0.7), .number(0.3)])])]))
        XCTAssertEqual(hybrid(.dbsf)["retrievers"], .array([textRefund, .object(["vector": .object([
            "field": .string("embedding"), "query": .array(embedding.map { .number($0) }), "k": .int(10),
        ])])]))

        let after = Search.on("notes").text("refund", fields: ["body"]).sort(.score(), .pk())
            .after([.number(1.5), .int(3)]).limit(2).json
        XCTAssertEqual(after["searchAfter"], .array([.number(1.5), .int(3)]))

        let expand = Search.on("notes").text("refund", fields: ["body"]).expand(graph: "notes_graph", fromField: "id").json
        XCTAssertEqual(expand["expandSpec"], .object(["graph": .string("notes_graph"), "fromField": .string("id"), "hops": .int(1)]))
    }

    func test_swift_filter_ir_matches_the_engine_mapping() {
        let ir = Filter.ir(Filter.and(Filter.all(), Filter.not(Filter.isNull("x")), Filter.ids([1, "a"])))
        XCTAssertEqual(ir, .object(["bool": .object(["must": .array([
            .string("match_all"),
            .object(["bool": .object(["must_not": .array([.object(["is_null": .object(["field": .string("x")])])])])]),
            .object(["ids": .array([.int(1), .string("a")])]),
        ])])]))
    }

    func test_swift_collection_handle_delegates() async throws {
        let stub = StubTransport(replies: [.success([:])])
        let loams = try client(stub)
        let notes = loams.collection("notes", namespace: "other")
        _ = try await notes.upsert([Doc(1, ["title": .string("a")])])
        _ = try await notes.get([1])
        _ = try await notes.delete([1])
        _ = try await notes.patch(1, set: ["title": .string("b")], unset: ["body"])
        _ = try await notes.count(where: Filter.eq("tags", "x"))
        _ = try await notes.search { $0.text("refund", fields: ["body"]) }
        _ = try await notes.vectorSearch(embedding, field: "embedding")
        _ = try await notes.info()
        _ = try await notes.drop()
        let c = "loams.collection.v1."
        XCTAssertEqual(stub.calls.map(\.rpc), [
            c + "DocumentService/WriteDocuments", c + "DocumentService/GetDocuments",
            c + "DocumentService/WriteDocuments", c + "DocumentService/WriteDocuments",
            c + "DocumentService/CountDocuments", c + "QueryService/Search", c + "QueryService/Search",
            c + "CollectionService/GetCollection", c + "CollectionService/DropCollection",
        ])
        for call in stub.calls {
            XCTAssertEqual(call.body["namespace"], .string("other"))
            XCTAssertEqual(call.body["collection"], .string("notes"))
        }
        XCTAssertEqual(stub.calls[4].body["filter"],
                       .object(["term": .object(["field": .string("tags"), "value": .string("x")])]))

        _ = try await loams.collection("notes").info()
        XCTAssertEqual(stub.calls.last?.body["namespace"], .string("app"))
    }

    func test_swift_search_query_all_requires_tiebreak() async throws {
        let stub = StubTransport(replies: [.success([:])])
        let loams = try client(stub)
        XCTAssertThrowsError(try loams.search.queryAll(Search.on("notes").text("refund").sort(.score()))) { error in
            XCTAssertEqual((error as? LoamsError)?.metadata["field"], "sort")
        }
        XCTAssertEqual(stub.calls.count, 0)

        let paged = StubTransport(replies: [
            .success(["hits": .array([
                .object(["pk": .object(["uint": .string("1")]), "sortValues": .array([.int(2), .int(1)])]),
                .object(["pk": .object(["uint": .string("2")]), "sortValues": .array([.int(1), .int(2)])]),
            ])]),
            .success(["hits": .array([
                .object(["pk": .object(["uint": .string("3")]), "sortValues": .array([.number(0.5), .int(3)])]),
            ])]),
        ])
        let other = try client(paged)
        var ids: [String] = []
        for try await hit in try other.search.queryAll(Search.on("notes").text("refund").sort(.score(), .pk()).limit(2)) {
            if case .object(let fields) = hit, let id = DocumentID(json: fields["pk"]) { ids.append(id.text) }
        }
        XCTAssertEqual(ids, ["1", "2", "3"])
        XCTAssertEqual(paged.calls.count, 2)
        XCTAssertEqual(paged.calls[1].body["searchAfter"], .array([.int(1), .int(2)]))
    }

    func test_swift_app_bindings() throws {
        let safe = [
            ("namespaces", "Create"), ("namespaces", "List"), ("collections", "Create"), ("collections", "List"),
            ("collections", "Get"), ("collections", "Drop"), ("collections", "AddFields"),
            ("collections", "ListVersions"), ("collections", "UpdateAliases"), ("collections", "SetHot"),
            ("collections", "Warm"), ("documents", "Get"), ("documents", "Scroll"), ("documents", "Count"),
            ("search", "Query"), ("vector", "Search"), ("graph", "Get"), ("graph", "List"), ("graph", "Schema"),
            ("graph", "Explain"),
        ]
        for (module, name) in safe {
            XCTAssertEqual(try loamsRequireBinding(module: module, call: name).retry, .safe, "\(module).\(name)")
        }
        let manual = [
            ("collections", "Scan"), ("documents", "Write"), ("documents", "DeleteByFilter"),
            ("documents", "PatchByFilter"), ("graph", "Create"), ("graph", "Update"), ("graph", "Delete"),
            ("graph", "Query"), ("graph", "QueryBatch"), ("graph", "QueryStream"),
        ]
        for (module, name) in manual {
            XCTAssertEqual(try loamsRequireBinding(module: module, call: name).retry, .manual, "\(module).\(name)")
        }
        XCTAssertEqual(try loamsRequireBinding(module: "search", call: "Query").rpc,
                       try loamsRequireBinding(module: "vector", call: "Search").rpc)
        XCTAssertEqual(try loamsRequireBinding(module: "documents", call: "Write").rpc,
                       "loams.collection.v1.DocumentService/WriteDocuments")
        XCTAssertTrue(try loamsRequireBinding(module: "documents", call: "Write").takesIdempotencyKey)
        XCTAssertEqual(try loamsRequireBinding(module: "graph", call: "QueryStream").streaming, .server)
        XCTAssertTrue(try XCTUnwrap(loamsModule("graph")).unstable)
    }

    func test_swift_graph_read_only_query_is_unkeyed_and_retried() async throws {
        let stub = StubTransport(replies: [
            .failure(code: .unavailable, message: "restarting", info: nil),
            .success(["rows": .object(["columns": .array([.string("n")])])]),
        ])
        let loams = try client(stub)
        _ = try await loams.graph.query("g", "MATCH (n) RETURN n", readOnly: true)
        XCTAssertEqual(stub.calls.count, 2)
        XCTAssertNil(stub.calls[0].body[ProtoField.idempotencyKey])
        XCTAssertEqual(stub.calls[0].body["namespace"], .string("app"))

        let write = StubTransport(replies: [.success([:])])
        let writer = try client(write)
        _ = try await writer.graph.query("g", "INSERT (:Note {id: $id})", parameters: ["id": GraphValue.int(1)])
        XCTAssertEqual(write.idempotencyKeys.count, 1)
    }
}
