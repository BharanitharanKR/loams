// B17: `TransportConfig.extraHeaders` reach the wire on a unary call and on a
// server stream through the real `ConnectTransport`, a call's header replaces
// the client's of the same name, and neither displaces the bearer.

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import XCTest
@testable import Loams

/// Answers every request with an empty Connect message and records its headers.
final class HeaderRecordingProtocol: URLProtocol, @unchecked Sendable {
    nonisolated(unsafe) static var seen: [[String: String]] = []
    static let lock = NSLock()

    static func reset() {
        lock.lock()
        seen = []
        lock.unlock()
    }

    static func recorded() -> [[String: String]] {
        lock.lock()
        defer { lock.unlock() }
        return seen
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        var headers: [String: String] = [:]
        for (name, value) in request.allHTTPHeaderFields ?? [:] {
            headers[name.lowercased()] = value
        }
        Self.lock.lock()
        Self.seen.append(headers)
        Self.lock.unlock()

        let streaming = (headers["content-type"] ?? "").hasPrefix("application/connect+")
        let body: Data
        let contentType: String
        if streaming {
            // One message, then a clean end frame.
            body = ConnectTransport.envelope(payload: Data(#"{"sessionId":"1"}"#.utf8), flags: 0)
                + ConnectTransport.envelope(payload: Data("{}".utf8), flags: 0x02)
            contentType = "application/connect+json"
        } else {
            body = Data("{}".utf8)
            contentType = "application/json"
        }
        let response = HTTPURLResponse(
            url: request.url!,
            statusCode: 200,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": contentType]
        )!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

final class HeadersTests: XCTestCase {
    private func transport() -> ConnectTransport {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [HeaderRecordingProtocol.self]
        let config = TransportConfig(
            endpoint: URL(string: "http://fixture.invalid")!,
            extraHeaders: [
                loamsLiveAppHeader: "blog",
                "x-tenant": "acme",
                "Authorization": "Bearer not-the-key",
            ]
        )
        return ConnectTransport(config: config, session: URLSession(configuration: configuration))
    }

    override func setUp() {
        super.setUp()
        HeaderRecordingProtocol.reset()
    }

    func testExtraHeadersReachTheWireOnUnaryAndStream() async throws {
        let wire = transport()
        let rpc = "loams.live.v1.LiveAdminService/GetApp"

        _ = try await wire.unary(
            WireRequest(rpc: rpc, body: [:], serverStreaming: false),
            bearer: "real-key"
        )
        _ = try await wire.unary(
            WireRequest(rpc: rpc, body: [:], serverStreaming: false, headers: [loamsLiveAppHeader: "shop"]),
            bearer: "real-key"
        )
        let stream = try await wire.serverStream(
            WireRequest(rpc: "loams.live.v1.LiveService/Watch", body: [:], serverStreaming: true),
            bearer: "real-key"
        )
        var messages = 0
        for try await _ in stream { messages += 1 }
        XCTAssertEqual(messages, 1)

        let seen = HeaderRecordingProtocol.recorded()
        XCTAssertEqual(seen.count, 3)
        for headers in seen {
            XCTAssertEqual(headers["x-tenant"], "acme")
            XCTAssertEqual(headers["authorization"], "Bearer real-key", "extraHeaders displaced the bearer")
        }
        XCTAssertEqual(seen[0][loamsLiveAppHeader], "blog")
        XCTAssertEqual(seen[1][loamsLiveAppHeader], "shop", "the call's header must replace the client's")
        XCTAssertEqual(seen[2][loamsLiveAppHeader], "blog")
    }
}
