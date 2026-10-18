// B17: `LoamsClientOptions.Headers` reach the wire on unary calls and on server
// streams, a call's header of the same name replaces the client's rather than
// sitting beside it, and neither can displace the bearer.

using System.Net;
using System.Text;
using Loams.Live.V1;
using Loams.Tests.Support;
using Xunit;

namespace Loams.Tests;

public sealed class HeadersTests
{
    private static LoamsClient ClientOver(StubHandler handler) => new(new LoamsClientOptions
    {
        Endpoint = "https://fixture.invalid",
        Codec = Codec.Json,
        Handler = handler,
        NoRetries = true,
        Auth = new StaticTokenSource("real-key"),
        Headers = new Dictionary<string, string>
        {
            [CallOptions.LiveAppHeader] = "blog",
            ["x-tenant"] = "acme",
            ["Authorization"] = "Bearer not-the-key",
        },
    });

    [Fact]
    public async Task Client_headers_reach_the_wire_on_a_unary_call()
    {
        var handler = new StubHandler(StubHandler.ConnectJson(HttpStatusCode.OK, "{}"));
        using var client = ClientOver(handler);

        await client.LiveAdmin.GetAppAsync(new GetAppRequest());

        var request = Assert.Single(handler.Requests);
        Assert.Equal("blog", request.Headers[CallOptions.LiveAppHeader]);
        Assert.Equal("acme", request.Headers["x-tenant"]);
        Assert.Equal("real-key", request.Bearer);
    }

    [Fact]
    public async Task A_calls_header_replaces_the_clients_header_of_the_same_name()
    {
        var handler = new StubHandler(StubHandler.ConnectJson(HttpStatusCode.OK, "{}"));
        using var client = ClientOver(handler);

        await client.LiveAdmin.GetAppAsync(new GetAppRequest(), CallOptions.None.WithLiveApp("shop"));

        var request = Assert.Single(handler.Requests);
        // The handler joins repeated values with a comma, so `blog,shop` here is the
        // bug: both apps on one request.
        Assert.Equal("shop", request.Headers[CallOptions.LiveAppHeader]);
        Assert.Equal("acme", request.Headers["x-tenant"]);
    }

    [Fact]
    public async Task Client_headers_reach_the_wire_on_a_server_stream()
    {
        // JSON, because the client speaks JSON: a stream's frames are in the
        // client's codec, and its end frame is JSON in every codec.
        var transition = Encoding.UTF8.GetBytes("{\"sessionId\":\"1\"}");
        var end = Encoding.UTF8.GetBytes("{}");
        var body = StubHandler.Frame(transition)
            .Concat(StubHandler.Frame(end, EnvelopeFlags.EndOfStream))
            .ToArray();
        var handler = new StubHandler(new StubReply(HttpStatusCode.OK, body, ContentTypes.ConnectStreamJson));
        using var client = ClientOver(handler);

        var seen = new List<string>();
        await foreach (var message in client.Live.WatchAsync(new WatchRequest()))
        {
            seen.Add(message.SessionId);
        }

        Assert.Equal(["1"], seen);
        var request = Assert.Single(handler.Requests);
        Assert.Equal("blog", request.Headers[CallOptions.LiveAppHeader]);
        Assert.Equal("acme", request.Headers["x-tenant"]);
        Assert.Equal("real-key", request.Bearer);
    }
}
