// `loams.live.admin`'s app-directory calls (LV1 Task 13): the five calls reach
// `LiveAdminService`, `CallOptions.WithLiveApp` puts the app on the wire as the
// `loams-live-app` header, and the `live_app_*` reasons are in the registry.

using System.Net;
using Loams.Live.V1;
using Loams.Tests.Support;
using Xunit;

namespace Loams.Tests;

public sealed class LiveAdminTests
{
    private const string Admin = "loams.live.v1.LiveAdminService";

    private static LoamsClient ClientOver(StubHandler handler) => new(new LoamsClientOptions
    {
        Endpoint = "https://fixture.invalid",
        Codec = Codec.Json,
        Handler = handler,
        MaxRetries = 3,
    });

    [Fact]
    public void The_catalogue_names_the_five_calls_with_the_right_retry_classes()
    {
        var module = Assert.Single(Facade.Modules, (binding) => binding.Name == "live.admin");
        Assert.Equal(Admin, module.Service);
        Assert.True(module.Unstable);

        var safe = new[] { "GetApp", "ListApps" };
        foreach (var name in new[] { "CreateApp", "ListApps", "GetApp", "UpdateApp", "DeleteApp" })
        {
            var binding = Facade.Binding("live.admin", name);
            Assert.Equal($"{Admin}/{name}", binding.Rpc);
            Assert.Equal(safe.Contains(name) ? RetryClass.Safe : RetryClass.Manual, binding.Retry);
            Assert.False(binding.TakesIdempotencyKey, $"{name} must not be keyed");
        }
        Assert.Equal("Apps", Facade.Binding("live.admin", "ListApps").Pagination!.ItemsField);
        Assert.Null(Facade.Binding("live.admin", "GetApp").Pagination);
        Assert.Equal("loams-live-app", CallOptions.LiveAppHeader);
    }

    [Fact]
    public async Task Each_call_reaches_the_admin_service_and_WithLiveApp_sets_the_header()
    {
        var handler = new StubHandler(StubHandler.ConnectJson(HttpStatusCode.OK, "{}"));
        using var client = ClientOver(handler);
        var blog = CallOptions.None.WithLiveApp("blog");

        await client.LiveAdmin.CreateAppAsync(new CreateAppRequest());
        await client.LiveAdmin.ListAppsAsync(new ListAppsRequest());
        await client.LiveAdmin.GetAppAsync(new GetAppRequest(), blog);
        await client.LiveAdmin.UpdateAppAsync(new UpdateAppRequest(), blog);
        await client.LiveAdmin.DeleteAppAsync(new DeleteAppRequest(), blog);

        Assert.Equal(
            new[] { "CreateApp", "ListApps", "GetApp", "UpdateApp", "DeleteApp" }
                .Select((name) => $"/{Admin}/{name}"),
            handler.Requests.Select((request) => request.Path));
        Assert.All(handler.Requests.Take(2), (request) =>
            Assert.False(request.Headers.ContainsKey("loams-live-app"), "no option, no header"));
        Assert.All(handler.Requests.Skip(2), (request) =>
            Assert.Equal("blog", request.Headers["loams-live-app"]));
        Assert.True(client.Modules.ContainsKey("live.admin"));
    }

    [Fact]
    public async Task A_create_is_not_retried()
    {
        var handler = new StubHandler(StubHandler.ConnectRefusal(HttpStatusCode.ServiceUnavailable,
            "unavailable", "the node is restarting", "unavailable"));
        using var client = ClientOver(handler);

        await Assert.ThrowsAnyAsync<LoamsError>(() =>
            client.LiveAdmin.CreateAppAsync(new CreateAppRequest()));
        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public async Task ListApps_follows_the_next_page_token()
    {
        var handler = new StubHandler(
            StubHandler.ConnectJson(HttpStatusCode.OK,
                "{\"apps\":[{\"name\":\"a\"},{\"name\":\"b\"}],\"nextPageToken\":\"p2\"}"),
            StubHandler.ConnectJson(HttpStatusCode.OK, "{\"apps\":[{\"name\":\"c\"}]}"));
        using var client = ClientOver(handler);
        var blog = CallOptions.None.WithLiveApp("blog");

        var names = new List<string>();
        await foreach (var app in Paginator.ItemsAsync(
                           Facade.Binding("live.admin", "ListApps"),
                           (ListAppsRequest request, string token, CancellationToken cancellationToken) =>
                           {
                               request.PageToken = token;
                               return client.LiveAdmin.ListAppsAsync(request, blog, cancellationToken);
                           },
                           new ListAppsRequest { PageSize = 2 },
                           (ListAppsResponse page) => page.Apps))
        {
            names.Add(app.Name);
        }

        Assert.Equal(new[] { "a", "b", "c" }, names);
        Assert.Equal(2, handler.CallCount);
        Assert.All(handler.Requests, (request) => Assert.Equal("blog", request.Headers["loams-live-app"]));
    }

    [Fact]
    public void The_live_app_reasons_are_in_the_registry()
    {
        Assert.Contains(Reason.LiveAppDisabled, ReasonRegistry.All);
        Assert.Contains(Reason.LiveAppExists, ReasonRegistry.All);
        Assert.Contains(Reason.LiveAppMismatch, ReasonRegistry.All);
    }

    [Fact]
    public async Task Authenticate_sends_the_session_and_the_bearer_and_surfaces_both_outcomes()
    {
        var binding = Facade.Binding("live", "Authenticate");
        Assert.Equal("loams.live.v1.LiveService/Authenticate", binding.Rpc);
        Assert.Equal(RetryClass.Manual, binding.Retry);
        Assert.Equal(Streaming.Unary, binding.Streaming);
        Assert.False(binding.TakesIdempotencyKey);

        // 4 is the version the session holds: the same caller answers 4 again (the
        // expiry moved), another caller answers 5 (the identity changed).
        var handler = new StubHandler(
            StubHandler.ConnectJson(HttpStatusCode.OK, "{\"identity\":\"4\"}"),
            StubHandler.ConnectJson(HttpStatusCode.OK, "{\"identity\":\"5\"}"));
        using var client = new LoamsClient(new LoamsClientOptions
        {
            Endpoint = "https://fixture.invalid",
            Codec = Codec.Json,
            Handler = handler,
            MaxRetries = 3,
            Auth = new StaticTokenSource("tok-bob"),
        });

        var extended = await client.Live.AuthenticateAsync(new AuthenticateRequest { SessionId = "s1" });
        var changed = await client.Live.AuthenticateAsync(new AuthenticateRequest { SessionId = "s1" });

        Assert.Equal(4UL, extended.Identity);
        Assert.Equal(5UL, changed.Identity);
        Assert.All(handler.Requests, (request) =>
        {
            Assert.Equal("/loams.live.v1.LiveService/Authenticate", request.Path);
            Assert.Equal("tok-bob", request.Bearer);
            Assert.Contains("s1", System.Text.Encoding.UTF8.GetString(request.Body));
        });
    }

    [Fact]
    public async Task Authenticate_is_not_retried()
    {
        var handler = new StubHandler(StubHandler.ConnectRefusal(HttpStatusCode.ServiceUnavailable,
            "unavailable", "the node is restarting", "unavailable"));
        using var client = ClientOver(handler);

        await Assert.ThrowsAnyAsync<LoamsError>(() =>
            client.Live.AuthenticateAsync(new AuthenticateRequest { SessionId = "s1" }));
        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public void The_session_reasons_are_in_the_registry()
    {
        Assert.Contains(Reason.SessionRotate, ReasonRegistry.All);
        Assert.Contains(Reason.TokenExpired, ReasonRegistry.All);
        Assert.Contains(Reason.TokenRevoked, ReasonRegistry.All);
    }
}
