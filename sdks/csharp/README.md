# The Loams SDK for .NET (`Loams`)

> **Preview.** `0.1.0-preview.*` packages track the Loams preview server and may
> change between previews. NuGet hides prereleases unless asked: pin the exact
> version, or pass `--prerelease`.

One `LoamsClient` with namespaced modules over the unified Connect API,
generated from the Loams protos. `net8.0`; the only dependency is
`Google.Protobuf`.

```console
dotnet add package Loams --version 0.1.0-preview.2
```

```csharp
using Loams;
using Loams.Instance.V1;

await using var client = new LoamsClient(new LoamsClientOptions
{
    Endpoint = Environment.GetEnvironmentVariable("LOAMS_ENDPOINT")!, // http://127.0.0.1:8080 for `loams dev`
    Auth = new EnvTokenSource(),                                      // reads LOAMS_API_KEY
});

// The first call any client makes: what this instance is, and no auth.
var info = await client.Instance.GetInstanceAsync(new GetInstanceRequest());
Console.WriteLine($"{info.Name} serves {string.Join(", ", info.ApiVersions)}");
```

The wire is Connect over `System.Net.Http` (D650); why there is no
`Grpc.Net.Client` dependency is in [DEPENDENCIES.md](https://github.com/ostrium-labs/loams/blob/dev/sdks/csharp/DEPENDENCIES.md).

Apache-2.0. Source and issues: <https://github.com/ostrium-labs/loams/tree/dev/sdks/csharp>.
