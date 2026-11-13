using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

using var rsa = RSA.Create(2048);
var certificateRequest = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
var names = new SubjectAlternativeNameBuilder();
names.AddDnsName("localhost");
names.AddIpAddress(IPAddress.Loopback);
certificateRequest.CertificateExtensions.Add(names.Build());
certificateRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
    new OidCollection { new("1.3.6.1.5.5.7.3.1") }, false));
using var generated = certificateRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
// Import the private key for Windows Schannel, which cannot serve an ephemeral CNG key.
using var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pfx), null);
var serve = args.Contains("--serve");
var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, serve ? 5443 : 0, listen =>
{
    listen.Protocols = HttpProtocols.Http1AndHttp2;
    listen.UseHttps(certificate);
}));
builder.Services.AddControllers();
await using var app = builder.Build();
app.MapControllers();
await app.StartAsync();
var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
Console.WriteLine($"Loopback-only probe: {address}/probe");
if (serve)
{
    await app.WaitForShutdownAsync();
    return;
}

try
{
    Console.WriteLine("case,request,negotiated_protocol,connection_id,total_ms");
    await CheckAsync("h1-close", HttpVersion.Version11, true);
    await CheckAsync("h1-reuse", HttpVersion.Version11, false);
    await CheckAsync("h2-reuse", HttpVersion.Version20, false);
    Console.WriteLine("Assertions passed: exact HTTP versions and distinct/reused server connection IDs.");
}
finally
{
    await app.StopAsync();
}

async Task CheckAsync(string label, Version version, bool close)
{
    using var handler = new SocketsHttpHandler
    {
        UseProxy = false,
        PooledConnectionLifetime = TimeSpan.FromMinutes(2),
        SslOptions = new()
        {
            // Trust only this run's in-memory certificate, without changing the OS trust store.
            RemoteCertificateValidationCallback = (_, peer, _, _) =>
                peer is not null && peer.GetCertHashString() == certificate.GetCertHashString()
        }
    };
    using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) };
    var connections = new HashSet<string>();
    for (var i = 1; i <= 5; i++)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{address}/probe")
        {
            Version = version,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact
        };
        request.Headers.ConnectionClose = close;
        var started = Stopwatch.GetTimestamp();
        using var response = await client.SendAsync(request);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadFromJsonAsync<ProbeResponse>()
            ?? throw new InvalidOperationException("Probe returned an empty body.");
        var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        var expectedProtocol = version == HttpVersion.Version20 ? "HTTP/2" : "HTTP/1.1";
        if (response.Version != version || body.Protocol != expectedProtocol)
            throw new InvalidOperationException($"Unexpected protocol: {response.Version}, {body.Protocol}");
        connections.Add(body.ConnectionId);
        Console.WriteLine(FormattableString.Invariant($"{label},{i},{body.Protocol},{body.ConnectionId},{elapsed:F3}"));
    }
    var expected = close ? 5 : 1;
    if (connections.Count != expected)
        throw new InvalidOperationException($"{label}: expected {expected} connections, got {connections.Count}.");
}

/// <summary>Reports the protocol and connection observed by the server, not a handshake timer.</summary>
public sealed record ProbeResponse(string Protocol, string ConnectionId);

[ApiController]
public sealed class ProbeController : ControllerBase
{
    [HttpGet("/probe")]
    [ProducesResponseType<ProbeResponse>(StatusCodes.Status200OK)]
    public ActionResult<ProbeResponse> Get(CancellationToken cancellationToken) =>
        Ok(new ProbeResponse(Request.Protocol, HttpContext.Connection.Id));
}
