# Lesson 05: connection reuse

This branch inherits the unchanged Lesson 01 API. A separate, dependency-free probe isolates client-to-server networking from SQL and pool waits. It is not a second production service.

## Run

From the repository root, in bash or PowerShell:

```text
dotnet run -c Release --project src/Tadka.NetworkingLab
```

The command starts a temporary loopback HTTPS server, generates a one-day in-memory certificate, runs three cases and stops the server. No database or Docker is needed. .NET 10 SDK is required.

| Case | Requests | Expected assertion, not a latency prediction |
|---|---:|---|
| h1-close | 5 sequential GETs, Connection: close | 5 different server connection IDs |
| h1-reuse | 5 sequential GETs, one shared handler | 1 server connection ID |
| h2-reuse | 5 sequential GETs, exact HTTP/2 | 1 server connection ID and HTTP/2 negotiated |

An assertion failure exits nonzero. Read the actual CSV values. `total_ms` starts immediately before `SendAsync` and ends after the response body has been deserialized. It includes client work, transport, server work and response reading. It does **not** isolate DNS, TCP or TLS. No speed ratio is asserted. Fresh TCP does not prove a full non-resumed TLS handshake. The first request also includes JIT and cold-path effects.

For curl timing inspection:

```text
dotnet run -c Release --project src/Tadka.NetworkingLab -- --serve
```

In another terminal:

```text
curl.exe --http1.1 --insecure --silent --output NUL --write-out "dns=%{time_namelookup} connect=%{time_connect} tls=%{time_appconnect} first_byte=%{time_starttransfer} total=%{time_total}\n" https://localhost:5443/probe
```

On bash use `curl`, `/dev/null` instead of `NUL`. The timing fields are cumulative milestones from transfer start, not independent phase durations. Subtract connect from appconnect for an approximate TLS interval on a fresh direct connection. `first_byte - tls` includes request transmission, server time and return transit, not only backend execution.

`--insecure` is only for this disposable loopback certificate. Never copy it into a production client. The automatic runner instead pins this run's exact certificate. No trust-store change is required. Stop serve mode with Ctrl+C.

## Limits

This demonstrates reuse and protocol negotiation, not a WAN benchmark or HTTP/2 multiplexing under concurrent traffic. DNS can be satisfied by the local resolver/hosts file. HTTP/3, packet loss, QUIC migration and real mobile RTT are board explanations only. No HTTP/3 measurement is claimed. Five requests are observations, not a percentile sample.

## References

- https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
- https://curl.se/docs/manpage.html
- https://www.rfc-editor.org/rfc/rfc9112
- https://www.rfc-editor.org/rfc/rfc9113
- https://www.rfc-editor.org/rfc/rfc9114
- https://www.rfc-editor.org/rfc/rfc9000
