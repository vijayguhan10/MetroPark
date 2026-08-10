# MetroPark observability stack

Prometheus for metrics, Jaeger for traces, Loki for logs, an OpenTelemetry
Collector in front of the trace pipeline, and Grafana over all three.

## Signal flow

```
                        ┌──────────────────────────────┐
                        │   MetroPark (host, :8080)    │
                        └──┬───────────┬────────────┬──┘
      /actuator/prometheus │           │ OTLP HTTP  │ Loki4j (logback)
             (scraped)     │           │ :4318      │ :3100
                           │           ▼            │
                           │   ┌───────────────┐    │
                           │   │ otel-collector│    │
                           │   └───┬───────┬───┘    │
                           │  OTLP │       │ :8889  │
                           │ :4317 ▼       │(scraped)
                           │  ┌─────────┐  │        │
                           │  │ Jaeger  │  │        ▼
                           │  └────┬────┘  │   ┌────────┐
                           ▼       │       ▼   │  Loki  │
                      ┌─────────────────────┐  └───┬────┘
                      │     Prometheus      │      │
                      └──────────┬──────────┘      │
                                 └────────┬────────┘
                                          ▼
                                     ┌─────────┐
                                     │ Grafana │
                                     └─────────┘
```

The collector is the single OTLP entrypoint. Besides forwarding spans to Jaeger,
its `spanmetrics` connector derives RED metrics from the trace stream and
publishes them on `:8889` for Prometheus — that is what powers the "Traces" row
of the dashboard without extra application instrumentation.

## Ports

| Service        | Port          | Purpose                          |
| -------------- | ------------- | -------------------------------- |
| Grafana        | 3000          | Dashboards (anonymous admin)     |
| Prometheus     | 9090          | Metrics store / query UI         |
| Jaeger         | 16686         | Trace search UI                  |
| Loki           | 3100          | Log ingest + query               |
| otel-collector | 4317 / 4318   | OTLP gRPC / HTTP from the app    |
| otel-collector | 8889 / 8888   | Pipeline metrics / self-telemetry|

## Running

```powershell
cd infra\observability
docker compose up -d
```

Or bring up everything (Kafka, RabbitMQ, Redis, Postgres, observability) with
`infra\compose-all.ps1 up`.

Then start the application; it needs no observability-specific flags, since
`application.properties` already points OTLP at `localhost:4318`.

Open Grafana at <http://localhost:3000> → **MetroPark → MetroPark Overview**.

## Correlation

All three signals are linked, so an investigation can move between them:

- **Logs → trace.** The logback pattern emits `traceID=<id>` on every line. The
  Loki datasource extracts it with a derived field and renders a "View trace"
  link into Jaeger.
- **Trace → logs.** A span in Jaeger links back to the Loki lines carrying its
  trace id.
- **Trace → metrics.** A span links to its `spanmetrics` rate and p95 series.

For this to work, the Loki `app` label, the OTLP `service.name` and the
dashboard queries must all agree on `metropark`. That value comes from
`spring.application.name`, so changing it means updating the dashboard too.

## Notes

- Tempo was removed. It duplicated Jaeger's role and both wanted host ports
  4317/4318, so only one of them could ever bind.
- Jaeger runs as `root` because its image's UID 10001 cannot create the badger
  storage directories inside a root-owned named volume. Fine for local dev.
- Jaeger's badger storage keeps traces across restarts; `docker compose down -v`
  discards them.
- The `activeSessionsGauge`, `availableSlotsGauge` and `activeReservationsGauge`
  beans in `MetricsConfig` are still placeholders returning `0`. The "Capacity"
  panel and the "Active parking sessions" tile will read zero until those are
  wired to the repositories.
- Tracing needs `spring-boot-starter-opentelemetry`. Spring Boot 4 split
  `spring-boot-actuator-autoconfigure` into per-technology modules, so
  `spring-boot-starter-actuator` alone gives metrics but no tracing
  autoconfiguration — the app then starts cleanly and exports no spans at all.
- `micrometer-registry-otlp` (pulled in by that starter) is disabled via
  `management.otlp.metrics.export.enabled=false`, so application metrics reach
  Prometheus only through the actuator scrape rather than twice.
- Meters must not be named with a trailing `.created`: the Prometheus client
  treats `_created` as a reserved suffix and strips it, turning
  `metropark.parking.session.created` into `metropark_parking_session_total`.
