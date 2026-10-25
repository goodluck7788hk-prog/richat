# gRPC subscription backlog metrics

These additive daemon `/metrics` gauges cover Dragon's Mouth `subscribe` and
`subscribe_accounts`. They do not instrument native Richat gRPC/QUIC or PubSub.
The plugin producer's channel and the daemon's upstream source are separate
queues; a downstream observation does not establish their health.

## Identity and units

Every subscription has `source_ip`, `subscription_id` and `x_subscription_id`.
The IP is the actual TCP peer, with IPv4-mapped IPv6 normalized. No forwarded IP
header is trusted. Behind a proxy/NAT it identifies that proxy/NAT, not the
original client. Missing transport metadata yields `source_ip="unknown"`.

`subscription_id` is unique within the daemon gRPC server's lifetime, including
multiple streams on the same connection. It is not a TCP connection ID and resets
on daemon restart. The optional client-supplied `x-subscription-id` is escaped and
truncated to 128 characters for these new metrics; blank/duplicate names remain
distinct because of `subscription_id`. Never put authentication secrets in it.

| Gauge | Meaning |
| --- | --- |
| `grpc_subscribe_pending_bytes` | Encoded protobuf payload bytes waiting in this subscription's Richat application queue, including ping/pong. |
| `grpc_subscribe_lag_messages` | `next_index - reader_head` in the reader's own commitment channel: unfiltered input records not yet scanned, not matched outputs. `-1` means unavailable. |
| `grpc_subscribe_backlog_available` | `1` if the cursor lag is known for this scrape, otherwise `0`. Queue bytes remain available independently. |
| `grpc_subscribe_replay_active` | `1` for disk replay, `0` for memory, `-1` when the cursor is unset/busy. |
| `grpc_subscribe_backlog_state` | One sample with value `1`, labelled `state="memory\|disk\|unset\|busy\|cursor_ahead"`. |

Cursor metrics also carry `commitment="processed|confirmed|finalized|unknown"`.
`busy` uses unknown commitment instead of inventing an observation. A memory
cursor can have numeric lag even after retained history has been overwritten;
the number is not proof that replay will succeed.

The disk replay index is not a commitment-channel index and currently has
different bootstrap/progress semantics. Consequently disk lag is **explicitly
unavailable**, not guessed or silently reported as zero. Queue bytes remain
observable and `replay_active=1` distinguishes this intentional mode. Numeric
memory lag resumes automatically on the existing disk-to-memory transition.

None of these values includes tonic/HTTP2 buffers, TCP Send-Q, receiving-process
queues or completed business processing. Queue bytes and cursor lag are separate:
filtering may leave no queued output while the reader is behind; a caught-up
reader may still have output awaiting transport. Scrapes are concurrent samples,
not an atomic transaction across every queue and cursor.

## Example queries

Pending application-queue bytes per source IP:

```promql
sum by (source_ip) (grpc_subscribe_pending_bytes)
```

Largest known reader lag per IP and commitment:

```promql
max by (source_ip, commitment) (grpc_subscribe_lag_messages >= 0)
```

Unavailable reader observations per IP (always check this alongside known lag):

```promql
sum by (source_ip) (1 - grpc_subscribe_backlog_available)
```

Why an observation is unavailable:

```promql
sum by (source_ip, state) (grpc_subscribe_backlog_state{state!="memory"})
```

Compare sustained queue/lag trends rather than alerting on one nonzero sample.
There is no generic safe threshold because filters and traffic volumes differ.
Instrument the receiving application's own queue and processing latency to
determine whether business processing, rather than server delivery, is behind.

## Lifecycle and performance boundary

- Register on subscribe and remove on response drop/terminal response error.
  Registry references to queue/state are weak; they do not keep clients alive.
- Render only active entries. Closed streams disappear from the next scrape,
  without leaving permanent per-subscription handles in the metrics recorder.
  Prometheus still retains historical series: reconnect churn and retention must
  be budgeted. Use recording rules to retain IP aggregates where appropriate.
- Snapshot each stream once per scrape, then emit each metric family's HELP/TYPE
  and samples contiguously, following the
  [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#grouping-and-sorting).
- Format static identity labels once at registration. During scraping, clone the
  active entry list under the registry mutex, then release it before sampling.
- `try_lock` copies client cursor scalars only; it never waits for a filter/replay
  worker. Release that lock before reading channel tail or formatting samples.
- Normal producer, filter/reader and response-delivery loops gain no new metric
  lookups, allocations, locks, label work or atomic writes. Queue accounting
  reuses the existing atomic. Terminal error draining subtracts only successfully
  popped payloads, preserving accounting for an in-flight concurrent dequeue.
- Scrape/registration work still has a cost proportional to active subscriptions,
  and briefly acquiring an existing state lock can cause contention. Local debug
  regressions and source-structure guards are not Linux release/P99 performance
  evidence; do not claim zero production performance impact.

This source change does not build a release, alter configuration or change the
running fleet. Deployment and production performance acceptance remain gated.
