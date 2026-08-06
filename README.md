# order/kafka-messaging

Unified Kafka messaging for the order platform — the shared library behind the **SNS → Kafka migration**.

It provides the message envelope, producer, consumer runtime, idempotency, and retry/DLQ semantics that every service needs, so that migrating a service is a matter of configuration rather than infrastructure code.

**PHP 8.1+ · one runtime dependency (`psr/log`) · framework-agnostic core with an optional Laravel bridge.**

The core is fully testable without a broker and without `ext-rdkafka`; the extension is only required by the production drivers.

---

## Table of contents

- [Why this exists](#why-this-exists)
- [Installation](#installation)
- [Quick start](#quick-start)
- [The message envelope](#the-message-envelope)
- [Producing](#producing)
- [Consuming](#consuming)
- [Failure policy](#failure-policy)
- [Idempotency](#idempotency)
- [Security (SASL/SSL)](#security-saslssl)
- [Console commands](#console-commands)
- [Testing](#testing)
- [Observability](#observability)
- [Migrating a service](#migrating-a-service)
- [Configuration reference](#configuration-reference)
- [Design decisions](#design-decisions)

---

## Why this exists

The platform's asynchronous events currently travel through AWS SNS and are re-distributed by the API gateway over HTTP. That path has no ordering guarantees, no dead-letter queue, no deduplication key, and no replay: a failed delivery is retried a few times and then dropped.

This library implements the target architecture from the migration plan:

| Guarantee | SNS path | This library |
|---|---|---|
| Ordering | none | per-tenant, via the partition key |
| Retries | 5 attempts, then dropped | in-process attempts → retry topics with escalating delays → DLQ |
| Deduplication | not possible (no message id) | mandatory, via `message_id` |
| Replay | none | `kafka:replay` from offset or timestamp |
| Delivery visibility | silent loss | per-outcome counts, metrics hooks, DLQ depth |

---

## Installation

```bash
composer config repositories.kafka-messaging '{"type":"vcs","url":"git@github.com:tryorder/kafka-messaging.git"}'
composer require order/kafka-messaging:^0.4.0
```

In Laravel the service provider is auto-discovered. Publish the configuration file if you need to customise it:

```bash
php artisan vendor:publish --tag=kafka-messaging-config
```

Minimum environment for a producer:

```env
KAFKA_SOURCE_SERVICE=service-order     # required — identifies the publisher
KAFKA_MESSAGING_DRIVER=log             # log (default) | rdkafka | memory
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_DUAL_WRITE=false                 # flip to true during the dual-write phase
```

`driver=log` is the safe default: it records the exact wire message that *would* be sent, so a service can be wired up and observed before it ever talks to a broker.

---

## Quick start

**Publish an event**

```php
use Order\KafkaMessaging\KafkaProducer;

$producer->safePublish(
    topic: 'order.events',
    eventType: 'OrderCreatedEvent',
    tenant: $tenant,                  // required — this is the partition key
    data: $payload,
    key: "{$tenant}:{$orderId}",      // optional; must be the tenant or "{tenant}:..."
);
```

**Consume events**

```php
use Order\KafkaMessaging\KafkaConsumer;

KafkaConsumer::group('svc-payment')
    ->driver(new RdKafkaConsumerDriver($brokerConfig))
    ->subscribe(['order.events'])
    ->onEvent('OrderCreatedEvent', $handler)
    ->withIdempotency($store)
    ->withRetryAndDlq($producerDriver)
    ->run(exitOnIdle: false);
```

In Laravel, prefer the shipped daemon (`php artisan kafka:consume`) and declare topics and handlers in configuration — see [Consuming](#consuming).

---

## The message envelope

Routing and tracing metadata travel in Kafka **headers**; the **value** keeps the exact shape the SNS-era consumers already understand, so transformation code survives the migration unchanged.

**Key** — `tenant`, or `{tenant}:{orderId}` for topics that require per-order ordering.

**Headers**

| Header | Purpose |
|---|---|
| `event_type` | Replaces the SNS `Subject`; drives handler routing |
| `message_id` | UUIDv7 — the deduplication key |
| `event_time` | When the event occurred (ISO-8601, millisecond precision) |
| `schema_version` | Payload schema version |
| `source_service` | Publishing service |
| `tenant` | Duplicated from the key for cheap filtering |
| `trace_id` | Distributed tracing across services |

**Value**

```json
{ "tenant": "acme", "data": { "order_id": "ord_9931", "total": 74.50 } }
```

`Envelope::create()` rejects an empty tenant or event type outright: an event that cannot be partitioned must never reach a topic.

---

## Producing

```php
// Throws on failure — for callers that must know delivery failed.
$envelope = $producer->publish(topic: 'order.events', eventType: '...', tenant: $t, data: $d);

// Never throws — logs and returns null. Use this on customer-facing request paths.
$envelope = $producer->safePublish(...);
```

**Batch publishing** — one broker round trip for many messages:

```php
$result = $producer->publishBatch('order.events', [
    ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => $a, 'key' => 'acme:ord_1'],
    ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => $b],
]);

$result->count();      // delivered
$result->failed();     // [['index' => 3, 'error' => '...'], ...]
```

Batching is transparent: drivers that implement `BatchProducerDriverInterface` (including the rdkafka driver) send in one flush; others fall back to sequential sends.

**Publishing off the request path.** In Laravel, `PublishToKafkaJob` moves the publish to a queue worker so the broker never adds latency to a customer request:

```php
PublishToKafkaJob::dispatch(topic: 'order.events', eventType: $subject, tenant: $tenant, data: $data);
```

---

## Consuming

Declare the consumer in `config/kafka-messaging.php` and run the daemon:

```php
'consumer' => [
    'group'  => env('KAFKA_CONSUMER_GROUP', 'svc-payment'),
    'topics' => ['order.events'],
    'handlers' => [
        'OrderCreatedEvent' => \App\Kafka\Handlers\OrderCreatedHandler::class,
    ],
],
```

```bash
php artisan kafka:consume                  # main daemon
php artisan kafka:consume --retry          # retry daemon (honours message delays)
php artisan kafka:consume --shadow         # observe only, no side effects
```

Handlers are resolved from the container; both `handle(Envelope $e)` and `__invoke(Envelope $e)` are supported. `SIGTERM`/`SIGINT` finish the in-flight message, commit it, and exit cleanly.

An event type with no handler is acknowledged and skipped — topics carry many event types and each consumer handles a subset.

#### Catch-all consumers

A consumer whose routing is data-driven rather than code-driven can register a **fallback** with the `'*'` key (or `onAnyEvent()` on the fluent builder). An exact-match handler always wins, so the two compose:

```php
'handlers' => [
    'OrderCreatedEvent' => \App\Kafka\Handlers\OrderCreatedHandler::class,  // wins for this type
    '*'                 => \App\Kafka\Handlers\CatchAllHandler::class,      // everything else
],
```

Use this when enumerating event types in configuration would be a correctness bug rather than a chore — a fan-out service that validates events against a catalog table, for example, must not silently drop a newly catalogued event.

**Middleware** wraps handler execution — useful for tenant context, logging, timing, or skipping messages:

```php
$consumer->middleware(function (Envelope $e, Closure $next) {
    Context::setTenant($e->tenant);
    return $next($e);               // omit the call to skip this message
});
```

**Shadow mode** (`--shadow`) runs under a separate `<group>-shadow` consumer group and replaces every handler with an observation log. Offsets and idempotency state for the real group stay untouched, which is what makes the migration's shadow-consumption phase safe to run alongside the live SNS path.

---

## Failure policy

Every outcome is deliberate and observable.

| Situation | Outcome | Offset committed |
|---|---|---|
| Handled successfully | `Processed` | yes |
| Already seen (`message_id`) | `Duplicate` | yes |
| No handler for this `event_type` | `NoHandler` | yes |
| Handler failed, attempts remain | `Retried` — copied to `<group>.<topic>.retry` | yes |
| Handler failed, retries exhausted | `DeadLettered` — copied to `<group>.<topic>.dlq` | yes |
| Unparseable message | `Malformed` — copied to the DLQ | yes |
| Retry/DLQ publish itself failed | `EscalationFailed` | **no** — the loop stops and Kafka redelivers |
| Handler failed with no retry/DLQ configured | `Dropped` — logged at error level | yes |

Two properties are worth calling out:

- **`EscalationFailed` never commits.** If a message could not be parked anywhere, advancing the offset would lose it silently — precisely the failure mode this migration exists to eliminate.
- **A degraded idempotency store never halts consumption.** Read failures fail open with a warning; a write failure after a successful handler is logged but the success stands, so the handler is never re-invoked by this loop.

Retries escalate through configurable rounds (default `5s`, `1m`, `10m`) recorded in `x-retry-round`, `x-retry-delay-seconds`, and an absolute `x-retry-not-before`. The retry daemon parks a message until it becomes eligible.

---

## Idempotency

Kafka is at-least-once, so consumers must be idempotent. The library ships a portable store backed by a `processed_messages` table (MySQL, PostgreSQL, and SQLite):

```php
Schema::create('processed_messages', function (Blueprint $t) {
    $t->string('consumer_group', 191);
    $t->string('message_id', 191);
    $t->string('processed_at', 32);
    $t->primary(['consumer_group', 'message_id']);
});
```

```php
$store = new PdoIdempotencyStore(DB::connection()->getPdo());
```

Concurrent consumers are safe: a duplicate-key violation is treated as "already recorded".

**Known limit.** A process crash between a successful handler and the idempotency write cannot be closed at the library level without a distributed transaction; the message is redelivered and re-processed once. Handlers must tolerate a duplicate. Services needing a hard guarantee should write the deduplication key inside the handler's own database transaction (inbox pattern).

---

## Security (SASL/SSL)

Connection settings are declared once and translated to `librdkafka` keys by `BrokerConfig`, which validates the combination at construction — a misconfigured deployment fails at boot rather than on the first publish.

```env
KAFKA_SECURITY_PROTOCOL=SASL_SSL         # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512       # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
KAFKA_SASL_USERNAME=order
KAFKA_SASL_PASSWORD=***
KAFKA_SSL_CA_LOCATION=/etc/ssl/certs/ca.pem
```

Credentials are redacted from any diagnostic output (`BrokerConfig::toRedactedArray()`). Raw `librdkafka` overrides remain available through `extra_conf`.

---

## Console commands

```bash
php artisan kafka:consume [--retry] [--shadow] [--group=] [--topics=] [--max-messages=] [--idle-exit]
php artisan kafka:replay <topic> [--from=earliest] [--since=] [--event=] [--tenant=] [--to=] [--confirm]
```

`kafka:replay` reads through a throwaway consumer group, so live consumer offsets are never affected. Without `--to` it is a read-only inspection; with `--to` and `--confirm` it re-injects matching messages into the target topic — the standard way to drain a DLQ after a fix. Re-injected messages keep their `message_id` (so consumers that already handled them skip them) and lose their retry bookkeeping headers.

```bash
# Inspect what a topic received in a time window
php artisan kafka:replay order.events --since="2026-08-05 10:00" --event=OrderCreatedEvent

# Drain a DLQ back into the main topic
php artisan kafka:replay svc-payment.order.events.dlq --to=order.events --confirm
```

---

## Testing

`KafkaFake` records publishes in memory and provides assertions, so producer behaviour can be tested without a broker:

```php
$fake = KafkaFake::bind();          // swaps the container binding

// ... exercise the flow ...

$fake->assertPublished('OrderCreatedEvent')
     ->assertPublishedOn('order.events', 'OrderCreatedEvent', fn ($m) => $m['tenant'] === 'acme')
     ->assertPublishedTimes('OrderCreatedEvent', 1)
     ->assertNotPublished('OrderCompletedEvent')
     ->assertAllHaveTenant();
```

Failure messages list what *was* published, which usually identifies the problem without further debugging.

For consumer tests, `InMemoryConsumerDriver` and `InMemoryProducerDriver` let a full consume → handle → retry → DLQ cycle run in a single process.

```bash
composer install && composer test
```

---

## Observability

```php
$consumer->withMetrics($exporter)                              // ConsumerMetricsInterface
         ->before(fn (ConsumedMessage $m) => ...)
         ->after(fn (ConsumedMessage $m, ProcessOutcome $o) => ...);
```

`afterProcess()` receives the outcome and the processing duration, which is enough to export per-outcome counters and latency. Failures inside metrics or callbacks are logged and swallowed — an observability backend must never stop consumption.

`run()` returns per-outcome counts for the batch it processed.

All log context is wrapped in a `data` key, as required by the platform's GELF pipeline.

---

## Migrating a service

1. **Install and configure** — set `KAFKA_SOURCE_SERVICE`, keep `KAFKA_MESSAGING_DRIVER=log`.
2. **Dual-write** — publish to Kafka alongside the existing SNS publish, behind `KAFKA_DUAL_WRITE`. Use `safePublish()` or `PublishToKafkaJob`; a Kafka failure must never affect the existing path.
3. **Shadow-consume** — run `kafka:consume --shadow` and compare against what the SNS path did.
4. **Cut over** — enable the real handlers, stop the SNS subscription for that topic.
5. **Decommission** — remove the SNS publish once every consumer of the topic has cut over.

Producers must emit a `tenant` on every event before dual-write begins; the envelope enforces this, and `SnsBridge` rejects un-normalised SNS payloads for the same reason.

**Bridging both paths.** During dual-write, the same logical event should carry the same `message_id` on both pipes so consumers can deduplicate across them:

```php
$envelope = SnsBridge::toEnvelope($subject, $decodedSnsMessage, 'service-order', $snsMessageId, $snsTimestamp);
```

---

## Configuration reference

| Key | Env | Default | Description |
|---|---|---|---|
| `driver` | `KAFKA_MESSAGING_DRIVER` | `log` | `log`, `rdkafka`, `memory` |
| `source_service` | `KAFKA_SOURCE_SERVICE` | — | **Required.** Publisher identity |
| `dual_write` | `KAFKA_DUAL_WRITE` | `false` | Dual-write phase switch |
| `brokers` | `KAFKA_BROKERS` | `localhost:9092` | Bootstrap servers |
| `security.protocol` | `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | See [Security](#security-saslssl) |
| `security.sasl_*` | `KAFKA_SASL_*` | — | SASL mechanism and credentials |
| `security.ssl_*` | `KAFKA_SSL_*` | — | Certificate paths |
| `extra_conf` | — | `[]` | Raw `librdkafka` overrides |
| `consumer.group` | `KAFKA_CONSUMER_GROUP` | — | e.g. `svc-payment` |
| `consumer.topics` | — | `[]` | Subscribed topics |
| `consumer.handlers` | — | `[]` | `event_type => handler class`; the `'*'` key registers a fallback for every unclaimed type |
| `idempotency.store` | `KAFKA_IDEMPOTENCY_STORE` | `pdo` | `pdo`, `memory` |
| `idempotency.table` | `KAFKA_IDEMPOTENCY_TABLE` | `processed_messages` | Table name |

---

## Design decisions

**A framework-agnostic core.** Only `src/Laravel/` depends on Illuminate. The fleet runs a mix of Laravel 8 and 9, which rules out the available Laravel-only Kafka packages, and the core stays usable from any PHP context.

**The library owns commit control.** Auto-commit is disabled deliberately: the commit decision belongs to the processing outcome. This is what makes `EscalationFailed` able to prevent silent loss, and it is why the auto-commit and custom-committer features of other packages were not adopted.

**Synchronous, acknowledged publishes.** Single publishes flush and verify delivery reports rather than firing and forgetting. Correctness is the priority; `publishBatch()` exists for volume.

**JSON payloads.** Pluggable serializers were skipped on purpose — the migration plan specifies JSON Schema for phase one, with Avro/Protobuf as a later decision.

**Not adopted:** auto-commit, custom committers, custom serializers, regex topic subscription, manual partition assignment. Each conflicts with a guarantee above or is already handled by consumer groups.
