<?php

declare(strict_types=1);

/**
 * E2E smoke against the LIVE local cluster (kafka-test-main/infra):
 *
 *   php bin/e2e-smoke.php [brokers=localhost:9092]
 *
 * Full pipeline: RdKafkaProducerDriver → order.events (tenant:order_id key)
 * → RdKafkaConsumerDriver → KafkaConsumer (idempotency sqlite + retry/DLQ)
 * — including a deliberately failing event that must land in
 * svc-e2e.order.events.retry with round headers.
 */

require __DIR__ . '/../vendor/autoload.php';

use Order\KafkaMessaging\Drivers\RdKafkaConsumerDriver;
use Order\KafkaMessaging\Drivers\RdKafkaProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\KafkaConsumer;
use Order\KafkaMessaging\KafkaProducer;
use Order\KafkaMessaging\PdoIdempotencyStore;

// 127.0.0.1 explicitly: 'localhost' resolves ::1 first and the container
// only publishes on IPv4 — librdkafka recovers but spams FAIL logs.
$brokers = $argv[1] ?? '127.0.0.1:9092';
$runId = bin2hex(random_bytes(4));
$group = 'svc-e2e';
// Dedicated topic (+ pre-created svc-e2e.e2e.smoke.{retry,dlq}) so business
// topics stay clean and old runs only ever produce NoHandler skips.
$topic = 'e2e.smoke';

echo "== order/kafka-messaging E2E smoke ==\nbrokers: {$brokers} · run: {$runId}\n\n";

if (!extension_loaded('rdkafka')) {
    fwrite(STDERR, "ext-rdkafka missing. Run:\n  brew install librdkafka\n  pecl install rdkafka\n");
    exit(1);
}

// ---- produce ----------------------------------------------------------
$producerDriver = new RdKafkaProducerDriver($brokers);
$producer = new KafkaProducer($producerDriver, 'service-e2e');

$ok = $producer->publish($topic, "E2eOkEvent{$runId}", 'jabourih', ['n' => 1], key: "jabourih:e2e-{$runId}");
$dup = $producer->publish($topic, "E2eOkEvent{$runId}", 'jabourih', ['n' => 1], key: "jabourih:e2e-{$runId}", messageId: $ok->messageId);
$bad = $producer->publish($topic, "E2eFailEvent{$runId}", 'jabourih', ['n' => 2], key: "jabourih:e2e-{$runId}");

echo "produced 3 messages (1 ok + 1 duplicate id + 1 poison) → {$topic}\n";
echo "message_id (ok+dup): {$ok->messageId}\n\n";

// ---- consume ----------------------------------------------------------
$pdo = new PDO('sqlite::memory:');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
PdoIdempotencyStore::createTable($pdo);

$handled = 0;
$counts = KafkaConsumer::group($group)
    ->driver(new RdKafkaConsumerDriver($brokers, offsetReset: 'earliest'))
    ->subscribe([$topic])
    ->onEvent("E2eOkEvent{$runId}", function (Envelope $e) use (&$handled): void {
        $handled++;
    })
    ->onEvent("E2eFailEvent{$runId}", fn () => throw new RuntimeException('deliberate e2e failure'))
    ->withIdempotency(new PdoIdempotencyStore($pdo))
    ->withRetryAndDlq(new RdKafkaProducerDriver($brokers))
    ->run(maxMessages: 200, pollTimeoutMs: 3000);

echo "consumer outcomes: " . json_encode($counts, JSON_UNESCAPED_UNICODE) . "\n";
echo "ok-handler invocations: {$handled}\n\n";

// ---- verdict ----------------------------------------------------------
$pass = $handled === 1
    && ($counts['processed'] ?? 0) >= 1
    && ($counts['duplicate'] ?? 0) >= 1
    && ($counts['retried'] ?? 0) >= 1;

echo $pass
    ? "✅ PASS — publish/consume/idempotency/retry all live against the real cluster.\n"
    : "❌ CHECK — expected: handler×1, ≥1 processed, ≥1 duplicate, ≥1 retried. Inspect svc-e2e.e2e.smoke.retry in Kafka UI.\n";

exit($pass ? 0 : 2);
