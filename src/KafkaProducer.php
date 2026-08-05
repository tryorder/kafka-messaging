<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Contracts\BatchProducerDriverInterface;
use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use Order\KafkaMessaging\Exceptions\PublishFailedException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * The producer side — replaces SendDataToServiceWithSNSListener
 * (03-message-envelope.md §5).
 *
 * publish()     throws — for callers that must know delivery failed.
 * safePublish() never throws — for the dual-write phase and for request
 *               paths where a messaging failure must not fail the customer
 *               order (standing platform rule).
 */
final class KafkaProducer
{
    public function __construct(
        private readonly ProducerDriverInterface $driver,
        private readonly string $sourceService,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }

    /**
     * @param array<string, mixed> $data
     */
    public function publish(
        string $topic,
        string $eventType,
        string $tenant,
        array $data,
        ?string $key = null,
        ?string $traceId = null,
        int $schemaVersion = 1,
        ?string $messageId = null,
        ?string $eventTime = null,
    ): Envelope {
        // messageId/eventTime pass-through exists for the SNS bridge and
        // replay tooling: re-publishing the same logical event MUST reuse its
        // message_id so consumer idempotency can dedupe across paths.
        $envelope = Envelope::create(
            tenant: $tenant,
            data: $data,
            eventType: $eventType,
            sourceService: $this->sourceService,
            messageId: $messageId,
            eventTime: $eventTime,
            schemaVersion: $schemaVersion,
            traceId: $traceId,
        );

        // Key defaults to the tenant; critical topics pass "{tenant}:{orderId}"
        // (02-kafka-topics.md §3). A caller-supplied key that drops the tenant
        // prefix would silently break per-tenant ordering, so it is rejected.
        $key ??= $envelope->tenant;
        $this->assertKeyMatchesTenant($key, $envelope->tenant);

        try {
            $this->driver->send($topic, $key, $envelope->toHeaders(), $envelope->toPayload());
        } catch (\Throwable $e) {
            throw $e instanceof PublishFailedException
                ? $e
                : new PublishFailedException("Publishing to '{$topic}' failed: {$e->getMessage()}", previous: $e);
        }

        return $envelope;
    }

    /**
     * Publish many messages to one topic with a single broker round trip
     * (when the driver supports batching; otherwise sequentially).
     *
     * One bad message does not discard the batch: envelope-building and
     * delivery failures are reported per index instead of thrown.
     *
     * @param list<array{eventType: string, tenant: string, data: array<string, mixed>, key?: string|null, traceId?: string|null, schemaVersion?: int, messageId?: string|null, eventTime?: string|null}> $messages
     */
    public function publishBatch(string $topic, array $messages): BatchResult
    {
        $prepared = [];   // wire messages for the driver
        $envelopes = [];  // parallel to $prepared
        $indexes = [];    // driver index → original input index
        $failures = [];

        foreach (array_values($messages) as $i => $message) {
            try {
                $envelope = Envelope::create(
                    tenant: (string) ($message['tenant'] ?? ''),
                    data: (array) ($message['data'] ?? []),
                    eventType: (string) ($message['eventType'] ?? ''),
                    sourceService: $this->sourceService,
                    messageId: $message['messageId'] ?? null,
                    eventTime: $message['eventTime'] ?? null,
                    schemaVersion: (int) ($message['schemaVersion'] ?? 1),
                    traceId: $message['traceId'] ?? null,
                );

                $key = $message['key'] ?? $envelope->tenant;
                $this->assertKeyMatchesTenant($key, $envelope->tenant);

                $indexes[] = $i;
                $envelopes[] = $envelope;
                $prepared[] = ['key' => $key, 'headers' => $envelope->toHeaders(), 'payload' => $envelope->toPayload()];
            } catch (\Throwable $e) {
                $failures[] = ['index' => $i, 'error' => $e->getMessage()];
            }
        }

        if ($prepared === []) {
            return new BatchResult([], $failures);
        }

        $driverFailures = $this->driver instanceof BatchProducerDriverInterface
            ? $this->driver->sendBatch($topic, $prepared)
            : $this->sendSequentially($topic, $prepared);

        // Map driver-relative indexes back to the caller's input indexes.
        $failedDriverIndexes = [];
        foreach ($driverFailures as $failure) {
            $failedDriverIndexes[$failure['index']] = true;
            $failures[] = ['index' => $indexes[$failure['index']] ?? $failure['index'], 'error' => $failure['error']];
        }

        $delivered = [];
        foreach ($envelopes as $driverIndex => $envelope) {
            if (!isset($failedDriverIndexes[$driverIndex])) {
                $delivered[] = $envelope;
            }
        }

        if ($failures !== []) {
            $this->logger->error('Kafka batch publish had failures', ['data' => [
                'topic' => $topic,
                'attempted' => count($messages),
                'delivered' => count($delivered),
                'failed' => count($failures),
                'errors' => array_slice($failures, 0, 10),
            ]]);
        }

        usort($failures, static fn (array $a, array $b): int => $a['index'] <=> $b['index']);

        return new BatchResult($delivered, $failures);
    }

    /**
     * @param list<array{key: string, headers: array<string, string>, payload: string}> $messages
     *
     * @return list<array{index: int, error: string}>
     */
    private function sendSequentially(string $topic, array $messages): array
    {
        $failures = [];
        foreach ($messages as $i => $message) {
            try {
                $this->driver->send($topic, $message['key'], $message['headers'], $message['payload']);
            } catch (\Throwable $e) {
                $failures[] = ['index' => $i, 'error' => $e->getMessage()];
            }
        }

        return $failures;
    }

    private function assertKeyMatchesTenant(string $key, string $tenant): void
    {
        if ($key !== $tenant && !str_starts_with($key, $tenant . ':')) {
            throw new PublishFailedException(
                "Partition key '{$key}' must be the tenant or start with '{$tenant}:'."
            );
        }
    }

    /**
     * Never throws. Returns the envelope on success, null on failure —
     * the failure is logged (context wrapped in 'data' per the platform's
     * GELF convention) and the caller's flow continues.
     *
     * @param array<string, mixed> $data
     */
    public function safePublish(
        string $topic,
        string $eventType,
        string $tenant,
        array $data,
        ?string $key = null,
        ?string $traceId = null,
        int $schemaVersion = 1,
        ?string $messageId = null,
        ?string $eventTime = null,
    ): ?Envelope {
        try {
            return $this->publish($topic, $eventType, $tenant, $data, $key, $traceId, $schemaVersion, $messageId, $eventTime);
        } catch (\Throwable $e) {
            $this->logger->error('Kafka publish failed (non-blocking)', ['data' => [
                'topic' => $topic,
                'event_type' => $eventType,
                'tenant' => $tenant,
                'error' => $e->getMessage(),
                'exception' => $e::class,
            ]]);

            return null;
        }
    }
}
