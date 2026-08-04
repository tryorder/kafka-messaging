<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

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
        if ($key !== $envelope->tenant && !str_starts_with($key, $envelope->tenant . ':')) {
            throw new PublishFailedException(
                "Partition key '{$key}' must be the tenant or start with '{$envelope->tenant}:'."
            );
        }

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
