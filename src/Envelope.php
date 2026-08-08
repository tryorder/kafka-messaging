<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Exceptions\InvalidEnvelopeException;

/**
 * The unified message envelope (03-message-envelope.md).
 *
 * Payload stays backward-compatible with the SNS-era body ({tenant, data});
 * everything new travels in Kafka headers so existing consumer transforms
 * keep working during the migration.
 */
final class Envelope
{
    /**
     * @param array<string, mixed> $data
     */
    private function __construct(
        public readonly string $tenant,
        public readonly array $data,
        public readonly string $eventType,
        public readonly string $messageId,
        public readonly string $eventTime,
        public readonly int $schemaVersion,
        public readonly string $sourceService,
        public readonly string $traceId,
        /**
         * The topic this message was READ from. Null on the produce side — it
         * is not part of the wire format, it is where the message was found.
         *
         * Handlers need it because cutover is per topic: a consumer subscribed
         * to many topics may be authoritative for one of them and still in
         * shadow for the rest, and only the topic distinguishes them.
         */
        public readonly ?string $sourceTopic = null,
    ) {
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function create(
        string $tenant,
        array $data,
        string $eventType,
        string $sourceService,
        ?string $messageId = null,
        ?string $eventTime = null,
        int $schemaVersion = 1,
        ?string $traceId = null,
    ): self {
        $tenant = trim($tenant);
        $eventType = trim($eventType);
        $sourceService = trim($sourceService);

        if ($tenant === '') {
            // Phase 0.5 enforcement: tenant is the partition key — an event
            // without it cannot be ordered and must never reach a topic.
            throw new InvalidEnvelopeException('Envelope requires a non-empty tenant (it is the partition key).');
        }
        if ($eventType === '') {
            throw new InvalidEnvelopeException('Envelope requires a non-empty event_type.');
        }
        if ($sourceService === '') {
            throw new InvalidEnvelopeException('Envelope requires a non-empty source_service.');
        }
        if ($schemaVersion < 1) {
            throw new InvalidEnvelopeException('schema_version must be >= 1.');
        }

        return new self(
            tenant: $tenant,
            data: $data,
            eventType: $eventType,
            messageId: $messageId ?? Uuid7::generate(),
            eventTime: $eventTime ?? (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->format('Y-m-d\TH:i:s.v\Z'),
            schemaVersion: $schemaVersion,
            sourceService: $sourceService,
            traceId: $traceId ?? Uuid7::generate(),
        );
    }

    /**
     * Rebuild an envelope on the consumer side from raw Kafka headers + value.
     *
     * Tolerant on purpose: during migration some fields may be missing from
     * messages produced by not-yet-upgraded publishers. tenant falls back to
     * the payload body; a missing message_id yields '' (idempotency layer
     * treats that as "cannot dedupe" and logs it, rather than dropping).
     *
     * @param array<string, string> $headers
     * @param string|null $sourceTopic The topic the message was read from, when known.
     */
    public static function fromConsumed(array $headers, string $payloadJson, ?string $sourceTopic = null): self
    {
        try {
            $payload = json_decode($payloadJson, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new InvalidEnvelopeException('Envelope payload is not valid JSON: ' . $e->getMessage(), previous: $e);
        }

        if (!is_array($payload)) {
            throw new InvalidEnvelopeException('Envelope payload must be a JSON object, got ' . gettype($payload) . '.');
        }

        $tenant = trim((string) ($headers['tenant'] ?? $payload['tenant'] ?? ''));
        $eventType = trim((string) ($headers['event_type'] ?? ''));

        if ($tenant === '') {
            throw new InvalidEnvelopeException('Consumed message has no tenant in headers or payload.');
        }
        if ($eventType === '') {
            throw new InvalidEnvelopeException('Consumed message has no event_type header.');
        }

        // 'data' must be PRESENT and an array. A truncated payload silently
        // becoming [] would hand handlers an empty event (and burn its
        // message_id in the idempotency store) — reject it as malformed.
        if (!is_array($payload) || !array_key_exists('data', $payload) || !is_array($payload['data'])) {
            throw new InvalidEnvelopeException('Consumed message payload must be an object with a "data" object/array.');
        }
        $data = $payload['data'];

        return new self(
            tenant: $tenant,
            data: $data,
            eventType: $eventType,
            messageId: trim((string) ($headers['message_id'] ?? '')),
            eventTime: trim((string) ($headers['event_time'] ?? '')),
            schemaVersion: max(1, (int) ($headers['schema_version'] ?? 1)),
            sourceService: trim((string) ($headers['source_service'] ?? '')),
            traceId: trim((string) ($headers['trace_id'] ?? '')),
            // Prefer the ORIGINAL topic: a message replayed from a retry or DLQ
            // topic must still be judged as the topic it came from, not as
            // "<group>.<topic>.retry".
            sourceTopic: ($headers[MessageProcessor::HEADER_ORIGINAL_TOPIC] ?? null) ?: $sourceTopic,
        );
    }

    /**
     * @return array<string, string>
     */
    public function toHeaders(): array
    {
        return [
            'event_type' => $this->eventType,
            'message_id' => $this->messageId,
            'event_time' => $this->eventTime,
            'schema_version' => (string) $this->schemaVersion,
            'source_service' => $this->sourceService,
            'tenant' => $this->tenant,
            'trace_id' => $this->traceId,
        ];
    }

    public function toPayload(): string
    {
        return json_encode(
            ['tenant' => $this->tenant, 'data' => $this->data],
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
        );
    }
}
