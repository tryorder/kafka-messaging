<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Exceptions\InvalidEnvelopeException;

/**
 * Migration bridge: turn an SNS-era message (Subject + {tenant, data}) into a
 * unified Envelope. Used during dual-write/shadow phases so the SAME logical
 * event carries the SAME message_id on both paths — consumer idempotency can
 * then dedupe regardless of which pipe delivered it first.
 */
final class SnsBridge
{
    /**
     * @param array<string, mixed> $snsMessage The decoded SNS "Message" body — the {tenant, data} shape.
     */
    public static function toEnvelope(
        string $subject,
        array $snsMessage,
        string $sourceService,
        ?string $snsMessageId = null,
        ?string $snsTimestamp = null,
    ): Envelope {
        $tenant = $snsMessage['tenant'] ?? null;
        if (!is_string($tenant) || trim($tenant) === '') {
            throw new InvalidEnvelopeException(
                "SNS message for subject '{$subject}' has no tenant — normalize the publisher first (Phase 0.5)."
            );
        }

        $data = $snsMessage['data'] ?? null;
        if (!is_array($data)) {
            throw new InvalidEnvelopeException(
                "SNS message for subject '{$subject}' has no data object — normalize the publisher first (Phase 0.5)."
            );
        }

        return Envelope::create(
            tenant: $tenant,
            data: $data,
            eventType: $subject,          // Subject IS the event_type (02-kafka-topics.md §core principle)
            sourceService: $sourceService,
            messageId: $snsMessageId,     // reuse SNS MessageId → cross-path idempotency
            eventTime: $snsTimestamp,
        );
    }
}
