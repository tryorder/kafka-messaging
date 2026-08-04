<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;

/**
 * Per-process store for tests/dev. Services should bind a Redis- or
 * DB-backed implementation (processed_messages table / SETNX with TTL) —
 * see 03-message-envelope.md §3.
 */
final class InMemoryIdempotencyStore implements IdempotencyStoreInterface
{
    /** @var array<string, true> */
    private array $seen = [];

    public function wasProcessed(string $consumerGroup, string $messageId): bool
    {
        return isset($this->seen[$consumerGroup . '|' . $messageId]);
    }

    public function markProcessed(string $consumerGroup, string $messageId): void
    {
        $this->seen[$consumerGroup . '|' . $messageId] = true;
    }
}
