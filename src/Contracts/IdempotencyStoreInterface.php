<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Contracts;

interface IdempotencyStoreInterface
{
    public function wasProcessed(string $consumerGroup, string $messageId): bool;

    public function markProcessed(string $consumerGroup, string $messageId): void;
}
