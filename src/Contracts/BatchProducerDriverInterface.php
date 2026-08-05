<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Contracts;

interface BatchProducerDriverInterface extends ProducerDriverInterface
{
    /**
     * Deliver many messages with a single flush.
     *
     * Implementations report per-message failures instead of throwing on the
     * first one: one bad message must not discard the rest of the batch.
     *
     * @param string $topic
     * @param list<array{key: string, headers: array<string, string>, payload: string}> $messages
     *
     * @return list<array{index: int, error: string}> failures, empty when all were delivered
     */
    public function sendBatch(string $topic, array $messages): array;
}
