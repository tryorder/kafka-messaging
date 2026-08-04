<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Contracts;

interface ProducerDriverInterface
{
    /**
     * Deliver one message to a topic. Implementations must throw on failure —
     * the safe/unsafe distinction is the producer's job, not the driver's.
     *
     * @param array<string, string> $headers
     */
    public function send(string $topic, string $key, array $headers, string $payload): void;
}
