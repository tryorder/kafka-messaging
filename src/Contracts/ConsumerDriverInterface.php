<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Contracts;

use Order\KafkaMessaging\ConsumedMessage;

interface ConsumerDriverInterface
{
    /**
     * @param list<string> $topics
     */
    public function subscribe(string $group, array $topics): void;

    /**
     * Return the next message, or null when nothing is available right now.
     */
    public function poll(int $timeoutMs): ?ConsumedMessage;

    /**
     * Acknowledge/commit a message after the processor decided its fate.
     */
    public function commit(ConsumedMessage $message): void;
}
