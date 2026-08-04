<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Drivers;

use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Contracts\ConsumerDriverInterface;

/**
 * Test/dev driver: a FIFO queue fed programmatically.
 */
final class InMemoryConsumerDriver implements ConsumerDriverInterface
{
    /** @var list<ConsumedMessage> */
    private array $queue = [];

    /** @var list<ConsumedMessage> */
    public array $committed = [];

    public ?string $group = null;

    /** @var list<string> */
    public array $subscribedTopics = [];

    public function feed(ConsumedMessage $message): void
    {
        $this->queue[] = $message;
    }

    public function subscribe(string $group, array $topics): void
    {
        $this->group = $group;
        $this->subscribedTopics = $topics;
    }

    public function poll(int $timeoutMs): ?ConsumedMessage
    {
        return array_shift($this->queue);
    }

    public function commit(ConsumedMessage $message): void
    {
        $this->committed[] = $message;
    }
}
