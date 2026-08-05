<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Drivers;

use Order\KafkaMessaging\Contracts\BatchProducerDriverInterface;
use Order\KafkaMessaging\Exceptions\PublishFailedException;

/**
 * Test/dev driver: collects messages per topic in memory.
 * Can be told to fail to exercise safePublish()/retry paths.
 */
final class InMemoryProducerDriver implements BatchProducerDriverInterface
{
    /** @var array<string, list<array{key: string, headers: array<string, string>, payload: string}>> */
    public array $topics = [];

    public bool $failNext = false;

    /** @var list<int> batch indexes to fail, for exercising partial-batch handling */
    public array $failBatchIndexes = [];

    public function send(string $topic, string $key, array $headers, string $payload): void
    {
        if ($this->failNext) {
            $this->failNext = false;
            throw new PublishFailedException('InMemoryProducerDriver was instructed to fail.');
        }

        $this->topics[$topic][] = ['key' => $key, 'headers' => $headers, 'payload' => $payload];
    }

    public function sendBatch(string $topic, array $messages): array
    {
        $failures = [];

        foreach (array_values($messages) as $i => $message) {
            if (in_array($i, $this->failBatchIndexes, true)) {
                $failures[] = ['index' => $i, 'error' => "InMemoryProducerDriver was told to fail index {$i}."];
                continue;
            }

            $this->topics[$topic][] = $message;
        }

        return $failures;
    }

    /**
     * @return list<array{key: string, headers: array<string, string>, payload: string}>
     */
    public function messagesOn(string $topic): array
    {
        return $this->topics[$topic] ?? [];
    }
}
