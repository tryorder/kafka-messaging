<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Drivers;

use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

/**
 * Shadow/dev driver: "publishes" by logging the full wire message. Used for
 * the dual-write dry-run phase — services can flip KAFKA_MESSAGING_DRIVER=log
 * and watch exactly what WOULD hit the broker, before any broker exists.
 */
final class LogProducerDriver implements ProducerDriverInterface
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly string $level = LogLevel::INFO,
    ) {
    }

    public function send(string $topic, string $key, array $headers, string $payload): void
    {
        $this->logger->log($this->level, 'Kafka shadow publish', ['data' => [
            'topic' => $topic,
            'key' => $key,
            'headers' => $headers,
            'payload' => $payload,
        ]]);
    }
}
