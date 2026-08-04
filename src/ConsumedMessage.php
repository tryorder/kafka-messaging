<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

final class ConsumedMessage
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public readonly string $topic,
        public readonly ?string $key,
        public readonly array $headers,
        public readonly string $payload,
        public readonly int $partition = 0,
        public readonly int $offset = 0,
    ) {
    }
}
