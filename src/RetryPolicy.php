<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

/**
 * 02-kafka-topics.md §5: N immediate in-process attempts, then escalating
 * retry-topic rounds (5s/1m/10m), then DLQ. The round delay is carried as a
 * header so the (future) delayed-consumer knows how long to park the message;
 * the core only tracks round counting and escalation.
 */
final class RetryPolicy
{
    /**
     * @param list<int> $roundDelaysSeconds
     */
    public function __construct(
        public readonly int $immediateAttempts = 3,
        public readonly array $roundDelaysSeconds = [5, 60, 600],
    ) {
        if ($this->immediateAttempts < 1) {
            throw new \InvalidArgumentException('immediateAttempts must be >= 1.');
        }
    }

    public function maxRounds(): int
    {
        return count($this->roundDelaysSeconds);
    }

    public function delayForRound(int $round): int
    {
        if ($this->roundDelaysSeconds === []) {
            return 0;
        }

        // No end(): it takes its argument by reference, which fatals on a
        // readonly property. Out-of-range rounds clamp to the last delay.
        return $this->roundDelaysSeconds[max(0, $round - 1)]
            ?? $this->roundDelaysSeconds[array_key_last($this->roundDelaysSeconds)];
    }
}
