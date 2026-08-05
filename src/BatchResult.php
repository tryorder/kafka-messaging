<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

/**
 * Outcome of a publishBatch() call: the envelopes that were built and the
 * per-message failures, keyed by their index in the input array.
 */
final class BatchResult
{
    /**
     * @param list<Envelope> $delivered
     * @param list<array{index: int, error: string}> $failures
     */
    public function __construct(
        private readonly array $delivered,
        private readonly array $failures,
    ) {
    }

    /**
     * @return list<Envelope>
     */
    public function delivered(): array
    {
        return $this->delivered;
    }

    /**
     * @return list<array{index: int, error: string}>
     */
    public function failed(): array
    {
        return $this->failures;
    }

    public function count(): int
    {
        return count($this->delivered);
    }

    public function failedCount(): int
    {
        return count($this->failures);
    }

    public function allDelivered(): bool
    {
        return $this->failures === [];
    }
}
