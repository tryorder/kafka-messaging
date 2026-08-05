<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Contracts;

use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\ProcessOutcome;

/**
 * Observation hooks for the consume loop — the plan's Phase-0 exit gate needs
 * per-outcome counts and processing latency exported somewhere (Prometheus,
 * StatsD, logs). Implementations must never throw; the loop does not guard them
 * beyond a catch-all, and a metrics backend must not stop consumption.
 */
interface ConsumerMetricsInterface
{
    public function beforeProcess(string $consumerGroup, ConsumedMessage $message): void;

    public function afterProcess(
        string $consumerGroup,
        ConsumedMessage $message,
        ProcessOutcome $outcome,
        float $durationMs,
    ): void;
}
