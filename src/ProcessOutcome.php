<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

enum ProcessOutcome: string
{
    case Processed = 'processed';
    case Duplicate = 'duplicate';
    case Retried = 'retried';
    case DeadLettered = 'dead_lettered';
    case NoHandler = 'no_handler';
    case Malformed = 'malformed';

    /**
     * Permanent failure with NO retry/DLQ wiring configured — the message was
     * NOT parked anywhere. Visible loss, committed.
     */
    case Dropped = 'dropped';

    /**
     * The retry/DLQ publish itself failed — the message survives only behind
     * its offset. The runner MUST NOT commit; Kafka will redeliver.
     */
    case EscalationFailed = 'escalation_failed';

    public function isCommittable(): bool
    {
        return $this !== self::EscalationFailed;
    }
}
