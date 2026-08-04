<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;
use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * The heart of the consumer: idempotency → route by event_type → handle with
 * immediate retries → escalate to retry topic → DLQ. Framework-agnostic and
 * fully testable without a broker; the polling loop (KafkaConsumer / rdkafka)
 * just feeds it messages.
 *
 * Failure policy (deliberate, reviewed):
 * - Idempotency store DOWN: fail-open. wasProcessed() failure logs and
 *   processes anyway; markProcessed() failure after a successful handler logs
 *   and still returns Processed. Duplicates are then bounded by Kafka's
 *   at-least-once redelivery — never amplified by this loop.
 * - Escalation (retry/DLQ publish) FAILED: the outcome is EscalationFailed and
 *   the runner must NOT commit the offset — redelivery re-drives the whole
 *   flow; idempotency skips the already-processed prefix.
 * - No escalation producer configured: permanent failures return Dropped
 *   (visible, committed) — not DeadLettered, because nothing was parked.
 * - Process CRASH between handler success and markProcessed/commit: inherent
 *   to at-least-once and unclosable here (no distributed transaction) — the
 *   message is redelivered and re-processed once. Handlers must tolerate
 *   duplicates; services wanting a hard guarantee write the dedupe key inside
 *   the handler's own DB transaction (inbox pattern).
 */
final class MessageProcessor
{
    public const HEADER_RETRY_ROUND = 'x-retry-round';
    public const HEADER_RETRY_DELAY = 'x-retry-delay-seconds';
    public const HEADER_ORIGINAL_TOPIC = 'x-original-topic';
    public const HEADER_LAST_ERROR = 'x-last-error';

    /** @var array<string, callable(Envelope): void> */
    private array $handlers = [];

    public function __construct(
        private readonly string $consumerGroup,
        private readonly ?IdempotencyStoreInterface $idempotency = null,
        private readonly ?ProducerDriverInterface $escalationProducer = null,
        private readonly RetryPolicy $retryPolicy = new RetryPolicy(),
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
        if ($this->consumerGroup === '') {
            throw new \InvalidArgumentException('consumerGroup must not be empty.');
        }
    }

    /**
     * @param callable(Envelope): void $handler
     */
    public function onEvent(string $eventType, callable $handler): self
    {
        $this->handlers[$eventType] = $handler;

        return $this;
    }

    public function process(ConsumedMessage $message): ProcessOutcome
    {
        $originalTopic = $message->headers[self::HEADER_ORIGINAL_TOPIC] ?? $message->topic;

        try {
            $envelope = Envelope::fromConsumed($message->headers, $message->payload);
        } catch (\Throwable $e) {
            // Any parse failure — not only InvalidEnvelopeException — would
            // fail forever; parking it in the DLQ keeps the partition moving
            // (poison-message rule, 04-runbook). DLQ is addressed by the
            // ORIGINAL topic so retry-topic poison lands in the right place.
            $this->logger->error('Kafka message malformed, dead-lettering', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $message->topic,
                'original_topic' => $originalTopic,
                'error' => $e->getMessage(),
            ]]);

            if (!$this->escalate($message, $this->dlqTopic($originalTopic), null, $e->getMessage(), $originalTopic)) {
                return $this->escalationProducer === null ? ProcessOutcome::Dropped : ProcessOutcome::EscalationFailed;
            }

            return ProcessOutcome::Malformed;
        }

        $handler = $this->handlers[$envelope->eventType] ?? null;
        if ($handler === null) {
            // Not an error: topics carry many event types and each consumer
            // handles a subset — everything else is acknowledged and skipped.
            // Logged at debug so a typo'd onEvent() registration is findable.
            $this->logger->debug('Kafka message skipped — no handler for event_type', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $message->topic,
                'event_type' => $envelope->eventType,
                'tenant' => $envelope->tenant,
            ]]);

            return ProcessOutcome::NoHandler;
        }

        if ($this->isDuplicate($message, $envelope)) {
            return ProcessOutcome::Duplicate;
        }

        $lastError = null;
        for ($attempt = 1; $attempt <= $this->retryPolicy->immediateAttempts; $attempt++) {
            try {
                $handler($envelope);
            } catch (\Throwable $e) {
                $lastError = $e;
                continue;
            }

            // Handler SUCCEEDED — its success stands no matter what the
            // idempotency write does. A failed mark only widens the dedupe
            // window to Kafka's own redelivery; it must never re-run the
            // handler or escalate a succeeded message.
            $this->markProcessedSafely($message, $envelope);

            return ProcessOutcome::Processed;
        }

        return $this->escalateAfterFailure($message, $envelope, $lastError, $originalTopic);
    }

    private function isDuplicate(ConsumedMessage $message, Envelope $envelope): bool
    {
        if ($this->idempotency === null) {
            return false;
        }

        if ($envelope->messageId === '') {
            $this->logger->warning('Kafka message without message_id — cannot dedupe, processing anyway', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $message->topic,
                'event_type' => $envelope->eventType,
                'tenant' => $envelope->tenant,
            ]]);

            return false;
        }

        try {
            return $this->idempotency->wasProcessed($this->consumerGroup, $envelope->messageId);
        } catch (\Throwable $e) {
            // Fail-open: a store outage must not halt every partition. The
            // handlers are required to be duplicate-tolerant anyway
            // (at-least-once), so processing without the dedupe gate is the
            // lesser evil — and it is logged, not silent.
            $this->logger->warning('Kafka idempotency store unavailable — cannot dedupe, processing anyway', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $message->topic,
                'event_type' => $envelope->eventType,
                'tenant' => $envelope->tenant,
                'message_id' => $envelope->messageId,
                'error' => $e->getMessage(),
            ]]);

            return false;
        }
    }

    private function markProcessedSafely(ConsumedMessage $message, Envelope $envelope): void
    {
        if ($this->idempotency === null || $envelope->messageId === '') {
            return;
        }

        try {
            $this->idempotency->markProcessed($this->consumerGroup, $envelope->messageId);
        } catch (\Throwable $e) {
            $this->logger->warning('Kafka markProcessed failed after successful handling — dedupe falls back to redelivery window', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $message->topic,
                'event_type' => $envelope->eventType,
                'tenant' => $envelope->tenant,
                'message_id' => $envelope->messageId,
                'error' => $e->getMessage(),
            ]]);
        }
    }

    private function escalateAfterFailure(
        ConsumedMessage $message,
        Envelope $envelope,
        \Throwable $error,
        string $originalTopic,
    ): ProcessOutcome {
        // Wire headers are untrusted input (hand-edited DLQ replays, foreign
        // producers): clamp so a negative/garbage round can never explode
        // downstream math — it just restarts the round ladder.
        $round = max(0, (int) ($message->headers[self::HEADER_RETRY_ROUND] ?? 0));

        if ($this->escalationProducer === null) {
            // No retry/DLQ wiring (early-migration groups): the loss is made
            // VISIBLE — outcome Dropped, loudest log level. Not DeadLettered:
            // nothing was parked anywhere.
            $this->logger->error('Kafka handler failed and no retry/DLQ producer is configured — message dropped after immediate attempts', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $message->topic,
                'event_type' => $envelope->eventType,
                'tenant' => $envelope->tenant,
                'message_id' => $envelope->messageId,
                'error' => $error->getMessage(),
            ]]);

            return ProcessOutcome::Dropped;
        }

        if ($round < $this->retryPolicy->maxRounds()) {
            $nextRound = $round + 1;
            if (!$this->escalate($message, $this->retryTopic($originalTopic), $nextRound, $error->getMessage(), $originalTopic)) {
                return ProcessOutcome::EscalationFailed;
            }

            $this->logger->warning('Kafka message escalated to retry topic', ['data' => [
                'group' => $this->consumerGroup,
                'topic' => $originalTopic,
                'event_type' => $envelope->eventType,
                'tenant' => $envelope->tenant,
                'message_id' => $envelope->messageId,
                'round' => $nextRound,
                'delay_seconds' => $this->retryPolicy->delayForRound($nextRound),
                'error' => $error->getMessage(),
            ]]);

            return ProcessOutcome::Retried;
        }

        if (!$this->escalate($message, $this->dlqTopic($originalTopic), null, $error->getMessage(), $originalTopic)) {
            return ProcessOutcome::EscalationFailed;
        }

        $this->logger->error('Kafka message dead-lettered after exhausting retries', ['data' => [
            'group' => $this->consumerGroup,
            'topic' => $originalTopic,
            'event_type' => $envelope->eventType,
            'tenant' => $envelope->tenant,
            'message_id' => $envelope->messageId,
            'rounds' => $round,
            'error' => $error->getMessage(),
        ]]);

        return ProcessOutcome::DeadLettered;
    }

    /**
     * Publish a copy of the message to a retry/DLQ topic.
     * Returns false when there is no producer or the publish failed —
     * the caller decides the outcome (and the runner withholds the commit).
     */
    private function escalate(
        ConsumedMessage $message,
        string $targetTopic,
        ?int $nextRound,
        string $error,
        string $originalTopic,
    ): bool {
        if ($this->escalationProducer === null) {
            return false;
        }

        $headers = $message->headers;
        $headers[self::HEADER_ORIGINAL_TOPIC] = $originalTopic;
        $headers[self::HEADER_LAST_ERROR] = mb_substr($error, 0, 500);

        if ($nextRound !== null) {
            $headers[self::HEADER_RETRY_ROUND] = (string) $nextRound;
            $headers[self::HEADER_RETRY_DELAY] = (string) $this->retryPolicy->delayForRound($nextRound);
        }

        try {
            $this->escalationProducer->send($targetTopic, $message->key ?? '', $headers, $message->payload);

            return true;
        } catch (\Throwable $e) {
            // The one place that must scream: without the escalation copy the
            // message only survives behind an UNCOMMITTED offset — the runner
            // sees EscalationFailed and refuses to commit, so Kafka redelivers.
            $this->logger->critical('Kafka escalation publish failed — offset must not be committed', ['data' => [
                'group' => $this->consumerGroup,
                'target_topic' => $targetTopic,
                'source_topic' => $message->topic,
                'error' => $e->getMessage(),
            ]]);

            return false;
        }
    }

    private function retryTopic(string $originalTopic): string
    {
        return "{$this->consumerGroup}.{$originalTopic}.retry";
    }

    private function dlqTopic(string $originalTopic): string
    {
        return "{$this->consumerGroup}.{$originalTopic}.dlq";
    }
}
