<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Contracts\ConsumerDriverInterface;
use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;
use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Fluent consumer entry point (03-message-envelope.md §5):
 *
 *   KafkaConsumer::group('svc-payment')
 *       ->driver($driver)
 *       ->subscribe(['order.events'])
 *       ->onEvent('OrderCreatedEvent', $handler)
 *       ->withIdempotency($store)
 *       ->withRetryAndDlq($producerDriver)
 *       ->run();
 */
final class KafkaConsumer
{
    /** @var list<string> */
    private array $topics = [];

    /** @var array<string, callable(Envelope): void> */
    private array $handlers = [];

    private ?ConsumerDriverInterface $driver = null;
    private ?IdempotencyStoreInterface $idempotency = null;
    private ?ProducerDriverInterface $escalationProducer = null;
    private ?\Order\KafkaMessaging\Contracts\ConsumerMetricsInterface $metrics = null;

    /** @var list<callable(ConsumedMessage): void> */
    private array $beforeCallbacks = [];

    /** @var list<callable(ConsumedMessage, ProcessOutcome): void> */
    private array $afterCallbacks = [];
    private RetryPolicy $retryPolicy;
    private LoggerInterface $logger;

    private function __construct(private readonly string $group)
    {
        $this->retryPolicy = new RetryPolicy();
        $this->logger = new NullLogger();
    }

    public static function group(string $group): self
    {
        if (trim($group) === '') {
            throw new \InvalidArgumentException('Consumer group must not be empty.');
        }

        return new self(trim($group));
    }

    public function driver(ConsumerDriverInterface $driver): self
    {
        $this->driver = $driver;

        return $this;
    }

    /**
     * @param list<string> $topics
     */
    public function subscribe(array $topics): self
    {
        $this->topics = array_values(array_unique(array_merge($this->topics, $topics)));

        return $this;
    }

    /**
     * @param callable(Envelope): void $handler
     */
    public function onEvent(string $eventType, callable $handler): self
    {
        $this->handlers[$eventType] = $handler;

        return $this;
    }

    public function withIdempotency(IdempotencyStoreInterface $store): self
    {
        $this->idempotency = $store;

        return $this;
    }

    public function withRetryAndDlq(ProducerDriverInterface $escalationProducer, ?RetryPolicy $policy = null): self
    {
        $this->escalationProducer = $escalationProducer;
        if ($policy !== null) {
            $this->retryPolicy = $policy;
        }

        return $this;
    }

    public function logger(LoggerInterface $logger): self
    {
        $this->logger = $logger;

        return $this;
    }

    public function withMetrics(\Order\KafkaMessaging\Contracts\ConsumerMetricsInterface $metrics): self
    {
        $this->metrics = $metrics;

        return $this;
    }

    /**
     * @param callable(ConsumedMessage): void $callback
     */
    public function before(callable $callback): self
    {
        $this->beforeCallbacks[] = $callback;

        return $this;
    }

    /**
     * @param callable(ConsumedMessage, ProcessOutcome): void $callback
     */
    public function after(callable $callback): self
    {
        $this->afterCallbacks[] = $callback;

        return $this;
    }

    private bool $shouldStop = false;
    private bool $honorRetryDelays = false;

    /** @var callable(int): void */
    private $sleeper = null;

    /**
     * Retry-daemon mode: before processing a message carrying
     * x-retry-not-before in the future, park until it becomes eligible.
     * Blocking the (dedicated) retry partition is the intended pattern.
     */
    public function honorRetryDelays(?callable $sleeper = null): self
    {
        $this->honorRetryDelays = true;
        $this->sleeper = $sleeper;

        return $this;
    }

    /**
     * Graceful shutdown — service daemons wire this to SIGTERM/SIGINT
     * (pcntl_async_signals(true) + pcntl_signal(SIGTERM, fn () => $c->stop())).
     */
    public function stop(): void
    {
        $this->shouldStop = true;
    }

    /**
     * Poll and process. With $exitOnIdle=true (default, batch/test mode) the
     * loop ends on the first empty poll; with false it keeps polling until
     * stop() — the long-running daemon the migration plan describes.
     * Returns per-outcome counts.
     *
     * Commit policy: every outcome commits EXCEPT EscalationFailed — there the
     * message survives only behind its offset, so the loop stops without
     * committing and Kafka redelivers (idempotency skips the processed prefix).
     *
     * @return array<string, int>
     */
    public function run(?int $maxMessages = null, int $pollTimeoutMs = 1000, bool $exitOnIdle = true): array
    {
        if ($this->driver === null) {
            throw new \LogicException('No consumer driver configured — call driver() first.');
        }
        if ($this->topics === []) {
            throw new \LogicException('No topics to subscribe to — call subscribe() first.');
        }

        $processor = new MessageProcessor(
            consumerGroup: $this->group,
            idempotency: $this->idempotency,
            escalationProducer: $this->escalationProducer,
            retryPolicy: $this->retryPolicy,
            logger: $this->logger,
        );
        foreach ($this->handlers as $eventType => $handler) {
            $processor->onEvent($eventType, $handler);
        }

        $this->driver->subscribe($this->group, $this->topics);

        $counts = [];
        $processed = 0;

        while (!$this->shouldStop && ($maxMessages === null || $processed < $maxMessages)) {
            $message = $this->driver->poll($pollTimeoutMs);
            if ($message === null) {
                if ($exitOnIdle) {
                    break;
                }
                continue;
            }

            if ($this->honorRetryDelays) {
                $remaining = (int) ($message->headers[MessageProcessor::HEADER_RETRY_NOT_BEFORE] ?? 0) - time();
                if ($remaining > 0) {
                    ($this->sleeper ?? static fn (int $s) => sleep($s))($remaining);
                }
                if ($this->shouldStop) {
                    // Interrupted mid-park (SIGTERM breaks sleep with
                    // pcntl_async_signals): leave WITHOUT processing or
                    // committing — redelivery re-parks it.
                    break;
                }
            }

            $this->fire($this->beforeCallbacks, [$message]);
            $this->observe(fn () => $this->metrics?->beforeProcess($this->group, $message));

            $startedAt = microtime(true);
            $outcome = $processor->process($message);
            $durationMs = (microtime(true) - $startedAt) * 1000;

            $this->observe(fn () => $this->metrics?->afterProcess($this->group, $message, $outcome, $durationMs));
            $this->fire($this->afterCallbacks, [$message, $outcome]);

            $counts[$outcome->value] = ($counts[$outcome->value] ?? 0) + 1;
            $processed++;

            if (!$outcome->isCommittable()) {
                // Do not advance past a message that exists nowhere else —
                // stop the loop and let redelivery re-drive it.
                break;
            }

            $this->driver->commit($message);
        }

        return $counts;
    }

    /**
     * Observation must never break consumption — a failing metrics backend or
     * callback is logged and swallowed.
     *
     * @param list<callable> $callbacks
     * @param list<mixed> $args
     */
    private function fire(array $callbacks, array $args): void
    {
        foreach ($callbacks as $callback) {
            $this->observe(static fn () => $callback(...$args));
        }
    }

    private function observe(callable $fn): void
    {
        try {
            $fn();
        } catch (\Throwable $e) {
            $this->logger->warning('Kafka observation hook failed (ignored)', ['data' => [
                'group' => $this->group,
                'error' => $e->getMessage(),
            ]]);
        }
    }
}
