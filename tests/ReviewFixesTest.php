<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Contracts\ConsumerDriverInterface;
use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;
use Order\KafkaMessaging\Drivers\InMemoryConsumerDriver;
use Order\KafkaMessaging\Drivers\InMemoryProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\KafkaConsumer;
use Order\KafkaMessaging\KafkaProducer;
use Order\KafkaMessaging\MessageProcessor;
use Order\KafkaMessaging\ProcessOutcome;
use Order\KafkaMessaging\RetryPolicy;
use Order\KafkaMessaging\SnsBridge;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for the confirmed findings of the adversarial review
 * (2026-08-04): duplicate-amplification, silent escalation loss, store-outage
 * crash loop, no daemon mode, malformed-DLQ addressing, dropped visibility.
 */
final class ReviewFixesTest extends TestCase
{
    private function message(string $topic = 'order.events', array $extraHeaders = []): ConsumedMessage
    {
        $e = Envelope::create('jabourih', ['order_id' => 'ord_1'], 'OrderCreatedEvent', 'service-order');

        return new ConsumedMessage($topic, 'jabourih', array_merge($e->toHeaders(), $extraHeaders), $e->toPayload());
    }

    private function throwingStore(bool $onRead, bool $onWrite): IdempotencyStoreInterface
    {
        return new class($onRead, $onWrite) implements IdempotencyStoreInterface {
            public int $reads = 0;
            public int $writes = 0;

            public function __construct(private readonly bool $onRead, private readonly bool $onWrite)
            {
            }

            public function wasProcessed(string $consumerGroup, string $messageId): bool
            {
                $this->reads++;
                if ($this->onRead) {
                    throw new \RuntimeException('store down (read)');
                }

                return false;
            }

            public function markProcessed(string $consumerGroup, string $messageId): void
            {
                $this->writes++;
                if ($this->onWrite) {
                    throw new \RuntimeException('store down (write)');
                }
            }
        };
    }

    public function testMarkProcessedFailureDoesNotRerunSuccessfulHandler(): void
    {
        $runs = 0;
        $processor = new MessageProcessor(
            'svc-wallet',
            idempotency: $this->throwingStore(onRead: false, onWrite: true),
            escalationProducer: $escalation = new InMemoryProducerDriver(),
        );
        $processor->onEvent('OrderCreatedEvent', function () use (&$runs): void {
            $runs++;
        });

        $outcome = $processor->process($this->message());

        $this->assertSame(ProcessOutcome::Processed, $outcome, 'handler success stands despite store write failure');
        $this->assertSame(1, $runs, 'the wallet deposit must NOT run twice');
        $this->assertSame([], $escalation->topics, 'a succeeded message must never be escalated');
    }

    public function testIdempotencyReadFailureFailsOpenInsteadOfCrashing(): void
    {
        $runs = 0;
        $processor = new MessageProcessor('svc-payment', idempotency: $this->throwingStore(onRead: true, onWrite: false));
        $processor->onEvent('OrderCreatedEvent', function () use (&$runs): void {
            $runs++;
        });

        $this->assertSame(ProcessOutcome::Processed, $processor->process($this->message()));
        $this->assertSame(1, $runs, 'store outage must not halt consumption');
    }

    public function testEscalationFailureWithholdsCommitAndStopsTheLoop(): void
    {
        $escalation = new InMemoryProducerDriver();
        $escalation->failNext = true;

        $driver = new InMemoryConsumerDriver();
        $driver->feed($this->message());
        $driver->feed($this->message()); // must NOT be reached after the failed escalation

        $counts = KafkaConsumer::group('svc-payment')
            ->driver($driver)
            ->subscribe(['order.events'])
            ->onEvent('OrderCreatedEvent', fn () => throw new \RuntimeException('handler down'))
            ->withRetryAndDlq($escalation, new RetryPolicy(immediateAttempts: 1))
            ->run();

        $this->assertSame(['escalation_failed' => 1], $counts);
        $this->assertCount(0, $driver->committed, 'offset must NOT be committed past an un-escalated message');
    }

    public function testMalformedMessageFromRetryTopicDeadLettersToOriginalTopicName(): void
    {
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor('svc-payment', escalationProducer: $escalation);

        $outcome = $processor->process(new ConsumedMessage(
            topic: 'svc-payment.order.events.retry',
            key: 'jabourih',
            headers: [MessageProcessor::HEADER_ORIGINAL_TOPIC => 'order.events'],
            payload: '{broken',
        ));

        $this->assertSame(ProcessOutcome::Malformed, $outcome);
        $this->assertCount(1, $escalation->messagesOn('svc-payment.order.events.dlq'), 'DLQ addressed by ORIGINAL topic');
        $this->assertSame([], $escalation->messagesOn('svc-payment.svc-payment.order.events.retry.dlq'), 'no doubled topic name');
    }

    public function testPermanentFailureWithoutEscalationWiringIsVisiblyDropped(): void
    {
        $processor = new MessageProcessor('svc-sms', retryPolicy: new RetryPolicy(immediateAttempts: 1));
        $processor->onEvent('OrderCreatedEvent', fn () => throw new \RuntimeException('boom'));

        $this->assertSame(ProcessOutcome::Dropped, $processor->process($this->message()));
        $this->assertTrue(ProcessOutcome::Dropped->isCommittable(), 'dropped is visible but committed');
    }

    public function testPayloadWithoutDataKeyIsMalformedNotSilentlyEmpty(): void
    {
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor('svc-payment', escalationProducer: $escalation);
        $processor->onEvent('OrderCreatedEvent', fn () => throw new \LogicException('must not run'));

        $outcome = $processor->process(new ConsumedMessage(
            topic: 'order.events',
            key: 'jabourih',
            headers: ['event_type' => 'OrderCreatedEvent', 'tenant' => 'jabourih'],
            payload: '{"tenant":"jabourih"}',
        ));

        $this->assertSame(ProcessOutcome::Malformed, $outcome);
        $this->assertCount(1, $escalation->messagesOn('svc-payment.order.events.dlq'));
    }

    public function testProducerReusesCallerSuppliedMessageIdForBridging(): void
    {
        $driver = new InMemoryProducerDriver();
        $producer = new KafkaProducer($driver, 'service-order');

        $envelope = $producer->publish(
            'order.events',
            'OrderCreatedEvent',
            'jabourih',
            [],
            messageId: 'sns-msg-id-123',
            eventTime: '2026-08-04T20:00:00.000Z',
        );

        $this->assertSame('sns-msg-id-123', $envelope->messageId);
        $this->assertSame('sns-msg-id-123', $driver->messagesOn('order.events')[0]['headers']['message_id']);
        $this->assertSame('2026-08-04T20:00:00.000Z', $envelope->eventTime);
    }

    public function testSnsBridgeMapsSubjectAndReusesSnsMessageId(): void
    {
        $envelope = SnsBridge::toEnvelope(
            subject: 'OrderCreatedEvent',
            snsMessage: ['tenant' => 'jabourih', 'data' => ['order_id' => 'ord_1']],
            sourceService: 'service-order',
            snsMessageId: 'aws-uuid-1',
            snsTimestamp: '2026-08-04T20:00:00.000Z',
        );

        $this->assertSame('OrderCreatedEvent', $envelope->eventType);
        $this->assertSame('aws-uuid-1', $envelope->messageId);
        $this->assertSame('jabourih', $envelope->tenant);
    }

    public function testSnsBridgeRejectsUnnormalizedPublishers(): void
    {
        $this->expectExceptionMessageMatches('/Phase 0\.5/');
        SnsBridge::toEnvelope('CustomerDataUpdated', ['id' => 1, 'token' => 'x'], 'service-user');
    }

    public function testSnsBridgeRejectsTenantWithoutDataObject(): void
    {
        $this->expectExceptionMessageMatches('/no data object/');
        SnsBridge::toEnvelope('OrderCreatedEvent', ['tenant' => 'jabourih'], 'service-order');
    }

    public function testHostileRetryRoundHeaderCannotCrashTheProcessor(): void
    {
        // x-retry-round is wire input (hand-edited DLQ replays, foreign
        // producers). A negative/garbage value used to fatal on readonly
        // RetryPolicy state and block the partition — now it clamps.
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor(
            'svc-payment',
            escalationProducer: $escalation,
            retryPolicy: new RetryPolicy(immediateAttempts: 1),
        );
        $processor->onEvent('OrderCreatedEvent', fn () => throw new \RuntimeException('boom'));

        $outcome = $processor->process($this->message(extraHeaders: [MessageProcessor::HEADER_RETRY_ROUND => '-5']));

        $this->assertSame(ProcessOutcome::Retried, $outcome, 'hostile header restarts the ladder instead of crashing');
        $retried = $escalation->messagesOn('svc-payment.order.events.retry');
        $this->assertSame('1', $retried[0]['headers'][MessageProcessor::HEADER_RETRY_ROUND]);
        $this->assertSame('5', $retried[0]['headers'][MessageProcessor::HEADER_RETRY_DELAY]);
    }

    public function testScalarJsonPayloadIsMalformedNotWarning(): void
    {
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor('svc-payment', escalationProducer: $escalation);

        $outcome = $processor->process(new ConsumedMessage(
            topic: 'order.events',
            key: 'jabourih',
            headers: ['event_type' => 'OrderCreatedEvent', 'tenant' => 'jabourih'],
            payload: '"5"',
        ));

        $this->assertSame(ProcessOutcome::Malformed, $outcome);
    }

    public function testSafePublishForwardsMessageIdAndEventTime(): void
    {
        $driver = new InMemoryProducerDriver();
        $producer = new KafkaProducer($driver, 'service-order');

        $envelope = $producer->safePublish(
            'order.events',
            'OrderCreatedEvent',
            'jabourih',
            [],
            messageId: 'bridge-id-9',
            eventTime: '2026-08-04T21:00:00.000Z',
        );

        $this->assertSame('bridge-id-9', $envelope?->messageId);
        $this->assertSame('bridge-id-9', $driver->messagesOn('order.events')[0]['headers']['message_id']);
        $this->assertSame('2026-08-04T21:00:00.000Z', $driver->messagesOn('order.events')[0]['headers']['event_time']);
    }

    public function testDaemonModeSurvivesIdlePollsAndStopsGracefully(): void
    {
        $consumerHolder = new \stdClass();

        // A driver that is idle first, then yields one message, then idles forever.
        $driver = new class($this->message()) implements ConsumerDriverInterface {
            private int $polls = 0;

            /** @var list<ConsumedMessage> */
            public array $committed = [];

            public function __construct(private readonly ConsumedMessage $message)
            {
            }

            public function subscribe(string $group, array $topics): void
            {
            }

            public function poll(int $timeoutMs): ?ConsumedMessage
            {
                $this->polls++;

                return $this->polls === 3 ? $this->message : null;
            }

            public function commit(ConsumedMessage $message): void
            {
                $this->committed[] = $message;
            }
        };

        $handled = 0;
        $consumer = KafkaConsumer::group('svc-payment')
            ->driver($driver)
            ->subscribe(['order.events'])
            ->onEvent('OrderCreatedEvent', function () use (&$handled, $consumerHolder): void {
                $handled++;
                $consumerHolder->consumer->stop(); // simulate SIGTERM after the first message
            });
        $consumerHolder->consumer = $consumer;

        $counts = $consumer->run(pollTimeoutMs: 1, exitOnIdle: false);

        $this->assertSame(1, $handled, 'daemon kept polling past idle intervals');
        $this->assertSame(['processed' => 1], $counts);
        $this->assertCount(1, $driver->committed);
    }
}
