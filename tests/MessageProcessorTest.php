<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Drivers\InMemoryProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\InMemoryIdempotencyStore;
use Order\KafkaMessaging\MessageProcessor;
use Order\KafkaMessaging\ProcessOutcome;
use Order\KafkaMessaging\RetryPolicy;
use PHPUnit\Framework\TestCase;

final class MessageProcessorTest extends TestCase
{
    private function message(?Envelope $envelope = null, array $extraHeaders = [], string $topic = 'order.events'): ConsumedMessage
    {
        $envelope ??= Envelope::create('jabourih', ['order_id' => 'ord_1'], 'OrderCreatedEvent', 'service-order');

        return new ConsumedMessage(
            topic: $topic,
            key: $envelope->tenant,
            headers: array_merge($envelope->toHeaders(), $extraHeaders),
            payload: $envelope->toPayload(),
        );
    }

    public function testProcessesAndMarksIdempotency(): void
    {
        $store = new InMemoryIdempotencyStore();
        $seen = [];

        $processor = new MessageProcessor('svc-payment', idempotency: $store);
        $processor->onEvent('OrderCreatedEvent', function (Envelope $e) use (&$seen): void {
            $seen[] = $e->data['order_id'];
        });

        $msg = $this->message();
        $this->assertSame(ProcessOutcome::Processed, $processor->process($msg));
        $this->assertSame(['ord_1'], $seen);

        // Same message again → duplicate, handler NOT re-invoked.
        $this->assertSame(ProcessOutcome::Duplicate, $processor->process($msg));
        $this->assertSame(['ord_1'], $seen);
    }

    public function testUnknownEventTypeIsSkippedNotFailed(): void
    {
        $processor = new MessageProcessor('svc-payment');
        $processor->onEvent('RefundRequestedEvent', fn () => throw new \LogicException('must not run'));

        $this->assertSame(ProcessOutcome::NoHandler, $processor->process($this->message()));
    }

    public function testFallbackHandlerReceivesUnclaimedEventTypes(): void
    {
        $seen = [];
        $processor = new MessageProcessor('svc-webhook');
        $processor->onAnyEvent(function (Envelope $e) use (&$seen): void {
            $seen[] = $e->eventType;
        });

        $this->assertSame(ProcessOutcome::Processed, $processor->process($this->message()));
        $this->assertSame(['OrderCreatedEvent'], $seen);

        // A type the consumer has never heard of still reaches the fallback —
        // that is the whole point for catalog-driven consumers.
        $unknown = Envelope::create('jabourih', ['x' => 1], 'SomeBrandNewEvent', 'service-order');
        $this->assertSame(ProcessOutcome::Processed, $processor->process($this->message($unknown)));
        $this->assertSame(['OrderCreatedEvent', 'SomeBrandNewEvent'], $seen);
    }

    public function testExactHandlerWinsOverFallback(): void
    {
        $calls = [];
        $processor = new MessageProcessor('svc-webhook');
        $processor->onEvent('OrderCreatedEvent', function () use (&$calls): void {
            $calls[] = 'exact';
        });
        $processor->onAnyEvent(function () use (&$calls): void {
            $calls[] = 'fallback';
        });

        $processor->process($this->message());
        $this->assertSame(['exact'], $calls);

        $other = Envelope::create('jabourih', ['x' => 1], 'OrderCompletedEvent', 'service-order');
        $processor->process($this->message($other));
        $this->assertSame(['exact', 'fallback'], $calls);
    }

    public function testHandlerSeesTheTopicTheMessageCameFrom(): void
    {
        // Cutover is per topic, so a catch-all handler must be able to tell
        // which topic it is looking at.
        $seen = [];
        $processor = new MessageProcessor('svc-webhook');
        $processor->onAnyEvent(function (Envelope $e) use (&$seen): void {
            $seen[] = $e->sourceTopic;
        });

        $coupon = Envelope::create('jabourih', ['id' => 'cpn_1'], 'CouponCreatedEvent', 'service-promocodes');
        $order = Envelope::create('jabourih', ['id' => 'ord_1'], 'OrderCreatedEvent', 'service-order');

        $processor->process($this->message($coupon, topic: 'promocode.events'));
        $processor->process($this->message($order, topic: 'order.events'));

        $this->assertSame(['promocode.events', 'order.events'], $seen);
    }

    public function testRetriedMessageKeepsItsOriginalTopic(): void
    {
        // Read back off the retry topic, a message must still be judged as the
        // topic it was published to — otherwise a retry would bypass a
        // per-topic cutover gate.
        $seen = null;
        $processor = new MessageProcessor('svc-webhook');
        $processor->onAnyEvent(function (Envelope $e) use (&$seen): void {
            $seen = $e->sourceTopic;
        });

        $processor->process($this->message(
            extraHeaders: [MessageProcessor::HEADER_ORIGINAL_TOPIC => 'promocode.events'],
            topic: 'svc-webhook.promocode.events.retry'
        ));

        $this->assertSame('promocode.events', $seen);
    }

    public function testFallbackIsNotRegisteredByDefault(): void
    {
        // Guard for the 13 services already running: without onAnyEvent(), an
        // unclaimed event type must still be skipped, never swallowed.
        $processor = new MessageProcessor('svc-payment');
        $processor->onEvent('RefundRequestedEvent', fn () => throw new \LogicException('must not run'));

        $this->assertSame(ProcessOutcome::NoHandler, $processor->process($this->message()));
    }

    public function testImmediateRetriesBeforeEscalation(): void
    {
        $attempts = 0;
        $processor = new MessageProcessor(
            'svc-payment',
            escalationProducer: new InMemoryProducerDriver(),
            retryPolicy: new RetryPolicy(immediateAttempts: 3),
        );
        $processor->onEvent('OrderCreatedEvent', function () use (&$attempts): void {
            $attempts++;
            if ($attempts < 3) {
                throw new \RuntimeException('transient');
            }
        });

        $this->assertSame(ProcessOutcome::Processed, $processor->process($this->message()));
        $this->assertSame(3, $attempts, 'third immediate attempt succeeded — no escalation');
    }

    public function testExhaustedImmediateAttemptsEscalateToRetryTopicWithRoundHeaders(): void
    {
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor(
            'svc-payment',
            escalationProducer: $escalation,
            retryPolicy: new RetryPolicy(immediateAttempts: 2, roundDelaysSeconds: [5, 60, 600]),
        );
        $processor->onEvent('OrderCreatedEvent', fn () => throw new \RuntimeException('boom'));

        $outcome = $processor->process($this->message());

        $this->assertSame(ProcessOutcome::Retried, $outcome);
        $retried = $escalation->messagesOn('svc-payment.order.events.retry');
        $this->assertCount(1, $retried);
        $this->assertSame('1', $retried[0]['headers'][MessageProcessor::HEADER_RETRY_ROUND]);
        $this->assertSame('5', $retried[0]['headers'][MessageProcessor::HEADER_RETRY_DELAY]);
        $this->assertSame('order.events', $retried[0]['headers'][MessageProcessor::HEADER_ORIGINAL_TOPIC]);
        $this->assertSame('boom', $retried[0]['headers'][MessageProcessor::HEADER_LAST_ERROR]);
    }

    public function testRetryRoundsEscalateAndFinallyDeadLetter(): void
    {
        $escalation = new InMemoryProducerDriver();
        $policy = new RetryPolicy(immediateAttempts: 1, roundDelaysSeconds: [5, 60]);
        $processor = new MessageProcessor('svc-payment', escalationProducer: $escalation, retryPolicy: $policy);
        $processor->onEvent('OrderCreatedEvent', fn () => throw new \RuntimeException('always fails'));

        // Round 0 (from main topic) → retry round 1
        $this->assertSame(ProcessOutcome::Retried, $processor->process($this->message()));

        // Simulate consuming from the retry topic at round 1 → retry round 2
        $retryMsg1 = new ConsumedMessage(
            topic: 'svc-payment.order.events.retry',
            key: 'jabourih',
            headers: $escalation->messagesOn('svc-payment.order.events.retry')[0]['headers'],
            payload: $escalation->messagesOn('svc-payment.order.events.retry')[0]['payload'],
        );
        $this->assertSame(ProcessOutcome::Retried, $processor->process($retryMsg1));
        $this->assertCount(2, $escalation->messagesOn('svc-payment.order.events.retry'));
        $this->assertSame('2', $escalation->messagesOn('svc-payment.order.events.retry')[1]['headers'][MessageProcessor::HEADER_RETRY_ROUND]);

        // Round 2 exhausted (maxRounds=2) → DLQ, addressed by ORIGINAL topic.
        $retryMsg2 = new ConsumedMessage(
            topic: 'svc-payment.order.events.retry',
            key: 'jabourih',
            headers: $escalation->messagesOn('svc-payment.order.events.retry')[1]['headers'],
            payload: $escalation->messagesOn('svc-payment.order.events.retry')[1]['payload'],
        );
        $this->assertSame(ProcessOutcome::DeadLettered, $processor->process($retryMsg2));
        $this->assertCount(1, $escalation->messagesOn('svc-payment.order.events.dlq'));
    }

    public function testMalformedMessageGoesStraightToDlq(): void
    {
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor('svc-payment', escalationProducer: $escalation);

        $outcome = $processor->process(new ConsumedMessage(
            topic: 'order.events',
            key: null,
            headers: [],
            payload: '{broken',
        ));

        $this->assertSame(ProcessOutcome::Malformed, $outcome);
        $this->assertCount(1, $escalation->messagesOn('svc-payment.order.events.dlq'));
    }

    public function testLegacyMessageWithoutMessageIdIsProcessedNotDeduped(): void
    {
        $store = new InMemoryIdempotencyStore();
        $runs = 0;

        $processor = new MessageProcessor('svc-payment', idempotency: $store);
        $processor->onEvent('OrderCreatedEvent', function () use (&$runs): void {
            $runs++;
        });

        // SNS-era shape: tenant in payload only, no message_id header.
        $legacy = new ConsumedMessage(
            topic: 'order.events',
            key: null,
            headers: ['event_type' => 'OrderCreatedEvent'],
            payload: '{"tenant":"jabourih","data":{}}',
        );

        $this->assertSame(ProcessOutcome::Processed, $processor->process($legacy));
        $this->assertSame(ProcessOutcome::Processed, $processor->process($legacy), 'no id → cannot dedupe → runs again');
        $this->assertSame(2, $runs);
    }
}
