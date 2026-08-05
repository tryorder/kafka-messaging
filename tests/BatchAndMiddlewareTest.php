<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use Order\KafkaMessaging\Drivers\InMemoryConsumerDriver;
use Order\KafkaMessaging\Drivers\InMemoryProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\KafkaConsumer;
use Order\KafkaMessaging\KafkaProducer;
use Order\KafkaMessaging\MessageProcessor;
use Order\KafkaMessaging\ProcessOutcome;
use PHPUnit\Framework\TestCase;

final class BatchAndMiddlewareTest extends TestCase
{
    // ---- batch producing ----------------------------------------------

    public function testBatchPublishesEveryMessageWithItsOwnEnvelope(): void
    {
        $driver = new InMemoryProducerDriver();
        $producer = new KafkaProducer($driver, 'service-order');

        $result = $producer->publishBatch('order.events', [
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => ['n' => 1], 'key' => 'acme:ord_1'],
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => ['n' => 2]],
            ['eventType' => 'OrderCompletedEvent', 'tenant' => 'other', 'data' => ['n' => 3]],
        ]);

        $this->assertTrue($result->allDelivered());
        $this->assertSame(3, $result->count());

        $sent = $driver->messagesOn('order.events');
        $this->assertCount(3, $sent);
        $this->assertSame('acme:ord_1', $sent[0]['key']);
        $this->assertSame('acme', $sent[1]['key'], 'key defaults to the tenant');
        $this->assertSame('OrderCompletedEvent', $sent[2]['headers']['event_type']);

        // Each message gets its own message_id.
        $ids = array_column(array_column($sent, 'headers'), 'message_id');
        $this->assertCount(3, array_unique($ids));
    }

    public function testInvalidMessageInBatchDoesNotDiscardTheRest(): void
    {
        $driver = new InMemoryProducerDriver();
        $producer = new KafkaProducer($driver, 'service-order');

        $result = $producer->publishBatch('order.events', [
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => ['ok' => 1]],
            ['eventType' => 'OrderCreatedEvent', 'tenant' => '', 'data' => ['bad' => 1]],           // no tenant
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => [], 'key' => 'nope'], // bad key
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => ['ok' => 2]],
        ]);

        $this->assertSame(2, $result->count());
        $this->assertSame([1, 2], array_column($result->failed(), 'index'), 'failures keep their INPUT index');
        $this->assertCount(2, $driver->messagesOn('order.events'));
    }

    public function testDriverLevelFailuresMapBackToInputIndexes(): void
    {
        $driver = new InMemoryProducerDriver();
        // Message at input index 2 is the driver's index 1 (index 0 is dropped
        // earlier by envelope validation) — the mapping must survive that.
        $driver->failBatchIndexes = [1];
        $producer = new KafkaProducer($driver, 'service-order');

        $result = $producer->publishBatch('order.events', [
            ['eventType' => 'X', 'tenant' => '', 'data' => []],                    // invalid → index 0
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => []], // driver index 0
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => []], // driver index 1 → fails
        ]);

        $this->assertSame([0, 2], array_column($result->failed(), 'index'));
        $this->assertSame(1, $result->count());
    }

    public function testBatchFallsBackToSequentialSendsForPlainDrivers(): void
    {
        // A driver that only implements the base contract must still work.
        $plain = new class implements ProducerDriverInterface {
            /** @var list<array{topic: string, key: string}> */
            public array $sent = [];

            public function send(string $topic, string $key, array $headers, string $payload): void
            {
                if ($key === 'boom') {
                    throw new \RuntimeException('driver refused');
                }
                $this->sent[] = ['topic' => $topic, 'key' => $key];
            }
        };

        $result = (new KafkaProducer($plain, 'service-order'))->publishBatch('order.events', [
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => []],
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'boom', 'data' => []],
            ['eventType' => 'OrderCreatedEvent', 'tenant' => 'acme', 'data' => []],
        ]);

        $this->assertSame(2, $result->count());
        $this->assertSame([1], array_column($result->failed(), 'index'));
        $this->assertCount(2, $plain->sent);
    }

    public function testEmptyBatchIsANoop(): void
    {
        $driver = new InMemoryProducerDriver();
        $result = (new KafkaProducer($driver, 'service-order'))->publishBatch('order.events', []);

        $this->assertSame(0, $result->count());
        $this->assertTrue($result->allDelivered());
        $this->assertSame([], $driver->topics);
    }

    // ---- consumer middleware -------------------------------------------

    private function message(string $eventType = 'OrderCreatedEvent'): ConsumedMessage
    {
        $e = Envelope::create('acme', ['n' => 1], $eventType, 'service-order');

        return new ConsumedMessage('order.events', 'acme', $e->toHeaders(), $e->toPayload());
    }

    public function testMiddlewareWrapsTheHandlerInRegistrationOrder(): void
    {
        $trace = [];
        $processor = new MessageProcessor('svc-payment');
        $processor
            ->middleware(function (Envelope $e, \Closure $next) use (&$trace) {
                $trace[] = 'outer:before';
                $next($e);
                $trace[] = 'outer:after';
            })
            ->middleware(function (Envelope $e, \Closure $next) use (&$trace) {
                $trace[] = 'inner:before';
                $next($e);
                $trace[] = 'inner:after';
            })
            ->onEvent('OrderCreatedEvent', function () use (&$trace): void {
                $trace[] = 'handler';
            });

        $this->assertSame(ProcessOutcome::Processed, $processor->process($this->message()));
        $this->assertSame(
            ['outer:before', 'inner:before', 'handler', 'inner:after', 'outer:after'],
            $trace
        );
    }

    public function testMiddlewareCanSkipTheHandler(): void
    {
        $ran = 0;
        $processor = new MessageProcessor('svc-payment');
        $processor
            ->middleware(fn (Envelope $e, \Closure $next) => $e->tenant === 'blocked' ? null : $next($e))
            ->onEvent('OrderCreatedEvent', function () use (&$ran): void {
                $ran++;
            });

        $blocked = Envelope::create('blocked', [], 'OrderCreatedEvent', 'service-order');
        $processor->process(new ConsumedMessage('order.events', 'blocked', $blocked->toHeaders(), $blocked->toPayload()));
        $this->assertSame(0, $ran, 'skipped by middleware');

        $processor->process($this->message());
        $this->assertSame(1, $ran, 'allowed through');
    }

    public function testMiddlewareThrowFlowsIntoTheRetryPath(): void
    {
        $escalation = new InMemoryProducerDriver();
        $processor = new MessageProcessor(
            'svc-payment',
            escalationProducer: $escalation,
            retryPolicy: new \Order\KafkaMessaging\RetryPolicy(immediateAttempts: 1),
        );
        $processor
            ->middleware(fn (Envelope $e, \Closure $next) => throw new \RuntimeException('middleware exploded'))
            ->onEvent('OrderCreatedEvent', fn () => null);

        $this->assertSame(ProcessOutcome::Retried, $processor->process($this->message()));
        $this->assertSame(
            'middleware exploded',
            $escalation->messagesOn('svc-payment.order.events.retry')[0]['headers'][MessageProcessor::HEADER_LAST_ERROR]
        );
    }

    public function testConsumerBuilderForwardsMiddleware(): void
    {
        $driver = new InMemoryConsumerDriver();
        $driver->feed($this->message());

        $seen = [];
        KafkaConsumer::group('svc-payment')
            ->driver($driver)
            ->subscribe(['order.events'])
            ->middleware(function (Envelope $e, \Closure $next) use (&$seen) {
                $seen[] = 'mw:' . $e->tenant;

                return $next($e);
            })
            ->onEvent('OrderCreatedEvent', function () use (&$seen): void {
                $seen[] = 'handler';
            })
            ->run();

        $this->assertSame(['mw:acme', 'handler'], $seen);
    }
}
