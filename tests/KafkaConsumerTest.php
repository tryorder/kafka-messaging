<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Drivers\InMemoryConsumerDriver;
use Order\KafkaMessaging\Drivers\InMemoryProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\InMemoryIdempotencyStore;
use Order\KafkaMessaging\KafkaConsumer;
use Order\KafkaMessaging\KafkaProducer;
use PHPUnit\Framework\TestCase;

final class KafkaConsumerTest extends TestCase
{
    public function testEndToEndProduceConsumeRoute(): void
    {
        // Produce through the real producer into the in-memory "broker"...
        $broker = new InMemoryProducerDriver();
        $producer = new KafkaProducer($broker, 'service-order');
        $producer->publish('order.events', 'OrderCreatedEvent', 'jabourih', ['order_id' => 'ord_1'], key: 'jabourih:ord_1');
        $producer->publish('order.events', 'OrderCompletedEvent', 'jabourih', ['order_id' => 'ord_1'], key: 'jabourih:ord_1');

        // ...then feed those exact wire messages to a consumer.
        $consumerDriver = new InMemoryConsumerDriver();
        foreach ($broker->messagesOn('order.events') as $m) {
            $consumerDriver->feed(new ConsumedMessage('order.events', $m['key'], $m['headers'], $m['payload']));
        }

        $created = [];
        $counts = KafkaConsumer::group('svc-payment')
            ->driver($consumerDriver)
            ->subscribe(['order.events'])
            ->onEvent('OrderCreatedEvent', function (Envelope $e) use (&$created): void {
                $created[] = $e->data['order_id'];
            })
            ->withIdempotency(new InMemoryIdempotencyStore())
            ->withRetryAndDlq(new InMemoryProducerDriver())
            ->run();

        $this->assertSame(['ord_1'], $created);
        $this->assertSame(['processed' => 1, 'no_handler' => 1], $counts);
        $this->assertSame('svc-payment', $consumerDriver->group);
        $this->assertCount(2, $consumerDriver->committed, 'both messages committed');
    }

    public function testRunRequiresDriverAndTopics(): void
    {
        $this->expectException(\LogicException::class);
        KafkaConsumer::group('svc-payment')->run();
    }
}
