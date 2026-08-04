<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\Drivers\InMemoryProducerDriver;
use Order\KafkaMessaging\Exceptions\InvalidEnvelopeException;
use Order\KafkaMessaging\Exceptions\PublishFailedException;
use Order\KafkaMessaging\KafkaProducer;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

final class KafkaProducerTest extends TestCase
{
    public function testPublishSendsTenantKeyedMessageWithFullEnvelope(): void
    {
        $driver = new InMemoryProducerDriver();
        $producer = new KafkaProducer($driver, 'service-order');

        $envelope = $producer->publish('order.events', 'OrderCreatedEvent', 'jabourih', ['order_id' => 'ord_1']);

        $messages = $driver->messagesOn('order.events');
        $this->assertCount(1, $messages);
        $this->assertSame('jabourih', $messages[0]['key']);
        $this->assertSame('OrderCreatedEvent', $messages[0]['headers']['event_type']);
        $this->assertSame($envelope->messageId, $messages[0]['headers']['message_id']);
        $this->assertSame('{"tenant":"jabourih","data":{"order_id":"ord_1"}}', $messages[0]['payload']);
    }

    public function testCompositeKeyKeepsTenantPrefix(): void
    {
        $driver = new InMemoryProducerDriver();
        $producer = new KafkaProducer($driver, 'service-order');

        $producer->publish('order.events', 'OrderCreatedEvent', 'jabourih', [], key: 'jabourih:ord_9931');

        $this->assertSame('jabourih:ord_9931', $driver->messagesOn('order.events')[0]['key']);
    }

    public function testKeyWithoutTenantPrefixIsRejected(): void
    {
        $producer = new KafkaProducer(new InMemoryProducerDriver(), 'service-order');

        $this->expectException(PublishFailedException::class);
        $producer->publish('order.events', 'OrderCreatedEvent', 'jabourih', [], key: 'ord_9931');
    }

    public function testPublishWithoutTenantThrows(): void
    {
        $producer = new KafkaProducer(new InMemoryProducerDriver(), 'service-user');

        $this->expectException(InvalidEnvelopeException::class);
        $producer->publish('user.events', 'CustomerDataUpdated', '', ['id' => 1]);
    }

    public function testSafePublishNeverThrowsAndLogsWithDataKey(): void
    {
        $driver = new InMemoryProducerDriver();
        $driver->failNext = true;

        $logged = [];
        $logger = new class($logged) extends AbstractLogger {
            /** @param array<int, array{level: mixed, message: string, context: array}> $sink */
            public function __construct(private array &$sink)
            {
            }

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->sink[] = ['level' => $level, 'message' => (string) $message, 'context' => $context];
            }
        };

        $producer = new KafkaProducer($driver, 'service-order', $logger);
        $result = $producer->safePublish('order.events', 'OrderCreatedEvent', 'jabourih', []);

        $this->assertNull($result);
        $this->assertCount(1, $logged);
        // Platform GELF rule: context MUST be wrapped in a 'data' key.
        $this->assertArrayHasKey('data', $logged[0]['context']);
        $this->assertSame('order.events', $logged[0]['context']['data']['topic']);
    }

    public function testSafePublishReturnsEnvelopeOnSuccess(): void
    {
        $producer = new KafkaProducer(new InMemoryProducerDriver(), 'service-order');

        $envelope = $producer->safePublish('order.events', 'OrderCreatedEvent', 'jabourih', []);

        $this->assertNotNull($envelope);
        $this->assertSame('jabourih', $envelope->tenant);
    }
}
