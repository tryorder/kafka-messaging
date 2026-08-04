<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\Drivers\LogProducerDriver;
use Order\KafkaMessaging\KafkaProducer;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

final class LogProducerDriverTest extends TestCase
{
    public function testShadowPublishLogsWireMessageWithDataWrappedContext(): void
    {
        $logged = [];
        $logger = new class($logged) extends AbstractLogger {
            public function __construct(private array &$sink)
            {
            }

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->sink[] = ['level' => $level, 'message' => (string) $message, 'context' => $context];
            }
        };

        $producer = new KafkaProducer(new LogProducerDriver($logger), 'service-order');
        $producer->publish('order.events', 'OrderCreatedEvent', 'jabourih', ['order_id' => 'ord_1']);

        $this->assertCount(1, $logged);
        $this->assertSame('Kafka shadow publish', $logged[0]['message']);
        // Platform GELF rule: context wrapped in 'data'.
        $this->assertArrayHasKey('data', $logged[0]['context']);
        $this->assertSame('order.events', $logged[0]['context']['data']['topic']);
        $this->assertSame('jabourih', $logged[0]['context']['data']['key']);
        $this->assertSame('OrderCreatedEvent', $logged[0]['context']['data']['headers']['event_type']);
    }
}
