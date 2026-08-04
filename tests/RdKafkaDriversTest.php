<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\Drivers\RdKafkaConsumerDriver;
use Order\KafkaMessaging\Drivers\RdKafkaProducerDriver;
use PHPUnit\Framework\TestCase;

final class RdKafkaDriversTest extends TestCase
{
    public function testProducerDriverFailsLoudlyWithoutExtension(): void
    {
        if (extension_loaded('rdkafka')) {
            $this->markTestSkipped('rdkafka present — guard path not reachable');
        }

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/pecl install rdkafka/');
        new RdKafkaProducerDriver('localhost:9092');
    }

    public function testConsumerDriverFailsLoudlyWithoutExtension(): void
    {
        if (extension_loaded('rdkafka')) {
            $this->markTestSkipped('rdkafka present — guard path not reachable');
        }

        $this->expectException(\RuntimeException::class);
        new RdKafkaConsumerDriver('localhost:9092');
    }
}
