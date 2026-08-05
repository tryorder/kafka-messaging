<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\BrokerConfig;
use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Contracts\ConsumerMetricsInterface;
use Order\KafkaMessaging\Drivers\InMemoryConsumerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\KafkaConsumer;
use Order\KafkaMessaging\KafkaProducer;
use Order\KafkaMessaging\ProcessOutcome;
use Order\KafkaMessaging\Testing\KafkaFake;
use PHPUnit\Framework\TestCase;

/**
 * Covers the features adopted from the laravel-kafka feature set:
 * SASL/SSL config, testing fake + assertions, metrics/callback hooks.
 */
final class FeatureParityTest extends TestCase
{
    // ---- BrokerConfig (SASL/SSL) -------------------------------------

    public function testPlaintextEmitsOnlyBrokers(): void
    {
        $conf = (new BrokerConfig('127.0.0.1:9092'))->toLibrdKafkaConf();

        $this->assertSame(['bootstrap.servers' => '127.0.0.1:9092'], $conf);
    }

    public function testSaslSslProducesTheExpectedLibrdkafkaKeys(): void
    {
        $conf = (new BrokerConfig(
            brokers: 'b-1.msk:9096',
            securityProtocol: BrokerConfig::PROTOCOL_SASL_SSL,
            saslMechanism: 'SCRAM-SHA-512',
            saslUsername: 'order',
            saslPassword: 's3cret',
            sslCaLocation: '/etc/ssl/certs/ca.pem',
        ))->toLibrdKafkaConf();

        $this->assertSame('SASL_SSL', $conf['security.protocol']);
        $this->assertSame('SCRAM-SHA-512', $conf['sasl.mechanisms']);
        $this->assertSame('order', $conf['sasl.username']);
        $this->assertSame('s3cret', $conf['sasl.password']);
        $this->assertSame('/etc/ssl/certs/ca.pem', $conf['ssl.ca.location']);
    }

    public function testSaslWithoutCredentialsFailsAtConstructionNotInProduction(): void
    {
        $this->expectExceptionMessageMatches('/requires a username and password/');

        new BrokerConfig('b-1.msk:9096', BrokerConfig::PROTOCOL_SASL_SSL, 'SCRAM-SHA-512');
    }

    public function testUnknownProtocolRejected(): void
    {
        $this->expectExceptionMessageMatches('/Unknown security\.protocol/');
        new BrokerConfig('b:9092', 'TOTALLY_SECURE');
    }

    public function testRedactionHidesSecrets(): void
    {
        $redacted = (new BrokerConfig(
            brokers: 'b:9096',
            securityProtocol: BrokerConfig::PROTOCOL_SASL_SSL,
            saslMechanism: 'PLAIN',
            saslUsername: 'u',
            saslPassword: 'p',
        ))->toRedactedArray();

        $this->assertSame('***', $redacted['sasl.password']);
        $this->assertSame('u', $redacted['sasl.username']);
    }

    public function testFromArrayReadsServiceConfigShape(): void
    {
        $broker = BrokerConfig::fromArray([
            'brokers' => 'b:9096',
            'security' => ['protocol' => 'SASL_SSL', 'sasl_mechanism' => 'PLAIN', 'sasl_username' => 'u', 'sasl_password' => 'p'],
            'extra_conf' => ['socket.keepalive.enable' => 'true'],
        ]);

        $this->assertTrue($broker->usesSasl());
        $this->assertSame('true', $broker->toLibrdKafkaConf()['socket.keepalive.enable']);
    }

    public function testEmptyEnvStringsAreTreatedAsUnset(): void
    {
        // Laravel returns '' for a declared-but-empty env key; that must not
        // be mistaken for a real credential.
        $broker = BrokerConfig::fromArray(['brokers' => 'b:9092', 'security' => ['protocol' => 'PLAINTEXT', 'sasl_username' => '']]);

        $this->assertNull($broker->saslUsername);
    }

    // ---- KafkaFake (testing utilities) --------------------------------

    public function testFakeRecordsAndAssertsPublishes(): void
    {
        $fake = new KafkaFake();
        $producer = new KafkaProducer($fake, 'service-order');

        $producer->publish('order.events', 'OrderCreatedEvent', 'jabourih', ['order_id' => 'ord_1']);
        $producer->publish('order.events', 'OrderCreatedEvent', 'other', ['order_id' => 'ord_2']);

        $fake->assertPublished('OrderCreatedEvent')
            ->assertPublishedOn('order.events', 'OrderCreatedEvent', fn (array $m) => $m['tenant'] === 'jabourih')
            ->assertPublishedTimes('OrderCreatedEvent', 2)
            ->assertNotPublished('OrderCompletedEvent')
            ->assertAllHaveTenant();

        $this->assertSame('ord_1', $fake->published('OrderCreatedEvent')[0]['data']['order_id']);
    }

    public function testFakeAssertNothingPublished(): void
    {
        (new KafkaFake())->assertNothingPublished();
    }

    public function testFakeFailsLoudlyWhenExpectedEventMissing(): void
    {
        $fake = new KafkaFake();
        (new KafkaProducer($fake, 'service-order'))->publish('order.events', 'OrderCreatedEvent', 'jabourih', []);

        // Caught explicitly: PHPUnit treats a failed assertion as a test
        // failure, not as an exception expectException() can intercept.
        try {
            $fake->assertPublished('OrderCompletedEvent');
            $this->fail('assertPublished should have failed.');
        } catch (\PHPUnit\Framework\ExpectationFailedException $e) {
            $this->assertStringContainsString('OrderCompletedEvent', $e->getMessage());
            $this->assertStringContainsString('Published: OrderCreatedEvent@order.events', $e->getMessage());
        }
    }

    // ---- metrics + callbacks ------------------------------------------

    public function testMetricsAndCallbacksFireAroundEachMessage(): void
    {
        $envelope = Envelope::create('jabourih', [], 'OrderCreatedEvent', 'service-order');
        $driver = new InMemoryConsumerDriver();
        $driver->feed(new ConsumedMessage('order.events', 'jabourih', $envelope->toHeaders(), $envelope->toPayload()));

        $metrics = new class implements ConsumerMetricsInterface {
            public array $before = [];
            public array $after = [];

            public function beforeProcess(string $consumerGroup, ConsumedMessage $message): void
            {
                $this->before[] = $consumerGroup;
            }

            public function afterProcess(string $consumerGroup, ConsumedMessage $message, ProcessOutcome $outcome, float $durationMs): void
            {
                $this->after[] = [$outcome, $durationMs];
            }
        };

        $seen = [];
        KafkaConsumer::group('svc-payment')
            ->driver($driver)
            ->subscribe(['order.events'])
            ->onEvent('OrderCreatedEvent', fn () => null)
            ->withMetrics($metrics)
            ->before(function (ConsumedMessage $m) use (&$seen): void {
                $seen[] = 'before:' . $m->topic;
            })
            ->after(function (ConsumedMessage $m, ProcessOutcome $o) use (&$seen): void {
                $seen[] = 'after:' . $o->value;
            })
            ->run();

        $this->assertSame(['svc-payment'], $metrics->before);
        $this->assertSame(ProcessOutcome::Processed, $metrics->after[0][0]);
        $this->assertGreaterThanOrEqual(0.0, $metrics->after[0][1]);
        $this->assertSame(['before:order.events', 'after:processed'], $seen);
    }

    public function testAFailingMetricsBackendNeverStopsConsumption(): void
    {
        $envelope = Envelope::create('jabourih', [], 'OrderCreatedEvent', 'service-order');
        $driver = new InMemoryConsumerDriver();
        $driver->feed(new ConsumedMessage('order.events', 'jabourih', $envelope->toHeaders(), $envelope->toPayload()));

        $handled = 0;
        $counts = KafkaConsumer::group('svc-payment')
            ->driver($driver)
            ->subscribe(['order.events'])
            ->onEvent('OrderCreatedEvent', function () use (&$handled): void {
                $handled++;
            })
            ->before(fn () => throw new \RuntimeException('metrics backend down'))
            ->after(fn () => throw new \RuntimeException('still down'))
            ->run();

        $this->assertSame(1, $handled);
        $this->assertSame(['processed' => 1], $counts);
    }
}
