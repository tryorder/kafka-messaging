<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Laravel;

use Illuminate\Support\ServiceProvider;
use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;
use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use Order\KafkaMessaging\Drivers\InMemoryProducerDriver;
use Order\KafkaMessaging\Drivers\LogProducerDriver;
use Order\KafkaMessaging\InMemoryIdempotencyStore;
use Order\KafkaMessaging\KafkaProducer;
use Order\KafkaMessaging\Drivers\RdKafkaProducerDriver;
use Order\KafkaMessaging\PdoIdempotencyStore;

/**
 * Laravel bridge — auto-discovered via composer extra.laravel.providers.
 * The library core stays framework-free; only this class touches Laravel.
 *
 * Per-service setup =
 *   1. composer require order/kafka-messaging
 *   2. .env: KAFKA_SOURCE_SERVICE=service-x (+ KAFKA_DUAL_WRITE when ready)
 *   3. migration for processed_messages (see README)
 *   4. inject KafkaProducer and call safePublish() next to the SNS publish.
 */
final class KafkaMessagingServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/config/kafka-messaging.php', 'kafka-messaging');

        $this->app->singleton(ProducerDriverInterface::class, function ($app) {
            return match ($app['config']['kafka-messaging.driver']) {
                'memory' => new InMemoryProducerDriver(),
                'rdkafka' => new RdKafkaProducerDriver(
                    (string) $app['config']['kafka-messaging.brokers'],
                ),
                default => new LogProducerDriver($app['log']),
            };
        });

        $this->app->singleton(KafkaProducer::class, function ($app) {
            $source = (string) $app['config']['kafka-messaging.source_service'];
            if ($source === '') {
                throw new \RuntimeException(
                    'KAFKA_SOURCE_SERVICE is not set — every service must declare its identity.'
                );
            }

            return new KafkaProducer(
                driver: $app->make(ProducerDriverInterface::class),
                sourceService: $source,
                logger: $app['log'],
            );
        });

        $this->app->singleton(IdempotencyStoreInterface::class, function ($app) {
            return match ($app['config']['kafka-messaging.idempotency.store']) {
                'memory' => new InMemoryIdempotencyStore(),
                default => new PdoIdempotencyStore(
                    $app['db']->connection()->getPdo(),
                    (string) $app['config']['kafka-messaging.idempotency.table'],
                ),
            };
        });
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__ . '/config/kafka-messaging.php' => $this->app->configPath('kafka-messaging.php'),
        ], 'kafka-messaging-config');
    }
}
