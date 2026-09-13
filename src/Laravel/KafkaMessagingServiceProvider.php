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
            $driver = $app['config']['kafka-messaging.driver'];

            if ($driver === 'memory') {
                return new InMemoryProducerDriver();
            }

            if ($driver === 'rdkafka') {
                // Order-survival rule: a missing/unloaded extension (e.g.
                // php-fpm not restarted since pecl install) must NEVER 500 the
                // caller's request. Fall back to the shadow log driver and
                // scream in the logs instead.
                if (!extension_loaded('rdkafka')) {
                    $app['log']->error('kafka-messaging: driver=rdkafka but ext-rdkafka is not loaded in this runtime — falling back to the log driver', ['data' => [
                        'sapi' => PHP_SAPI,
                        'hint' => 'restart php-fpm to load the extension for web requests',
                    ]]);

                    return new LogProducerDriver($app['log']);
                }

                return new RdKafkaProducerDriver(
                    \Order\KafkaMessaging\BrokerConfig::fromArray((array) $app['config']['kafka-messaging']),
                );
            }

            return new LogProducerDriver($app['log']);
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
                // A resolver, not a handle. This store is a singleton inside a
                // daemon that outlives its database connection: passing the PDO
                // pinned the one that existed at boot, so once the server closed
                // it the framework reconnected for the handler's own queries
                // while the store kept writing through a dead handle. The mark
                // then failed after the work had already succeeded — a message
                // processed but never recorded, which is exactly the state a
                // redelivery re-runs.
                default => new PdoIdempotencyStore(
                    fn (): \PDO => $app['db']->connection()->getPdo(),
                    (string) $app['config']['kafka-messaging.idempotency.table'],
                    // getPdo() never checks the handle it returns, so without
                    // this the retry resolves the same dead connection again.
                    fn () => $app['db']->connection()->reconnect(),
                ),
            };
        });
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__ . '/config/kafka-messaging.php' => $this->app->configPath('kafka-messaging.php'),
        ], 'kafka-messaging-config');

        if ($this->app->runningInConsole()) {
            $this->commands([
                Commands\KafkaConsumeCommand::class,
                Commands\KafkaReplayCommand::class,
                Commands\KafkaReconcileCommand::class,
            ]);
        }
    }
}
