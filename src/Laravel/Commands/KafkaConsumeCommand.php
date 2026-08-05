<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Laravel\Commands;

use Illuminate\Console\Command;
use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;
use Order\KafkaMessaging\Drivers\RdKafkaConsumerDriver;
use Order\KafkaMessaging\Drivers\RdKafkaProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\KafkaConsumer;

/**
 * The consumer daemon (replaces SNSController::__invoke + the Redis worker).
 *
 * Configuration lives in config/kafka-messaging.php:
 *   consumer.group     — e.g. "svc-payment"
 *   consumer.topics    — e.g. ['order.events']
 *   consumer.handlers  — ['OrderCreatedEvent' => \App\Kafka\OrderCreatedHandler::class]
 *
 * Handlers are resolved from the container; a handle(Envelope) method or
 * __invoke(Envelope) both work.
 *
 * Run under supervisor:
 *   php artisan kafka:consume            # main daemon
 *   php artisan kafka:consume --retry    # retry daemon (honors x-retry-not-before)
 */
final class KafkaConsumeCommand extends Command
{
    protected $signature = 'kafka:consume
        {--group= : Override consumer.group}
        {--topics= : Comma-separated topics override}
        {--retry : Consume the retry topics of the configured topics, honoring delays}
        {--max-messages= : Stop after N messages (default: run forever)}
        {--idle-exit : Exit on first idle poll instead of running as a daemon}';

    protected $description = 'Run the Kafka consumer daemon (order/kafka-messaging)';

    public function handle(): int
    {
        $config = (array) $this->laravel['config']['kafka-messaging'];
        $brokers = (string) ($config['brokers'] ?? '127.0.0.1:9092');

        $group = (string) ($this->option('group') ?: ($config['consumer']['group'] ?? ''));
        if ($group === '') {
            $this->error('No consumer group — set KAFKA_CONSUMER_GROUP or pass --group.');

            return self::FAILURE;
        }

        $topics = $this->option('topics')
            ? array_values(array_filter(array_map('trim', explode(',', (string) $this->option('topics')))))
            : (array) ($config['consumer']['topics'] ?? []);
        if ($topics === []) {
            $this->error('No topics — set consumer.topics in config/kafka-messaging.php or pass --topics.');

            return self::FAILURE;
        }

        $retryMode = (bool) $this->option('retry');
        if ($retryMode) {
            $topics = array_map(static fn (string $t): string => "{$group}.{$t}.retry", $topics);
        }

        $consumer = KafkaConsumer::group($group)
            ->driver(new RdKafkaConsumerDriver($brokers))
            ->subscribe($topics)
            ->withIdempotency($this->laravel->make(IdempotencyStoreInterface::class))
            ->withRetryAndDlq(new RdKafkaProducerDriver($brokers))
            ->logger($this->laravel['log']);

        if ($retryMode) {
            $consumer->honorRetryDelays();
        }

        foreach ((array) ($config['consumer']['handlers'] ?? []) as $eventType => $handlerClass) {
            $handler = $this->laravel->make($handlerClass);
            $callable = method_exists($handler, 'handle') ? [$handler, 'handle'] : $handler;
            $consumer->onEvent((string) $eventType, static fn (Envelope $e) => $callable($e));
        }

        // Graceful shutdown: SIGTERM/SIGINT finish the in-flight message,
        // commit it, and exit — supervisor restarts cleanly, nothing is lost.
        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, static fn () => $consumer->stop());
            pcntl_signal(SIGINT, static fn () => $consumer->stop());
        }

        $mode = $retryMode ? 'retry daemon' : 'daemon';
        $this->info("kafka:consume [{$mode}] group={$group} topics=" . implode(',', $topics));

        $max = $this->option('max-messages') !== null ? (int) $this->option('max-messages') : null;
        $counts = $consumer->run(maxMessages: $max, exitOnIdle: (bool) $this->option('idle-exit'));

        foreach ($counts as $outcome => $count) {
            $this->line("  {$outcome}: {$count}");
        }
        $this->info('consumer stopped.');

        return self::SUCCESS;
    }
}
