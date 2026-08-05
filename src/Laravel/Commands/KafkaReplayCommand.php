<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Laravel\Commands;

use Illuminate\Console\Command;
use Order\KafkaMessaging\BrokerConfig;
use Order\KafkaMessaging\Drivers\RdKafkaProducerDriver;
use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\MessageProcessor;

/**
 * Replay — the runbook's "قدرة replay مثبتة" success criterion.
 *
 * Reads a topic from a given offset/timestamp with a THROWAWAY consumer group
 * (never the live group, so production offsets are untouched) and either:
 *   - prints what it finds (default, dry-run), or
 *   - re-injects the messages into a target topic (--to), which is how a DLQ
 *     is drained back into the main topic after a fix.
 *
 *   php artisan kafka:replay order.events --from=earliest --limit=20
 *   php artisan kafka:replay svc-payment.order.events.dlq --to=order.events --confirm
 *   php artisan kafka:replay order.events --since="2026-08-05 10:00:00" --event=OrderCreatedEvent
 */
final class KafkaReplayCommand extends Command
{
    protected $signature = 'kafka:replay
        {topic : Topic to read from (e.g. order.events or svc-payment.order.events.dlq)}
        {--from=earliest : earliest|latest — where to start when no --since is given}
        {--since= : Start at this time instead (any strtotime-able value)}
        {--limit=50 : Maximum messages to read}
        {--event= : Only messages whose event_type matches}
        {--tenant= : Only messages for this tenant}
        {--to= : Re-inject matches into this topic (omit for a dry run)}
        {--confirm : Required to actually re-inject; without it --to only previews}
        {--poll-timeout=5000 : Poll timeout in ms}';

    protected $description = 'Replay/inspect messages from a topic without touching live consumer offsets';

    public function handle(): int
    {
        if (!extension_loaded('rdkafka')) {
            $this->error('ext-rdkafka is not loaded.');

            return self::FAILURE;
        }

        $config = (array) $this->laravel['config']['kafka-messaging'];
        $broker = BrokerConfig::fromArray($config);

        $topic = (string) $this->argument('topic');
        $limit = max(1, (int) $this->option('limit'));
        $target = $this->option('to') !== null ? (string) $this->option('to') : null;
        $reinject = $target !== null && (bool) $this->option('confirm');

        // Throwaway group: replay must never move the live group's offsets.
        $group = 'replay-' . substr(sha1($topic . microtime(true)), 0, 12);

        $conf = new \RdKafka\Conf();
        foreach ($broker->toLibrdKafkaConf() as $k => $v) {
            $conf->set($k, $v);
        }
        $conf->set('group.id', $group);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('auto.offset.reset', $this->option('since') ? 'earliest' : (string) $this->option('from'));

        $consumer = new \RdKafka\KafkaConsumer($conf);
        $consumer->subscribe([$topic]);

        $since = $this->option('since') ? strtotime((string) $this->option('since')) : null;
        if ($since === false) {
            $this->error('Could not parse --since.');

            return self::FAILURE;
        }

        $producer = $reinject ? new RdKafkaProducerDriver($broker) : null;

        $this->info("kafka:replay {$topic} · group={$group} (throwaway)"
            . ($target !== null ? " · → {$target}" . ($reinject ? ' [RE-INJECTING]' : ' [dry run — pass --confirm]') : ' [read only]'));

        $read = 0;
        $matched = 0;
        $injected = 0;
        $idle = 0;

        while ($read < $limit && $idle < 3) {
            $message = $consumer->consume((int) $this->option('poll-timeout'));

            if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                $idle++;
                continue;
            }
            $idle = 0;
            $read++;

            /** @var array<string, string> $headers */
            $headers = [];
            foreach ($message->headers ?? [] as $name => $value) {
                $headers[(string) $name] = (string) $value;
            }

            if ($since !== null && (int) ($message->timestamp / 1000) < $since) {
                continue;
            }
            if ($this->option('event') && ($headers['event_type'] ?? null) !== $this->option('event')) {
                continue;
            }
            if ($this->option('tenant') && ($headers['tenant'] ?? null) !== $this->option('tenant')) {
                continue;
            }

            $matched++;
            $this->line(sprintf(
                '  p%d@%d %s · %s · %s',
                $message->partition,
                $message->offset,
                $headers['event_type'] ?? '(no event_type)',
                $headers['tenant'] ?? '(no tenant)',
                $headers['message_id'] ?? '(no message_id)'
            ));

            if ($reinject && $producer !== null) {
                // Keep message_id: consumers dedupe replays they already
                // handled. Strip retry bookkeeping so the message re-enters
                // the flow clean.
                unset(
                    $headers[MessageProcessor::HEADER_RETRY_ROUND],
                    $headers[MessageProcessor::HEADER_RETRY_DELAY],
                    $headers[MessageProcessor::HEADER_RETRY_NOT_BEFORE],
                    $headers[MessageProcessor::HEADER_LAST_ERROR],
                    $headers[MessageProcessor::HEADER_ORIGINAL_TOPIC],
                );

                try {
                    Envelope::fromConsumed($headers, (string) $message->payload); // validate before re-injecting
                    $producer->send((string) $target, (string) ($message->key ?? ''), $headers, (string) $message->payload);
                    $injected++;
                } catch (\Throwable $e) {
                    $this->warn('    skipped: ' . $e->getMessage());
                }
            }
        }

        $this->info("read={$read} matched={$matched}" . ($reinject ? " injected={$injected}" : ''));
        if ($target !== null && !$reinject) {
            $this->warn('Dry run — nothing was written. Re-run with --confirm to re-inject.');
        }

        return self::SUCCESS;
    }
}
