<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Laravel\Commands;

use Illuminate\Console\Command;
use Order\KafkaMessaging\BrokerConfig;

/**
 * Phase-1 exit gate: compare what Kafka received against what the SNS path
 * actually delivered.
 *
 * The measurement is taken AT THE CONSUMER, deliberately. The gateway relay
 * runs Guzzle with `http_errors => false`, so a subscriber answering 4xx/5xx is
 * treated as delivered and never retried. Counting at the sender reports those
 * as successes and hides the very loss this migration exists to remove — the
 * runbook calls this out explicitly.
 *
 * Sources:
 *   Kafka — read with a THROWAWAY consumer group, exactly like kafka:replay,
 *           so live consumer offsets are never moved.
 *   SNS   — the gateway's own delivery trace. Each attempt logs the subscriber
 *           URI and the HTTP status it answered with. Requires
 *           SNS_DEBUG_LOGS=true in the gateway's environment.
 *
 *   php artisan kafka:reconcile --since="2026-08-06 10:00"
 *   php artisan kafka:reconcile --gateway-log=/path/to/apigateway/storage/logs/laravel.log
 */
final class KafkaReconcileCommand extends Command
{
    protected $signature = 'kafka:reconcile
        {--topics= : Comma-separated topics (default: consumer.topics from config)}
        {--since= : Only count events at or after this time (any strtotime-able value)}
        {--gateway-log= : Path to the gateway log carrying the SNS delivery trace}
        {--limit=5000 : Maximum messages to read per topic}
        {--poll-timeout=1500 : Poll timeout in ms — reconcile reads many topics, so it idles out faster than the daemon}';

    protected $description = 'Compare Kafka volume against what SNS actually delivered (migration phase-1 gate)';

    /** A subscriber URI is http://host/api/v1/<service>/… — that segment names the endpoint. */
    private const URI_SERVICE = '#https?://[^/]+/api/v\d+/([^/?]+)#';

    public function handle(): int
    {
        if (!extension_loaded('rdkafka')) {
            $this->error('ext-rdkafka is not loaded.');

            return self::FAILURE;
        }

        $config = (array) $this->laravel['config']['kafka-messaging'];

        $topics = $this->option('topics')
            ? array_values(array_filter(array_map('trim', explode(',', (string) $this->option('topics')))))
            : (array) ($config['consumer']['topics'] ?? []);

        if ($topics === []) {
            $this->error('No topics — pass --topics or set consumer.topics.');

            return self::FAILURE;
        }

        $since = $this->option('since') ? strtotime((string) $this->option('since')) : null;
        if ($since === false) {
            $this->error('Could not parse --since.');

            return self::FAILURE;
        }

        $kafka = $this->readKafka(BrokerConfig::fromArray($config), $topics, $since);
        [$sns, $silent] = $this->readSnsTrace($since);

        $this->renderKafka($topics, $kafka);
        $this->renderSns($sns);
        $this->renderGap($kafka, $sns, $silent);

        return self::SUCCESS;
    }

    /**
     * @param list<string> $topics
     * @return array<string, array<string, int>> topic => event_type => count
     */
    private function readKafka(BrokerConfig $broker, array $topics, ?int $since): array
    {
        $limit = max(1, (int) $this->option('limit'));
        $pollTimeout = (int) $this->option('poll-timeout');

        $counts = [];
        foreach ($topics as $topic) {
            $counts[$topic] = [];
        }

        $conf = new \RdKafka\Conf();
        foreach ($broker->toLibrdKafkaConf() as $k => $v) {
            $conf->set($k, $v);
        }
        // Throwaway group + no commit: reconciliation is an observation, it must
        // never move a live consumer's position.
        $conf->set('group.id', 'reconcile-' . substr(sha1(implode(',', $topics) . microtime(true)), 0, 12));
        $conf->set('enable.auto.commit', 'false');
        $conf->set('auto.offset.reset', 'earliest');

        // ONE subscription across every topic. Subscribing per topic would pay a
        // group join + rebalance each time, which dominates the runtime when
        // most topics are empty.
        $consumer = new \RdKafka\KafkaConsumer($conf);
        $consumer->subscribe($topics);

        $this->output->write('  reading ' . count($topics) . ' topic(s) ... ');

        $read = 0;
        $idle = 0;
        while ($read < $limit && $idle < 3) {
            $message = $consumer->consume($pollTimeout);
            if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                $idle++;
                continue;
            }
            $idle = 0;
            $read++;

            if ($since !== null && (int) ($message->timestamp / 1000) < $since) {
                continue;
            }

            $eventType = '(no event_type)';
            foreach ($message->headers ?? [] as $name => $value) {
                if ((string) $name === 'event_type') {
                    $eventType = (string) $value;
                }
            }

            $topic = (string) $message->topic_name;
            $counts[$topic][$eventType] = ($counts[$topic][$eventType] ?? 0) + 1;
        }

        $consumer->unsubscribe();
        $this->output->writeln($read . ' message(s)');

        return $counts;
    }

    /**
     * @return array{0: array<string, array<int, int>>, 1: list<array{service: string, status: int, uri: string}>}
     */
    private function readSnsTrace(?int $since): array
    {
        $path = (string) ($this->option('gateway-log') ?? '');
        if ($path === '') {
            $this->warn('No --gateway-log given — the SNS side cannot be measured.');

            return [[], []];
        }
        if (!is_readable($path)) {
            $this->warn("Gateway log not readable: {$path}");

            return [[], []];
        }

        $delivered = [];
        $silent = [];

        $fh = fopen($path, 'r');
        if ($fh === false) {
            return [[], []];
        }

        while (($line = fgets($fh)) !== false) {
            if (!str_contains($line, 'subscriber POST done')) {
                continue;
            }
            if ($since !== null && preg_match('#^\[([^\]]+)\]#', $line, $ts) === 1) {
                $at = strtotime($ts[1]);
                if ($at !== false && $at < $since) {
                    continue;
                }
            }
            if (preg_match('#"uri":"([^"]+)"#', $line, $u) !== 1
                || preg_match('#"status":(\d+)#', $line, $s) !== 1) {
                continue;
            }

            $uri = $u[1];
            $status = (int) $s[1];
            $service = preg_match(self::URI_SERVICE, $uri, $m) === 1 ? $m[1] : $uri;

            $delivered[$service][$status] = ($delivered[$service][$status] ?? 0) + 1;

            if ($status >= 400) {
                // http_errors => false: the relay counted this as delivered.
                $silent[] = ['service' => $service, 'status' => $status, 'uri' => $uri];
            }
        }
        fclose($fh);

        return [$delivered, $silent];
    }

    /**
     * @param list<string> $topics
     * @param array<string, array<string, int>> $kafka
     */
    private function renderKafka(array $topics, array $kafka): void
    {
        $this->newLine();
        $this->info('KAFKA — what the producers wrote');

        $total = 0;
        foreach ($topics as $topic) {
            $events = $kafka[$topic] ?? [];
            $count = array_sum($events);
            $total += $count;

            arsort($events);
            $detail = [];
            foreach ($events as $type => $n) {
                $detail[] = "{$type}×{$n}";
            }

            $this->line(sprintf('  %-24s %-5d %s', $topic, $count, implode(', ', $detail)));
        }
        $this->line(sprintf('  %-24s %d', 'TOTAL', $total));
    }

    /** @param array<string, array<int, int>> $sns */
    private function renderSns(array $sns): void
    {
        $this->newLine();
        $this->info('SNS — what the gateway actually delivered, per subscriber');

        if ($sns === []) {
            $this->line('  (nothing — is SNS_DEBUG_LOGS=true in the gateway environment?)');

            return;
        }

        ksort($sns);
        foreach ($sns as $service => $codes) {
            $ok = 0;
            $bad = 0;
            ksort($codes);
            $detail = [];
            foreach ($codes as $status => $n) {
                $status >= 400 ? $bad += $n : $ok += $n;
                $detail[] = "{$status}×{$n}";
            }

            $this->line(sprintf(
                '  %-24s ok=%-4d failed=%-4d [%s]%s',
                $service,
                $ok,
                $bad,
                implode(', ', $detail),
                $bad > 0 ? '   <-- COUNTED AS DELIVERED' : ''
            ));
        }
    }

    /**
     * @param array<string, array<string, int>> $kafka
     * @param array<string, array<int, int>> $sns
     * @param list<array{service: string, status: int, uri: string}> $silent
     */
    private function renderGap(array $kafka, array $sns, array $silent): void
    {
        $kafkaTotal = 0;
        foreach ($kafka as $events) {
            $kafkaTotal += array_sum($events);
        }

        $snsTotal = 0;
        foreach ($sns as $codes) {
            $snsTotal += array_sum($codes);
        }

        $this->newLine();
        $this->info('THE GAP');
        $this->line(sprintf('  %-34s %d', 'Kafka messages written', $kafkaTotal));
        $this->line(sprintf('  %-34s %d', 'SNS delivery attempts', $snsTotal));
        $this->line(sprintf('  %-34s %d', '...of which answered 4xx/5xx', count($silent)));

        if ($silent !== []) {
            $this->newLine();
            $this->line('  Not retried and not dropped loudly — http_errors => false treats');
            $this->line('  these as success. This is the loss the migration removes:');
            foreach (array_slice($silent, 0, 15) as $row) {
                $this->line(sprintf('    %d  %-20s %s', $row['status'], $row['service'], $row['uri']));
            }
            if (count($silent) > 15) {
                $this->line('    ... and ' . (count($silent) - 15) . ' more');
            }
        }

        $this->newLine();
        $this->line('  One Kafka message per PUBLISHED event; one SNS row per SUBSCRIBER.');
        $this->line('  A topic with N subscribers yields N SNS rows for one event —');
        $this->line('  compare the shapes, not the raw totals.');
    }
}
