<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Drivers;

use Order\KafkaMessaging\BrokerConfig;
use Order\KafkaMessaging\Contracts\BatchProducerDriverInterface;
use Order\KafkaMessaging\Exceptions\PublishFailedException;

/**
 * Real broker driver over php-rdkafka (librdkafka).
 *
 * Durability-first configuration (02-kafka-topics.md §2):
 *   acks=all + enable.idempotence — the broker-side dedupe complements (does
 *   not replace) the consumer-side message_id idempotency.
 *
 * send() is SYNCHRONOUS: it flushes and throws PublishFailedException unless
 * the broker acknowledged delivery. Correctness over throughput — dual-write
 * volumes are modest; batching can come later behind the same interface.
 */
final class RdKafkaProducerDriver implements BatchProducerDriverInterface
{
    private \RdKafka\Producer $producer;

    /** @var array<string, \RdKafka\ProducerTopic> */
    private array $topics = [];

    private ?string $lastDeliveryError = null;

    /** @var list<string> collected per-message delivery errors during a batch */
    private array $batchErrors = [];

    private bool $collectingBatch = false;

    /**
     * @param BrokerConfig|string $brokers a full BrokerConfig (SASL/SSL aware) or a bare broker list
     * @param array<string, string> $extraConf raw librdkafka overrides
     */
    public function __construct(
        BrokerConfig|string $brokers,
        private readonly int $flushTimeoutMs = 10000,
        array $extraConf = [],
    ) {
        if (!extension_loaded('rdkafka')) {
            throw new \RuntimeException(
                'ext-rdkafka is not loaded. Install: brew install librdkafka && pecl install rdkafka'
            );
        }

        $broker = is_string($brokers) ? new BrokerConfig($brokers) : $brokers;

        $conf = new \RdKafka\Conf();
        foreach ($broker->toLibrdKafkaConf() as $key => $value) {
            $conf->set($key, $value);
        }
        $conf->set('acks', 'all');
        $conf->set('enable.idempotence', 'true');
        // No producer-side compression: zstd inside php-fpm was implicated in
        // SIGABRT worker crashes (2026-08-05); the topics carry
        // compression.type=zstd broker-side anyway, so nothing is lost.
        $conf->set('socket.timeout.ms', '5000');
        $conf->set('message.timeout.ms', (string) $this->flushTimeoutMs);
        foreach ($extraConf as $k => $v) {
            $conf->set($k, $v);
        }

        // Delivery-report callback is the ONLY reliable per-message error
        // signal in librdkafka's async model — flush() alone can return OK
        // while an individual message failed.
        $conf->setDrMsgCb(function (\RdKafka\Producer $producer, \RdKafka\Message $message): void {
            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                return;
            }

            $error = rd_kafka_err2str($message->err);
            $this->lastDeliveryError = $error;

            if ($this->collectingBatch) {
                // opaque carries the message's index within the batch
                $this->batchErrors[] = ($message->opaque ?? '?') . '|' . $error;
            }
        });

        $this->producer = new \RdKafka\Producer($conf);
    }

    public function send(string $topic, string $key, array $headers, string $payload): void
    {
        $this->lastDeliveryError = null;

        $topicHandle = $this->topics[$topic] ??= $this->producer->newTopic($topic);
        $topicHandle->producev(RD_KAFKA_PARTITION_UA, 0, $payload, $key, $headers);

        $result = $this->producer->flush($this->flushTimeoutMs);

        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new PublishFailedException(
                "Kafka flush failed for topic '{$topic}': " . rd_kafka_err2str($result)
            );
        }
        if ($this->lastDeliveryError !== null) {
            throw new PublishFailedException(
                "Kafka delivery failed for topic '{$topic}': {$this->lastDeliveryError}"
            );
        }
    }

    public function sendBatch(string $topic, array $messages): array
    {
        if ($messages === []) {
            return [];
        }

        $this->batchErrors = [];
        $this->collectingBatch = true;
        $topicHandle = $this->topics[$topic] ??= $this->producer->newTopic($topic);

        try {
            foreach (array_values($messages) as $i => $message) {
                // opaque = the batch index, so the delivery report can name
                // exactly which message failed rather than failing the lot.
                $topicHandle->producev(
                    RD_KAFKA_PARTITION_UA,
                    0,
                    $message['payload'],
                    $message['key'],
                    $message['headers'],
                    null,
                    (string) $i,
                );
            }

            $result = $this->producer->flush($this->flushTimeoutMs);

            if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                // The flush itself timed out: report every message as failed
                // rather than pretending a partial success we cannot verify.
                $error = 'flush failed: ' . rd_kafka_err2str($result);

                return array_map(
                    static fn (int $i): array => ['index' => $i, 'error' => $error],
                    range(0, count($messages) - 1)
                );
            }

            $failures = [];
            foreach ($this->batchErrors as $entry) {
                [$index, $error] = explode('|', $entry, 2);
                $failures[] = ['index' => (int) $index, 'error' => $error];
            }

            return $failures;
        } finally {
            $this->collectingBatch = false;
            $this->batchErrors = [];
        }
    }

}
