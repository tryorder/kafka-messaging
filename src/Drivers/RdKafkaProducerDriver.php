<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Drivers;

use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
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
final class RdKafkaProducerDriver implements ProducerDriverInterface
{
    private \RdKafka\Producer $producer;

    /** @var array<string, \RdKafka\ProducerTopic> */
    private array $topics = [];

    private ?string $lastDeliveryError = null;

    /**
     * @param array<string, string> $extraConf raw librdkafka overrides
     */
    public function __construct(
        string $brokers,
        private readonly int $flushTimeoutMs = 10000,
        array $extraConf = [],
    ) {
        if (!extension_loaded('rdkafka')) {
            throw new \RuntimeException(
                'ext-rdkafka is not loaded. Install: brew install librdkafka && pecl install rdkafka'
            );
        }

        $conf = new \RdKafka\Conf();
        $conf->set('bootstrap.servers', $brokers);
        $conf->set('acks', 'all');
        $conf->set('enable.idempotence', 'true');
        $conf->set('compression.type', 'zstd');
        $conf->set('socket.timeout.ms', '5000');
        $conf->set('message.timeout.ms', (string) $this->flushTimeoutMs);
        foreach ($extraConf as $k => $v) {
            $conf->set($k, $v);
        }

        // Delivery-report callback is the ONLY reliable per-message error
        // signal in librdkafka's async model — flush() alone can return OK
        // while an individual message failed.
        $conf->setDrMsgCb(function (\RdKafka\Producer $producer, \RdKafka\Message $message): void {
            if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                $this->lastDeliveryError = rd_kafka_err2str($message->err);
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

    public function __destruct()
    {
        // Best effort: drain anything still queued so a dying process does
        // not silently drop acknowledged-to-caller messages (there are none
        // in sync mode, but extraConf could have disabled per-send flushing).
        if (isset($this->producer)) {
            $this->producer->flush(1000);
        }
    }
}
