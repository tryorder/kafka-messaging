<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Drivers;

use Order\KafkaMessaging\BrokerConfig;
use Order\KafkaMessaging\ConsumedMessage;
use Order\KafkaMessaging\Contracts\ConsumerDriverInterface;

/**
 * Real broker consumer over php-rdkafka's high-level KafkaConsumer.
 *
 * enable.auto.commit=false is non-negotiable: the commit decision belongs to
 * MessageProcessor's outcome (EscalationFailed must NOT advance the offset).
 */
final class RdKafkaConsumerDriver implements ConsumerDriverInterface
{
    private ?\RdKafka\KafkaConsumer $consumer = null;

    /** @var \SplObjectStorage<ConsumedMessage, \RdKafka\Message> */
    private \SplObjectStorage $native;

    private readonly BrokerConfig $broker;

    /**
     * @param BrokerConfig|string $brokers a full BrokerConfig (SASL/SSL aware) or a bare broker list
     * @param array<string, string> $extraConf raw librdkafka overrides
     */
    public function __construct(
        BrokerConfig|string $brokers,
        private readonly string $offsetReset = 'earliest',
        private readonly array $extraConf = [],
    ) {
        $this->broker = is_string($brokers) ? new BrokerConfig($brokers) : $brokers;

        if (!extension_loaded('rdkafka')) {
            throw new \RuntimeException(
                'ext-rdkafka is not loaded. Install: brew install librdkafka && pecl install rdkafka'
            );
        }

        $this->native = new \SplObjectStorage();
    }

    public function subscribe(string $group, array $topics): void
    {
        $conf = new \RdKafka\Conf();
        foreach ($this->broker->toLibrdKafkaConf() as $key => $value) {
            $conf->set($key, $value);
        }
        $conf->set('group.id', $group);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('auto.offset.reset', $this->offsetReset);
        foreach ($this->extraConf as $k => $v) {
            $conf->set($k, $v);
        }

        $this->consumer = new \RdKafka\KafkaConsumer($conf);
        $this->consumer->subscribe($topics);
    }

    public function poll(int $timeoutMs): ?ConsumedMessage
    {
        if ($this->consumer === null) {
            throw new \LogicException('subscribe() must be called before poll().');
        }

        $message = $this->consumer->consume($timeoutMs);

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return null;
            default:
                // Transport-level errors are retryable by the poll loop;
                // surfacing them as null keeps the daemon alive and librdkafka
                // reconnects internally.
                return null;
        }

        /** @var array<string, string> $headers */
        $headers = [];
        foreach ($message->headers ?? [] as $name => $value) {
            $headers[(string) $name] = (string) $value;
        }

        $consumed = new ConsumedMessage(
            topic: (string) $message->topic_name,
            key: $message->key !== null ? (string) $message->key : null,
            headers: $headers,
            payload: (string) $message->payload,
            partition: $message->partition,
            offset: (int) $message->offset,
        );

        $this->native[$consumed] = $message;

        return $consumed;
    }

    public function commit(ConsumedMessage $message): void
    {
        if ($this->consumer === null) {
            throw new \LogicException('subscribe() must be called before commit().');
        }

        if (isset($this->native[$message])) {
            $this->consumer->commit($this->native[$message]);
            unset($this->native[$message]);
        }
    }
}
