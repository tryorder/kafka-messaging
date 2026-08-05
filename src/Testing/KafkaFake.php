<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Testing;

use Order\KafkaMessaging\Contracts\ProducerDriverInterface;
use PHPUnit\Framework\Assert;

/**
 * Test double for producers: records every publish and exposes assertions,
 * so a service can prove "this flow emits that event" without a broker.
 *
 *   $fake = KafkaFake::bind();            // Laravel: swaps the bound driver
 *   ... exercise the flow ...
 *   $fake->assertPublished('OrderCreatedEvent');
 *   $fake->assertPublishedOn('order.events', 'OrderCreatedEvent', fn ($m) => $m['tenant'] === 'jabourih');
 */
final class KafkaFake implements ProducerDriverInterface
{
    /** @var list<array{topic: string, key: string, headers: array<string, string>, payload: string, tenant: string, event_type: string, data: array<string, mixed>}> */
    private array $published = [];

    /**
     * Swap the container's producer driver for a fake (Laravel only).
     * Returns the fake so the test can assert on it.
     */
    public static function bind(): self
    {
        if (!function_exists('app')) {
            throw new \LogicException('KafkaFake::bind() requires a Laravel container; instantiate KafkaFake directly otherwise.');
        }

        $fake = new self();
        app()->instance(ProducerDriverInterface::class, $fake);
        app()->forgetInstance(\Order\KafkaMessaging\KafkaProducer::class);

        return $fake;
    }

    public function send(string $topic, string $key, array $headers, string $payload): void
    {
        $decoded = json_decode($payload, true);

        $this->published[] = [
            'topic' => $topic,
            'key' => $key,
            'headers' => $headers,
            'payload' => $payload,
            'tenant' => (string) ($headers['tenant'] ?? ($decoded['tenant'] ?? '')),
            'event_type' => (string) ($headers['event_type'] ?? ''),
            'data' => is_array($decoded['data'] ?? null) ? $decoded['data'] : [],
        ];
    }

    /**
     * @return list<array{topic: string, key: string, headers: array<string, string>, payload: string, tenant: string, event_type: string, data: array<string, mixed>}>
     */
    public function published(?string $eventType = null, ?string $topic = null, ?callable $filter = null): array
    {
        return array_values(array_filter($this->published, static function (array $m) use ($eventType, $topic, $filter): bool {
            if ($eventType !== null && $m['event_type'] !== $eventType) {
                return false;
            }
            if ($topic !== null && $m['topic'] !== $topic) {
                return false;
            }

            return $filter === null || $filter($m) === true;
        }));
    }

    public function assertPublished(string $eventType, ?callable $filter = null): self
    {
        Assert::assertNotEmpty(
            $this->published($eventType, null, $filter),
            "Expected event [{$eventType}] to be published, but it was not." . $this->summary()
        );

        return $this;
    }

    public function assertPublishedOn(string $topic, string $eventType, ?callable $filter = null): self
    {
        Assert::assertNotEmpty(
            $this->published($eventType, $topic, $filter),
            "Expected event [{$eventType}] on topic [{$topic}], but it was not published there." . $this->summary()
        );

        return $this;
    }

    public function assertPublishedTimes(string $eventType, int $times): self
    {
        $actual = count($this->published($eventType));
        Assert::assertSame(
            $times,
            $actual,
            "Expected event [{$eventType}] to be published {$times} time(s), got {$actual}." . $this->summary()
        );

        return $this;
    }

    public function assertNotPublished(string $eventType): self
    {
        Assert::assertEmpty(
            $this->published($eventType),
            "Expected event [{$eventType}] NOT to be published, but it was." . $this->summary()
        );

        return $this;
    }

    public function assertNothingPublished(): self
    {
        Assert::assertEmpty($this->published, 'Expected no events to be published.' . $this->summary());

        return $this;
    }

    /**
     * Every published message must carry a tenant — the partition key contract
     * (02-kafka-topics.md §3). Handy as a blanket assertion in producer tests.
     */
    public function assertAllHaveTenant(): self
    {
        foreach ($this->published as $m) {
            Assert::assertNotSame('', $m['tenant'], "Event [{$m['event_type']}] was published without a tenant.");
        }

        return $this;
    }

    public function flush(): void
    {
        $this->published = [];
    }

    private function summary(): string
    {
        if ($this->published === []) {
            return ' Nothing was published.';
        }

        $seen = array_map(static fn (array $m): string => "{$m['event_type']}@{$m['topic']}", $this->published);

        return ' Published: ' . implode(', ', $seen) . '.';
    }
}
