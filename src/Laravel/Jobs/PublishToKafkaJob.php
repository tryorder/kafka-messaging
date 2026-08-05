<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Laravel\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Order\KafkaMessaging\KafkaProducer;

/**
 * Publish out-of-band, from a queue worker instead of the web request.
 *
 * Why this is the default path for producers:
 *  - The customer's request never waits on the broker (dual-write must be
 *    invisible to the order flow).
 *  - A broker outage is absorbed by the queue's own retry/backoff.
 *  - macOS-specific but decisive: creating a librdkafka producer inside
 *    php-fpm makes the worker multi-threaded, and any later ObjC/XPC call on
 *    the child side of FPM's fork aborts it (SIGABRT, "multi-threaded process
 *    forked"). CLI workers are unaffected. Verified from crash reports,
 *    2026-08-05.
 *
 * Requires a real queue connection (redis/database) + a running worker;
 * with QUEUE_CONNECTION=sync this executes inline and gains nothing.
 */
final class PublishToKafkaJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    public int $tries = 3;

    /**
     * @param array<string, mixed> $data
     */
    public function __construct(
        public readonly string $topic,
        public readonly string $eventType,
        public readonly string $tenant,
        public readonly array $data,
        public readonly ?string $key = null,
        public readonly ?string $messageId = null,
    ) {
    }

    public function handle(KafkaProducer $producer): void
    {
        // safePublish: a failure here must not fail the job's siblings or
        // spam the failed_jobs table — it is logged inside the producer.
        $producer->safePublish(
            topic: $this->topic,
            eventType: $this->eventType,
            tenant: $this->tenant,
            data: $this->data,
            key: $this->key,
            messageId: $this->messageId,
        );
    }
}
