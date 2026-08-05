<?php

// Published into each service as config/kafka-messaging.php
return [
    // Which producer driver to bind:
    //   'log'     → LogProducerDriver: shadow mode, logs the wire message (safe default)
    //   'memory'  → InMemoryProducerDriver: tests
    //   'rdkafka' → real broker driver (requires ext-rdkafka; Phase 0 step 2)
    'driver' => env('KAFKA_MESSAGING_DRIVER', 'log'),

    // The producing service's identity — goes into the source_service header.
    // MUST be set per service, e.g. "service-order".
    'source_service' => env('KAFKA_SOURCE_SERVICE', ''),

    // Dual-write master switch (04-migration-runbook.md Phase 1). Publish via
    // KafkaProducer::safePublish() ONLY when this is true — SNS remains the
    // source of truth until cutover.
    'dual_write' => env('KAFKA_DUAL_WRITE', false),

    // Broker list for the rdkafka driver (unused by log/memory).
    'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),

    'consumer' => [
        // Consumer group per service, e.g. "svc-order" (02-kafka-topics.md §4).
        'group' => env('KAFKA_CONSUMER_GROUP', ''),

        // Topics this service subscribes to, e.g. ['order.events', 'user.events'].
        // The retry daemon (kafka:consume --retry) derives
        // "<group>.<topic>.retry" from the same list.
        'topics' => [],

        // event_type => handler class. Resolved from the container;
        // handle(Envelope) or __invoke(Envelope) both work.
        // e.g. 'OrderCreatedEvent' => \App\Kafka\Handlers\OrderCreatedHandler::class,
        'handlers' => [],
    ],

    'idempotency' => [
        // 'pdo' binds PdoIdempotencyStore on the service's default DB
        // connection; 'memory' for tests; null disables (NOT allowed for
        // cutover — see 04-migration-runbook.md المخاطر).
        'store' => env('KAFKA_IDEMPOTENCY_STORE', 'pdo'),
        'table' => env('KAFKA_IDEMPOTENCY_TABLE', 'processed_messages'),
    ],
];
