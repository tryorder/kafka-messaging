<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

use Order\KafkaMessaging\Contracts\IdempotencyStoreInterface;

/**
 * The production idempotency store from 03-message-envelope.md §3:
 * processed_messages(consumer_group, message_id) with a composite PK.
 *
 * Portable across MySQL/Postgres/SQLite by design: markProcessed() attempts a
 * plain INSERT and treats a duplicate-key violation (SQLSTATE 23xxx) as
 * "already marked" — no dialect-specific UPSERT needed.
 */
final class PdoIdempotencyStore implements IdempotencyStoreInterface
{
    public function __construct(
        private readonly \PDO $pdo,
        private readonly string $table = 'processed_messages',
    ) {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $table)) {
            throw new \InvalidArgumentException('Invalid idempotency table name.');
        }
    }

    public function wasProcessed(string $consumerGroup, string $messageId): bool
    {
        $stmt = $this->pdo->prepare(
            "SELECT 1 FROM {$this->table} WHERE consumer_group = ? AND message_id = ? LIMIT 1"
        );
        $stmt->execute([$consumerGroup, $messageId]);

        return $stmt->fetchColumn() !== false;
    }

    public function markProcessed(string $consumerGroup, string $messageId): void
    {
        $stmt = $this->pdo->prepare(
            "INSERT INTO {$this->table} (consumer_group, message_id, processed_at) VALUES (?, ?, ?)"
        );

        try {
            $stmt->execute([$consumerGroup, $messageId, gmdate('Y-m-d\TH:i:s\Z')]);
        } catch (\PDOException $e) {
            // Duplicate key (concurrent consumer marked it first) = success.
            if (!str_starts_with((string) $e->getCode(), '23')) {
                throw $e;
            }
        }
    }

    /**
     * Convenience for tests and quick local setup. Services ship this as a
     * real migration instead (index the PK, add a pruning job on processed_at).
     */
    public static function createTable(\PDO $pdo, string $table = 'processed_messages'): void
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $table)) {
            throw new \InvalidArgumentException('Invalid idempotency table name.');
        }

        $pdo->exec(
            "CREATE TABLE IF NOT EXISTS {$table} (
                consumer_group VARCHAR(191) NOT NULL,
                message_id VARCHAR(191) NOT NULL,
                processed_at VARCHAR(32) NOT NULL,
                PRIMARY KEY (consumer_group, message_id)
            )"
        );
    }
}
