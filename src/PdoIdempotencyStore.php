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
 *
 * ── CONNECTIONS OUTLIVE THIS OBJECT'S ASSUMPTIONS ────────────────────────────
 * A consumer daemon runs for days and idles between messages, so the database
 * closes the connection under it. Accepting a \PDO handle and holding it meant
 * the store kept using a dead one: the framework reconnects transparently for
 * its own queries, so the handler succeeded and only the mark failed — the
 * message was processed and never recorded as processed, which is the one
 * combination that lets a redelivery run twice.
 *
 * Two changes fix it. The constructor also accepts a resolver, so the store
 * asks for the current connection instead of remembering one; and every
 * statement retries once on a lost connection after dropping the cached handle,
 * because a resolver alone still returns the stale object when nothing has
 * forced a reconnect yet.
 *
 * Dropping the cached handle turned out not to be that force. Under Laravel the
 * resolver is getPdo(), which returns whatever the connection is holding with no
 * liveness check, so the retry resolved the very same dead handle and failed the
 * same way. On stage a Postgres restart (2026-09-11 04:50 UTC) left long-running
 * consumers processing without dedupe until they were restarted. So the retry
 * now asks the owner of the connection to replace it first, through $reconnect.
 */
final class PdoIdempotencyStore implements IdempotencyStoreInterface
{
    /** @var \PDO|(\Closure(): \PDO) */
    private $connection;

    private ?\PDO $resolved = null;

    /**
     * @param \PDO|(\Closure(): \PDO) $pdo A handle, or a resolver returning the
     *        current one. Prefer the resolver anywhere the connection can be
     *        replaced under you — which is every long-running consumer.
     * @param (\Closure(): mixed)|null $reconnect Replaces the underlying
     *        connection before the retry. Required whenever the resolver can
     *        hand back a handle it has not checked, as Laravel's getPdo() does.
     */
    public function __construct(
        \PDO|\Closure $pdo,
        private readonly string $table = 'processed_messages',
        private readonly ?\Closure $reconnect = null,
    ) {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $table)) {
            throw new \InvalidArgumentException('Invalid idempotency table name.');
        }

        $this->connection = $pdo;
    }

    public function wasProcessed(string $consumerGroup, string $messageId): bool
    {
        return $this->run(function (\PDO $pdo) use ($consumerGroup, $messageId): bool {
            $stmt = $pdo->prepare(
                "SELECT 1 FROM {$this->table} WHERE consumer_group = ? AND message_id = ? LIMIT 1"
            );
            $stmt->execute([$consumerGroup, $messageId]);

            return $stmt->fetchColumn() !== false;
        });
    }

    public function markProcessed(string $consumerGroup, string $messageId): void
    {
        $this->run(function (\PDO $pdo) use ($consumerGroup, $messageId): void {
            $stmt = $pdo->prepare(
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
        });
    }

    /**
     * Run a statement, and retry it once against a freshly resolved connection
     * when the failure was the connection itself rather than the statement.
     *
     * Only once: a second lost connection is a real outage, and retrying past
     * that would stall the consumer on a broker whose messages keep arriving.
     */
    private function run(callable $fn): mixed
    {
        try {
            return $fn($this->pdo());
        } catch (\PDOException $e) {
            if (!$this->isLostConnection($e) || $this->connection instanceof \PDO) {
                // A bare handle cannot be re-resolved — nothing to retry with.
                throw $e;
            }

            $this->resolved = null;

            if ($this->reconnect !== null) {
                ($this->reconnect)();
            }

            return $fn($this->pdo());
        }
    }

    private function pdo(): \PDO
    {
        if ($this->connection instanceof \PDO) {
            return $this->connection;
        }

        return $this->resolved ??= ($this->connection)();
    }

    /**
     * Driver messages rather than SQLSTATE: MySQL reports a dropped connection
     * as HY000/2006 and Postgres as 08006, but both also use those codes for
     * faults a retry cannot fix, so the text is what actually distinguishes them.
     */
    private function isLostConnection(\PDOException $e): bool
    {
        $message = $e->getMessage();

        foreach ([
            'server has gone away',
            'Lost connection',
            'no connection to the server',
            'Error while sending',
            'SSL connection has been closed unexpectedly',
            'Connection refused',
            'server closed the connection unexpectedly',
            'connection is no longer usable',
            'Broken pipe',
        ] as $needle) {
            if (stripos($message, $needle) !== false) {
                return true;
            }
        }

        return false;
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
