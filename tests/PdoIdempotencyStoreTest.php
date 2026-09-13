<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\PdoIdempotencyStore;
use PHPUnit\Framework\TestCase;

final class PdoIdempotencyStoreTest extends TestCase
{
    private \PDO $pdo;
    private PdoIdempotencyStore $store;

    protected function setUp(): void
    {
        if (!extension_loaded('pdo_sqlite')) {
            $this->markTestSkipped('pdo_sqlite not available');
        }

        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        PdoIdempotencyStore::createTable($this->pdo);
        $this->store = new PdoIdempotencyStore($this->pdo);
    }

    public function testMarkThenCheck(): void
    {
        $this->assertFalse($this->store->wasProcessed('svc-payment', 'msg-1'));

        $this->store->markProcessed('svc-payment', 'msg-1');

        $this->assertTrue($this->store->wasProcessed('svc-payment', 'msg-1'));
    }

    public function testGroupsAreIndependent(): void
    {
        $this->store->markProcessed('svc-payment', 'msg-1');

        $this->assertFalse(
            $this->store->wasProcessed('svc-wallet', 'msg-1'),
            'the same message_id must be processable once PER consumer group'
        );
    }

    public function testConcurrentDuplicateMarkIsNotAnError(): void
    {
        $this->store->markProcessed('svc-payment', 'msg-1');
        $this->store->markProcessed('svc-payment', 'msg-1'); // second insert hits the PK — swallowed

        $this->assertTrue($this->store->wasProcessed('svc-payment', 'msg-1'));
    }

    public function testHostileTableNameRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new PdoIdempotencyStore($this->pdo, 'processed; DROP TABLE x --');
    }

    /**
     * The daemon case: the connection the store was given dies, the resolver
     * hands back a live one, and the statement goes through on the retry.
     */
    public function testResolverIsAskedAgainAfterAConnectionIsLost(): void
    {
        $live = $this->pdo;
        $dead = new class('sqlite::memory:') extends \PDO {
            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                throw new \PDOException('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away');
            }
        };

        $handles = [$dead, $live];
        $asked = 0;
        $store = new PdoIdempotencyStore(function () use (&$handles, &$asked): \PDO {
            $asked++;

            return array_shift($handles) ?? $this->pdo;
        });

        $store->markProcessed('svc-wallet', 'msg-lost');

        $this->assertSame(2, $asked, 'the resolver must be consulted again, not reused');
        $this->assertTrue($store->wasProcessed('svc-wallet', 'msg-lost'));
    }

    public function testAFailureThatIsNotAConnectionLossIsNotRetried(): void
    {
        $attempts = 0;
        $store = new PdoIdempotencyStore(function () use (&$attempts): \PDO {
            $attempts++;

            return new class('sqlite::memory:') extends \PDO {
                public function prepare(string $query, array $options = []): \PDOStatement|false
                {
                    throw new \PDOException('SQLSTATE[42S02]: Base table or view not found');
                }
            };
        });

        $this->expectException(\PDOException::class);

        try {
            $store->markProcessed('svc-wallet', 'msg-x');
        } finally {
            $this->assertSame(1, $attempts, 'a schema error must not be retried');
        }
    }

    public function testABareHandleStillWorks(): void
    {
        $store = new PdoIdempotencyStore($this->pdo);

        $store->markProcessed('svc-order', 'msg-bare');

        $this->assertTrue($store->wasProcessed('svc-order', 'msg-bare'));
    }

    /**
     * What Laravel actually does: getPdo() hands back the connection it is
     * holding with no liveness check, so asking the resolver again returns the
     * very same dead handle until something calls reconnect(). The resolver in
     * testResolverIsAskedAgainAfterAConnectionIsLost is kinder than the
     * framework, which is how this slipped through.
     */
    public function testReconnectsWhenTheResolverKeepsReturningTheSameDeadHandle(): void
    {
        $dead = new class('sqlite::memory:') extends \PDO {
            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                throw new \PDOException('SQLSTATE[HY000]: General error: 7 no connection to the server');
            }
        };

        $current = $dead;
        $reconnects = 0;

        $store = new PdoIdempotencyStore(
            function () use (&$current): \PDO {
                return $current;
            },
            'processed_messages',
            function () use (&$current, &$reconnects): void {
                $reconnects++;
                $current = $this->pdo;
            },
        );

        $store->markProcessed('svc-menu', 'msg-same-dead-handle');

        $this->assertSame(1, $reconnects, 'a lost connection must force a reconnect before the retry');
        $this->assertTrue($store->wasProcessed('svc-menu', 'msg-same-dead-handle'));
    }

    public function testNoReconnectForAFailureThatIsNotAConnectionLoss(): void
    {
        $reconnects = 0;
        $store = new PdoIdempotencyStore(
            fn (): \PDO => new class('sqlite::memory:') extends \PDO {
                public function prepare(string $query, array $options = []): \PDOStatement|false
                {
                    throw new \PDOException('SQLSTATE[42S02]: Base table or view not found');
                }
            },
            'processed_messages',
            function () use (&$reconnects): void {
                $reconnects++;
            },
        );

        try {
            $store->markProcessed('svc-menu', 'msg-schema');
            $this->fail('a schema error must surface');
        } catch (\PDOException) {
            $this->assertSame(0, $reconnects, 'only a lost connection may trigger a reconnect');
        }
    }
}
