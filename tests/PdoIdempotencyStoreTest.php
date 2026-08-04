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
}
