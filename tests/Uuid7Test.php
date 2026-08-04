<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\Uuid7;
use PHPUnit\Framework\TestCase;

final class Uuid7Test extends TestCase
{
    public function testFormatIsRfc9562Version7(): void
    {
        for ($i = 0; $i < 200; $i++) {
            $uuid = Uuid7::generate();
            $this->assertMatchesRegularExpression(
                '/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
                $uuid
            );
        }
    }

    public function testTimeOrdering(): void
    {
        $earlier = Uuid7::generate(1_000_000);
        $later = Uuid7::generate(2_000_000);

        $this->assertLessThan(0, strcmp($earlier, $later), 'UUIDv7 must sort by generation time');
    }

    public function testUniqueness(): void
    {
        $ids = [];
        for ($i = 0; $i < 1000; $i++) {
            $ids[] = Uuid7::generate();
        }

        $this->assertCount(1000, array_unique($ids));
    }
}
