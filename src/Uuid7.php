<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

/**
 * RFC 9562 UUIDv7 generator: 48-bit unix-ms timestamp + version + 74 random
 * bits. Time-ordered, which is what makes message_id usable both for
 * idempotency and for rough ordering diagnostics — do not swap for UUIDv4.
 */
final class Uuid7
{
    public static function generate(?int $unixMs = null): string
    {
        $unixMs ??= (int) (microtime(true) * 1000);

        $time = str_pad(dechex($unixMs), 12, '0', STR_PAD_LEFT);

        $randA = substr(bin2hex(random_bytes(2)), 0, 3);   // 12 bits after the version nibble
        $randB = bin2hex(random_bytes(8));                 // 16 hex chars: 1 merges with variant, 3 + 12 fill the rest

        // variant bits: first hex digit of rand_b section forced to 8..b
        $variantDigit = dechex(0x8 | (hexdec($randB[0]) & 0x3));

        return sprintf(
            '%s-%s-7%s-%s%s-%s',
            substr($time, 0, 8),
            substr($time, 8, 4),
            $randA,
            $variantDigit,
            substr($randB, 1, 3),
            substr($randB, 4, 12)
        );
    }
}
