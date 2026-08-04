<?php

declare(strict_types=1);

namespace Order\KafkaMessaging\Tests;

use Order\KafkaMessaging\Envelope;
use Order\KafkaMessaging\Exceptions\InvalidEnvelopeException;
use PHPUnit\Framework\TestCase;

final class EnvelopeTest extends TestCase
{
    public function testCreateBuildsFullEnvelopeWithGeneratedIds(): void
    {
        $e = Envelope::create('jabourih', ['order_id' => 'ord_1'], 'OrderCreatedEvent', 'service-order');

        $this->assertSame('jabourih', $e->tenant);
        $this->assertSame('OrderCreatedEvent', $e->eventType);
        $this->assertSame('service-order', $e->sourceService);
        $this->assertSame(1, $e->schemaVersion);
        $this->assertMatchesRegularExpression('/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/', $e->messageId);
        $this->assertNotSame('', $e->traceId);
        $this->assertNotSame('', $e->eventTime);
    }

    public function testTenantIsMandatoryPhase05Enforcement(): void
    {
        $this->expectException(InvalidEnvelopeException::class);
        $this->expectExceptionMessageMatches('/tenant/');

        Envelope::create('   ', ['x' => 1], 'CustomerDataUpdated', 'service-user');
    }

    public function testEventTypeIsMandatory(): void
    {
        $this->expectException(InvalidEnvelopeException::class);
        Envelope::create('jabourih', [], '', 'service-order');
    }

    public function testPayloadIsBackwardCompatibleSnsShape(): void
    {
        $e = Envelope::create('jabourih', ['order_id' => 'ord_9931', 'total' => 74.5], 'OrderCreatedEvent', 'service-order');

        $payload = json_decode($e->toPayload(), true);
        $this->assertSame(['tenant', 'data'], array_keys($payload));
        $this->assertSame('jabourih', $payload['tenant']);
        $this->assertSame('ord_9931', $payload['data']['order_id']);
    }

    public function testHeadersCarryAllMetadata(): void
    {
        $e = Envelope::create('jabourih', [], 'OrderCreatedEvent', 'service-order', traceId: 'trace-1');
        $h = $e->toHeaders();

        $this->assertSame('OrderCreatedEvent', $h['event_type']);
        $this->assertSame('jabourih', $h['tenant']);
        $this->assertSame('service-order', $h['source_service']);
        $this->assertSame('1', $h['schema_version']);
        $this->assertSame('trace-1', $h['trace_id']);
        $this->assertArrayHasKey('message_id', $h);
        $this->assertArrayHasKey('event_time', $h);
    }

    public function testRoundTripThroughConsumedMessage(): void
    {
        $original = Envelope::create('jabourih', ['a' => 'ب'], 'OrderCreatedEvent', 'service-order');

        $rebuilt = Envelope::fromConsumed($original->toHeaders(), $original->toPayload());

        $this->assertSame($original->tenant, $rebuilt->tenant);
        $this->assertSame($original->data, $rebuilt->data);
        $this->assertSame($original->messageId, $rebuilt->messageId);
        $this->assertSame($original->traceId, $rebuilt->traceId);
    }

    public function testFromConsumedFallsBackToPayloadTenantForLegacyMessages(): void
    {
        // A message produced by a not-yet-migrated publisher: headers only
        // carry event_type; tenant lives in the payload body (SNS-era shape).
        $rebuilt = Envelope::fromConsumed(
            ['event_type' => 'OrderCreatedEvent'],
            '{"tenant":"jabourih","data":{"order_id":"x"}}'
        );

        $this->assertSame('jabourih', $rebuilt->tenant);
        $this->assertSame('', $rebuilt->messageId, 'legacy message has no message_id');
    }

    public function testFromConsumedRejectsTenantlessMessage(): void
    {
        $this->expectException(InvalidEnvelopeException::class);
        Envelope::fromConsumed(['event_type' => 'CustomerDataUpdated'], '{"data":{"id":1}}');
    }

    public function testFromConsumedRejectsInvalidJson(): void
    {
        $this->expectException(InvalidEnvelopeException::class);
        Envelope::fromConsumed(['event_type' => 'X', 'tenant' => 'jabourih'], '{not json');
    }
}
