# order/kafka-messaging

المكتبة المشتركة لاستبدال SNS + الـ apigateway relay بـ Kafka — قلب **المرحلة 0** في [خطة الهجرة](../kafka-test-main/04-migration-runbook.md). Pure PHP 8.1+ (اعتماد وحيد: `psr/log`)، الـ core بيشتغل ويتاختبر **من غير broker ومن غير ext-rdkafka**.

> **الحالة:** الـ core كامل ومُراجَع عدائيًا — 46 tests / 316 assertions. الـ rdkafka driver الحقيقي = الخطوة الجاية (محتاج `ext-rdkafka`).

---

## المنتِج (بديل `SendDataToServiceWithSNSListener`)

في مسار طلب العميل استخدم `safePublish` — عمرها ما ترمي exception (فشل الرسالة لا يفشل الأوردر، وSNS يظل مصدر الحقيقة أثناء الـ dual-write). الـ `tenant` إلزامي لأنه الـ partition key، والـ `key` المركّب للـ topics الحرجة لازم يبدأ بالـ tenant:

```php
use Order\KafkaMessaging\KafkaProducer;

$producer->safePublish(
    topic: 'order.events',
    eventType: 'OrderCreatedEvent',
    tenant: $tenant,                    // mandatory — the partition key
    data: $payload,
    key: "{$tenant}:{$orderId}",       // critical topics: must start with tenant
);
```

الظرف بيتبني تلقائيًا: `message_id` (UUIDv7) + `event_time` + `trace_id` + `source_service` + `schema_version` في الـ headers، والحمولة `{tenant, data}` متوافقة مع شكل SNS الحالي.

## المستهلك (بديل `SNSController::__invoke` + الـ Redis worker)

الـ idempotency **إلزامي قبل أي cutover**، والـ retry/DLQ بيتسموا تلقائيًا `svc-payment.order.events.retry` / `.dlq`، و`run(exitOnIdle: false)` هو وضع الـ daemon (مع `stop()` للـ SIGTERM):

```php
use Order\KafkaMessaging\KafkaConsumer;

KafkaConsumer::group('svc-payment')
    ->driver($driver)                          // RdKafkaConsumerDriver in prod, InMemory in tests
    ->subscribe(['order.events'])
    ->onEvent('OrderCreatedEvent', CheckPaymentHandler::class)
    ->withIdempotency($store)
    ->withRetryAndDlq($producerDriver)
    ->logger($logger)
    ->run(exitOnIdle: false);
```

## سياسة الفشل (مُراجعة ومقصودة)

| الموقف | السلوك |
|---|---|
| handler فشل | 3 محاولات فورية ← retry topic (rounds بـ 5s/1m/10m) ← DLQ |
| رسالة مشوّهة (poison) | DLQ فورًا — الـ partition عمره ما يتسدّ |
| نشر الـ retry/DLQ نفسه فشل | `EscalationFailed` → **مفيش commit** → Kafka يعيد التسليم (صفر فقدان صامت) |
| مفيش retry/DLQ متوصّل | `Dropped` — خسارة **معلنة** في اللوج والعدادات، مش متنكرة |
| الـ idempotency store واقع | fail-open مع warning — المنصة متتوقفش عشان Redis عطس |
| handler نجح والـ markProcessed فشل | النجاح بيثبت — الـ handler **عمره ما يتعاد** من جوه اللوب |

كل اللوجات بسياق ملفوف في `data` (قاعدة GELF).

## الـ Idempotency

جدول `processed_messages` (مواصفة `03-message-envelope.md §3` في خطة الهجرة) — الـ migration دي بتتعمل في كل خدمة:

```php
Schema::create('processed_messages', function (Blueprint $t) {
    $t->string('consumer_group', 191);
    $t->string('message_id', 191);
    $t->string('processed_at', 32);
    $t->primary(['consumer_group', 'message_id']);
});

$store = new PdoIdempotencyStore(DB::connection()->getPdo());
```

آمن للتوازي (duplicate-key = نجاح). **ملحوظة at-least-once:** نافذة الـ crash بين نجاح الـ handler والتسجيل غير قابلة للقفل مكتبيًا — الـ handlers لازم تستحمل duplicate واحد، واللي عايز ضمان أقوى يكتب مفتاح الـ dedupe جوه transaction الـ handler نفسه (inbox pattern).

## جسر SNS (للـ dual-write)

نفس الحدث المنطقي = نفس `message_id` على المسارين → الـ dedupe بيشتغل عبرهما:

```php
$envelope = SnsBridge::toEnvelope($subject, $decodedSnsMessage, 'service-order', $snsMessageId, $snsTimestamp);
```

بيرفض بصرامة أي رسالة من publisher غير مُوحَّد (بلا `tenant` أو `data`) — راجع المرحلة 0.5 في الـ runbook.

## الأمان (SASL/SSL)

محليًا `PLAINTEXT`، وفي الإنتاج (AWS MSK) `SASL_SSL` — كله من الـ env، و`BrokerConfig` بيترجمها لمفاتيح librdkafka ويرفض أي إعداد ناقص **وقت الإقلاع** مش عند أول نشرة:

```env
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=order
KAFKA_SASL_PASSWORD=***
KAFKA_SSL_CA_LOCATION=/etc/ssl/certs/ca.pem
```

الـ passwords بتتحجب تلقائيًا في أي لوج (`toRedactedArray`).

## الـ Replay (معيار نجاح في خطة الهجرة)

بيقرا بـ consumer group مؤقتة — **عمره ما يلمس offsets المجموعة الحية**:

```bash
php artisan kafka:replay order.events --since="2026-08-05 10:00" --event=OrderCreatedEvent
php artisan kafka:replay svc-payment.order.events.dlq --to=order.events --confirm
```

من غير `--confirm` بيبقى معاينة بس. وعند إعادة الحقن بيحافظ على `message_id` (فالمستهلك بيتخطى اللي عالجه قبل كده) وبيشيل headers الـ retry عشان الرسالة تدخل نضيفة.

## الاختبارات (KafkaFake)

```php
$fake = KafkaFake::bind();

// ... شغّل الفلو ...

$fake->assertPublished('OrderCreatedEvent')
     ->assertPublishedOn('order.events', 'OrderCreatedEvent', fn ($m) => $m['tenant'] === 'jabourih')
     ->assertPublishedTimes('OrderCreatedEvent', 1)
     ->assertNotPublished('OrderCompletedEvent')
     ->assertAllHaveTenant();
```

## المراقبة (metrics + callbacks)

```php
$consumer->withMetrics($prometheusExporter)   // ConsumerMetricsInterface
         ->before(fn ($msg) => Log::debug('in'))
         ->after(fn ($msg, $outcome) => Log::debug($outcome->value));
```

أي فشل في طبقة المراقبة **مش بيوقف الاستهلاك** — بيتسجل warning وبس.

## تكامل Laravel

Auto-discovery عبر `KafkaMessagingServiceProvider`. لكل خدمة:

`KAFKA_SOURCE_SERVICE` إلزامي (الـ provider بيرفض يشتغل من غيره)، و`driver=log` هو الـ shadow mode الافتراضي الآمن، و`KAFKA_DUAL_WRITE` بيتقلب `true` في المرحلة 1:

```env
KAFKA_SOURCE_SERVICE=service-order
KAFKA_MESSAGING_DRIVER=log
KAFKA_DUAL_WRITE=false
KAFKA_CONSUMER_GROUP=svc-order
```

`driver=log` بينشر في اللوج بس (اللي *كان* هيتبعت للـ broker بالحرف) — أول خطوة رصد بدون أي أثر.

## تشغيل الاختبارات

```bash
composer install && composer test
```
