# order/kafka-messaging

المكتبة المشتركة لاستبدال SNS + الـ apigateway relay بـ Kafka — قلب **المرحلة 0** في [خطة الهجرة](../kafka-test-main/04-migration-runbook.md). Pure PHP 8.1+ (اعتماد وحيد: `psr/log`)، الـ core بيشتغل ويتاختبر **من غير broker ومن غير ext-rdkafka**.

> **الحالة:** الـ core كامل ومُراجَع عدائيًا — 46 tests / 316 assertions. الـ rdkafka driver الحقيقي = الخطوة الجاية (محتاج `ext-rdkafka`).

---

## المنتِج (بديل `SendDataToServiceWithSNSListener`)

```php
use Order\KafkaMessaging\KafkaProducer;

// في مسار طلب العميل استخدم safePublish — عمرها ما ترمي exception
// (فشل الرسالة لا يفشل الأوردر — وSNS يظل مصدر الحقيقة أثناء الـ dual-write):
$producer->safePublish(
    topic: 'order.events',
    eventType: 'OrderCreatedEvent',
    tenant: $tenant,                    // إلزامي — ده الـ partition key
    data: $payload,
    key: "{$tenant}:{$orderId}",       // للـ topics الحرجة (لازم يبدأ بالـ tenant)
);
```

الظرف بيتبني تلقائيًا: `message_id` (UUIDv7) + `event_time` + `trace_id` + `source_service` + `schema_version` في الـ headers، والحمولة `{tenant, data}` متوافقة مع شكل SNS الحالي.

## المستهلك (بديل `SNSController::__invoke` + الـ Redis worker)

```php
use Order\KafkaMessaging\KafkaConsumer;

KafkaConsumer::group('svc-payment')
    ->driver($driver)                          // rdkafka لاحقًا / InMemory للاختبارات
    ->subscribe(['order.events'])
    ->onEvent('OrderCreatedEvent', CheckPaymentHandler::class)
    ->withIdempotency($store)                  // إلزامي قبل أي cutover
    ->withRetryAndDlq($producerDriver)         // svc-payment.order.events.retry / .dlq
    ->logger($logger)
    ->run(exitOnIdle: false);                  // daemon mode + stop() للـ SIGTERM
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

```php
// جدول processed_messages (03-message-envelope.md §3) — migration في كل خدمة:
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

```php
// نفس الحدث المنطقي = نفس message_id على المسارين → الـ dedupe يشتغل عبرهما
$envelope = SnsBridge::toEnvelope($subject, $decodedSnsMessage, 'service-order', $snsMessageId, $snsTimestamp);
```

بيرفض بصرامة أي رسالة من publisher غير مُوحَّد (بلا `tenant` أو `data`) — راجع المرحلة 0.5 في الـ runbook.

## تكامل Laravel

Auto-discovery عبر `KafkaMessagingServiceProvider`. لكل خدمة:

```env
KAFKA_SOURCE_SERVICE=service-order   # إلزامي
KAFKA_MESSAGING_DRIVER=log           # shadow mode — الافتراضي الآمن
KAFKA_DUAL_WRITE=false               # يتقلب true في المرحلة 1
KAFKA_CONSUMER_GROUP=svc-order
```

`driver=log` بينشر في اللوج بس (اللي *كان* هيتبعت للـ broker بالحرف) — أول خطوة رصد بدون أي أثر.

## تشغيل الاختبارات

```bash
composer install && composer test
```
