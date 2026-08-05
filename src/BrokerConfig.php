<?php

declare(strict_types=1);

namespace Order\KafkaMessaging;

/**
 * Connection + auth settings, translated to raw librdkafka keys in one place.
 *
 * Production (AWS MSK) needs SASL_SSL; local runs PLAINTEXT. Keeping the
 * mapping here means a driver never hand-rolls security keys and a service
 * only sets env values.
 */
final class BrokerConfig
{
    public const PROTOCOL_PLAINTEXT = 'PLAINTEXT';
    public const PROTOCOL_SSL = 'SSL';
    public const PROTOCOL_SASL_PLAINTEXT = 'SASL_PLAINTEXT';
    public const PROTOCOL_SASL_SSL = 'SASL_SSL';

    /**
     * @param array<string, string> $extra raw librdkafka overrides, applied last
     */
    public function __construct(
        public readonly string $brokers,
        public readonly string $securityProtocol = self::PROTOCOL_PLAINTEXT,
        public readonly ?string $saslMechanism = null,   // PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI | OAUTHBEARER
        public readonly ?string $saslUsername = null,
        public readonly ?string $saslPassword = null,
        public readonly ?string $sslCaLocation = null,
        public readonly ?string $sslCertificateLocation = null,
        public readonly ?string $sslKeyLocation = null,
        public readonly ?string $sslKeyPassword = null,
        public readonly array $extra = [],
    ) {
        if ($this->brokers === '') {
            throw new \InvalidArgumentException('brokers must not be empty.');
        }

        $known = [self::PROTOCOL_PLAINTEXT, self::PROTOCOL_SSL, self::PROTOCOL_SASL_PLAINTEXT, self::PROTOCOL_SASL_SSL];
        if (!in_array($this->securityProtocol, $known, true)) {
            throw new \InvalidArgumentException(
                "Unknown security.protocol '{$this->securityProtocol}'. Expected one of: " . implode(', ', $known)
            );
        }

        if ($this->usesSasl()) {
            // Fail at construction, not on the first publish in production.
            if (($this->saslMechanism ?? '') === '') {
                throw new \InvalidArgumentException("security.protocol {$this->securityProtocol} requires a SASL mechanism.");
            }
            $needsCredentials = in_array($this->saslMechanism, ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'], true);
            if ($needsCredentials && (($this->saslUsername ?? '') === '' || ($this->saslPassword ?? '') === '')) {
                throw new \InvalidArgumentException("SASL mechanism {$this->saslMechanism} requires a username and password.");
            }
        }
    }

    /**
     * @param array<string, mixed> $config a service's config('kafka-messaging') array
     */
    public static function fromArray(array $config): self
    {
        $security = (array) ($config['security'] ?? []);

        return new self(
            brokers: (string) ($config['brokers'] ?? '127.0.0.1:9092'),
            securityProtocol: (string) ($security['protocol'] ?? self::PROTOCOL_PLAINTEXT),
            saslMechanism: self::nullableString($security['sasl_mechanism'] ?? null),
            saslUsername: self::nullableString($security['sasl_username'] ?? null),
            saslPassword: self::nullableString($security['sasl_password'] ?? null),
            sslCaLocation: self::nullableString($security['ssl_ca_location'] ?? null),
            sslCertificateLocation: self::nullableString($security['ssl_certificate_location'] ?? null),
            sslKeyLocation: self::nullableString($security['ssl_key_location'] ?? null),
            sslKeyPassword: self::nullableString($security['ssl_key_password'] ?? null),
            extra: array_map('strval', (array) ($config['extra_conf'] ?? [])),
        );
    }

    public function usesSasl(): bool
    {
        return str_starts_with($this->securityProtocol, 'SASL');
    }

    /**
     * Raw librdkafka key/value pairs for this connection.
     *
     * @return array<string, string>
     */
    public function toLibrdKafkaConf(): array
    {
        $conf = ['bootstrap.servers' => $this->brokers];

        if ($this->securityProtocol !== self::PROTOCOL_PLAINTEXT) {
            $conf['security.protocol'] = $this->securityProtocol;
        }

        if ($this->usesSasl()) {
            $conf['sasl.mechanisms'] = (string) $this->saslMechanism;
            if ($this->saslUsername !== null) {
                $conf['sasl.username'] = $this->saslUsername;
            }
            if ($this->saslPassword !== null) {
                $conf['sasl.password'] = $this->saslPassword;
            }
        }

        foreach ([
            'ssl.ca.location' => $this->sslCaLocation,
            'ssl.certificate.location' => $this->sslCertificateLocation,
            'ssl.key.location' => $this->sslKeyLocation,
            'ssl.key.password' => $this->sslKeyPassword,
        ] as $key => $value) {
            if ($value !== null) {
                $conf[$key] = $value;
            }
        }

        return array_merge($conf, $this->extra);
    }

    /**
     * Safe for logging: credentials replaced with a marker.
     *
     * @return array<string, string>
     */
    public function toRedactedArray(): array
    {
        $conf = $this->toLibrdKafkaConf();
        foreach (['sasl.password', 'ssl.key.password'] as $secret) {
            if (isset($conf[$secret])) {
                $conf[$secret] = '***';
            }
        }

        return $conf;
    }

    private static function nullableString(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }
        $value = trim((string) $value);

        return $value === '' ? null : $value;
    }
}
