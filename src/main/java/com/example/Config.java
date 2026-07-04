package com.example;

import java.time.Duration;
import java.util.Map;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Immutable resolved configuration for the consumer.
 *
 * <p>Use {@link #fromEnv()} to read from {@code System.getenv()}, or {@link #fromMap(Map)} for
 * testing with an injected environment.
 */
public record Config(
    String serviceUrl,
    String topic,
    String subscription,
    SubscriptionType subscriptionType,
    long maxMessages,
    Duration idleTimeout) {

  /** Env var name constants. */
  public static final String ENV_SERVICE_URL = "PULSAR_SERVICE_URL";
  public static final String ENV_TOPIC = "PULSAR_TOPIC";
  public static final String ENV_SUBSCRIPTION = "PULSAR_SUBSCRIPTION";
  public static final String ENV_SUBSCRIPTION_TYPE = "PULSAR_SUBSCRIPTION_TYPE";
  public static final String ENV_MAX_MESSAGES = "PULSAR_MAX_MESSAGES";
  public static final String ENV_RECEIVE_TIMEOUT_MS = "PULSAR_RECEIVE_TIMEOUT_MS";

  public static final long DEFAULT_MAX_MESSAGES = 10000L;
  public static final long DEFAULT_IDLE_TIMEOUT_MS = 10000L;

  /** Read configuration from the process environment. */
  public static Config fromEnv() {
    return fromMap(System.getenv());
  }

  /** Read configuration from an explicit map. Package-visible for tests. */
  static Config fromMap(Map<String, String> env) {
    String url = envOr(env, ENV_SERVICE_URL, "pulsar://localhost:6650");
    String topic = envOr(env, ENV_TOPIC, "persistent://public/default/my-topic");
    String sub = envOr(env, ENV_SUBSCRIPTION, "my-subscription");
    SubscriptionType type = parseType(envOr(env, ENV_SUBSCRIPTION_TYPE, "Shared"));
    long max = parseLong(envOr(env, ENV_MAX_MESSAGES, "10000"), DEFAULT_MAX_MESSAGES);
    long idleMs = parseLong(envOr(env, ENV_RECEIVE_TIMEOUT_MS, "10000"), DEFAULT_IDLE_TIMEOUT_MS);
    if (max <= 0) {
      throw new IllegalArgumentException("PULSAR_MAX_MESSAGES must be > 0");
    }
    if (idleMs < 100) {
      throw new IllegalArgumentException("PULSAR_RECEIVE_TIMEOUT_MS must be >= 100");
    }
    return new Config(url, topic, sub, type, max, Duration.ofMillis(idleMs));
  }

  /** Parse a case-insensitive subscription type name. Throws on unknown values. */
  public static SubscriptionType parseType(String raw) {
    if (raw == null) {
      throw new IllegalArgumentException(
          "PULSAR_SUBSCRIPTION_TYPE must be one of Exclusive|Failover|Shared|Key_Shared (got: null)");
    }
    return switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
      case "exclusive" -> SubscriptionType.Exclusive;
      case "failover" -> SubscriptionType.Failover;
      case "shared" -> SubscriptionType.Shared;
      case "key_shared", "key-shared" -> SubscriptionType.Key_Shared;
      default -> throw new IllegalArgumentException(
          "PULSAR_SUBSCRIPTION_TYPE must be one of Exclusive|Failover|Shared|Key_Shared (got: "
              + raw + ")");
    };
  }

  private static String envOr(Map<String, String> env, String name, String fallback) {
    String v = env.get(name);
    return (v == null || v.isBlank()) ? fallback : v.trim();
  }

  private static long parseLong(String raw, long fallback) {
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}
