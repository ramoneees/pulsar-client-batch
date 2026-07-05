package com.example;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

class ConfigTest {

  @Test
  void fromMap_appliesDefaultsWhenEnvMissing() {
    Config config = Config.fromMap(Map.of());
    assertEquals("pulsar://localhost:6650", config.serviceUrl());
    assertEquals("persistent://public/default/my-topic", config.topic());
    assertEquals("my-subscription", config.subscription());
    assertEquals(SubscriptionType.Shared, config.subscriptionType());
    assertEquals(SubscriptionInitialPosition.Earliest, config.initialPosition());
    assertEquals(10_000L, config.maxMessages());
    assertEquals(Duration.ofMillis(10_000L), config.idleTimeout());
  }

  @Test
  void fromMap_appliesProvidedValues() {
    Map<String, String> env = new HashMap<>();
    env.put(Config.ENV_SERVICE_URL, "pulsar+ssl://broker.example.com:6651");
    env.put(Config.ENV_TOPIC, "persistent://tenant/ns/foo");
    env.put(Config.ENV_SUBSCRIPTION, "sub-x");
    env.put(Config.ENV_SUBSCRIPTION_TYPE, "Exclusive");
    env.put(Config.ENV_INITIAL_POSITION, "Latest");
    env.put(Config.ENV_MAX_MESSAGES, "4242");
    env.put(Config.ENV_RECEIVE_TIMEOUT_MS, "30000");

    Config config = Config.fromMap(env);

    assertEquals("pulsar+ssl://broker.example.com:6651", config.serviceUrl());
    assertEquals("persistent://tenant/ns/foo", config.topic());
    assertEquals("sub-x", config.subscription());
    assertEquals(SubscriptionType.Exclusive, config.subscriptionType());
    assertEquals(SubscriptionInitialPosition.Latest, config.initialPosition());
    assertEquals(4242L, config.maxMessages());
    assertEquals(Duration.ofMillis(30_000L), config.idleTimeout());
  }

  @Test
  void parseInitialPosition_acceptsEarliestAndLatestCaseInsensitive() {
    assertEquals(SubscriptionInitialPosition.Earliest, Config.parseInitialPosition("earliest"));
    assertEquals(SubscriptionInitialPosition.Latest, Config.parseInitialPosition("LATEST"));
    assertEquals(SubscriptionInitialPosition.Earliest, Config.parseInitialPosition("  Earliest  "));
  }

  @Test
  void parseInitialPosition_unknownThrows() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> Config.parseInitialPosition("middle"));
    assertTrue(ex.getMessage().contains("Earliest or Latest"));
    assertTrue(ex.getMessage().contains("middle"));
  }

  @Test
  void fromMap_blankValueFallsBackToDefault() {
    Map<String, String> env = new HashMap<>();
    env.put(Config.ENV_MAX_MESSAGES, "   ");
    Config config = Config.fromMap(env);
    assertEquals(10_000L, config.maxMessages());
  }

  @Test
  void fromMap_nonNumericMaxFallsBackToDefault() {
    Config config = Config.fromMap(Map.of(Config.ENV_MAX_MESSAGES, "not-a-number"));
    assertEquals(10_000L, config.maxMessages());
  }

  @Test
  void fromMap_zeroMaxMessagesThrows() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> Config.fromMap(Map.of(Config.ENV_MAX_MESSAGES, "0")));
    assertEquals("PULSAR_MAX_MESSAGES must be > 0", ex.getMessage());
  }

  @Test
  void fromMap_negativeMaxMessagesThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Config.fromMap(Map.of(Config.ENV_MAX_MESSAGES, "-5")));
  }

  @Test
  void fromMap_idleTimeoutBelow100msThrows() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> Config.fromMap(Map.of(Config.ENV_RECEIVE_TIMEOUT_MS, "99")));
    assertEquals("PULSAR_RECEIVE_TIMEOUT_MS must be >= 100", ex.getMessage());
  }

  @Test
  void parseType_acceptsAllFourTypesCaseInsensitive() {
    assertEquals(SubscriptionType.Exclusive, Config.parseType("exclusive"));
    assertEquals(SubscriptionType.Failover, Config.parseType("FAILOVER"));
    assertEquals(SubscriptionType.Shared, Config.parseType("Shared"));
    assertEquals(SubscriptionType.Key_Shared, Config.parseType("key_shared"));
    assertEquals(SubscriptionType.Key_Shared, Config.parseType("key-shared"));
    assertEquals(SubscriptionType.Shared, Config.parseType("  shared  "));
  }

  @Test
  void parseType_unknownValueThrows() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Config.parseType("bogus"));
    assertTrue(ex.getMessage().contains("bogus"));
    assertTrue(ex.getMessage().contains("Exclusive|Failover|Shared|Key_Shared"));
  }

  @Test
  void parseType_nullThrows() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Config.parseType(null));
    assertTrue(ex.getMessage().contains("got: null"));
  }
}
