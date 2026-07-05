package com.example;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.BatchReceivePolicy;

/**
 * Self-contained Apache Pulsar consumer.
 *
 * <p>Drains up to {@code PULSAR_MAX_MESSAGES} (default 10,000) messages from a single topic,
 * prints progress, then closes the consumer and client cleanly and exits.
 *
 * <p>Stops early when the topic is idle for {@code PULSAR_RECEIVE_TIMEOUT_MS}ms.
 *
 * <p>Exit codes: {@code 0} on normal completion, {@code 2} on configuration error,
 * {@code 3} on Pulsar runtime error.
 */
public final class PulsarConsumerApp {

  // Per-batch receive policy sizing.
  static final int BATCH_MAX_MESSAGES = 1000;
  static final long BATCH_MAX_BYTES = 10L * 1024 * 1024; // 10 MiB
  static final long BATCH_TIMEOUT_MS = 200L;

  public static void main(String[] args) {
    Config config;
    try {
      config = Config.fromEnv();
    } catch (IllegalArgumentException e) {
      System.err.println("Configuration error: " + e.getMessage());
      System.exit(2);
      return; // unreachable; satisfies analyzers
    }

    System.out.printf(
        "Connecting to Pulsar:%n  service-url : %s%n  topic       : %s%n  subscription: %s (%s)%n"
            + "  initial-pos : %s%n  max-messages: %d%n  idle-timeout: %d ms%n",
        config.serviceUrl(),
        config.topic(),
        config.subscription(),
        config.subscriptionType(),
        config.initialPosition(),
        config.maxMessages(),
        config.idleTimeout().toMillis());

    try (PulsarClient client = PulsarClient.builder().serviceUrl(config.serviceUrl()).build()) {
      Consumer<byte[]> consumer = buildConsumer(client, config);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> closeQuietly(consumer, "consumer")));
      try (consumer) {
        long startTime = System.nanoTime();
        DrainRunner.Result result = new DrainRunner(consumer, config).run();
        printSummary(result, System.nanoTime() - startTime);
      }
    } catch (PulsarClientException e) {
      // AlreadyClosedException is a subclass of PulsarClientException and is caught here.
      System.err.println("Pulsar error: " + e);
      e.printStackTrace(System.err);
      System.exit(3);
    }
  }

  static Consumer<byte[]> buildConsumer(PulsarClient client, Config config)
      throws PulsarClientException {
    BatchReceivePolicy policy =
        BatchReceivePolicy.builder()
            .maxNumMessages(BATCH_MAX_MESSAGES)
            .maxNumBytes((int) Math.min(BATCH_MAX_BYTES, Integer.MAX_VALUE))
            .timeout((int) BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();

    return client.newConsumer()
        .topic(config.topic())
        .subscriptionName(config.subscription())
        .subscriptionType(config.subscriptionType())
        .subscriptionInitialPosition(config.initialPosition())
        .batchReceivePolicy(policy)
        .subscribe();
  }

  private static void printSummary(DrainRunner.Result result, long elapsedNanos) {
    double elapsedSecs = elapsedNanos / 1_000_000_000.0;
    double avg = elapsedSecs > 0 ? result.acked() / elapsedSecs : 0;
    System.out.println("--------");
    System.out.printf(
        "Done. acked=%d, reason=%s, elapsed=%.3fs, avg=%.0f msgs/s%n",
        result.acked(), result.reason(), elapsedSecs, avg);
  }

  private static void printSummary(long total, long elapsedNanos) {
    double elapsedSecs = elapsedNanos / 1_000_000_000.0;
    double avg = elapsedSecs > 0 ? total / elapsedSecs : 0;
    System.out.println("--------");
    System.out.printf(
        "Done. total=%d, elapsed=%.3fs, avg=%.0f msgs/s%n", total, elapsedSecs, avg);
  }

  private static void closeQuietly(Closeable c, String what) {
    try {
      c.close();
    } catch (Exception e) {
      System.err.println("Error closing " + what + ": " + e.getMessage());
    }
  }

  private PulsarConsumerApp() {}
}
