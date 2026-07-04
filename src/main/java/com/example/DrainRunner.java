package com.example;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Drain loop extracted from the main entrypoint so it can be unit-tested with a mocked
 * {@link Consumer}.
 *
 * <p>The loop calls {@link Consumer#batchReceive()} repeatedly and acks messages one by one
 * (Shared-safe). It stops when EITHER the cumulative cap is reached (truncated exactly, never
 * exceeded) OR the topic has been idle for at least {@link Config#idleTimeout()}.
 *
 * <p>An injectable {@link Clock} lets tests advance time without {@code Thread.sleep}.
 */
public final class DrainRunner {

  /** Nano-time provider. Default uses {@link System#nanoTime()}. */
  @FunctionalInterface
  public interface Clock {
    long nanoTime();

    static Clock system() {
      return System::nanoTime;
    }
  }

  private final Consumer<byte[]> consumer;
  private final Config config;
  private final Clock clock;

  public DrainRunner(Consumer<byte[]> consumer, Config config) {
    this(consumer, config, Clock.system());
  }

  DrainRunner(Consumer<byte[]> consumer, Config config, Clock clock) {
    this.consumer = consumer;
    this.config = config;
    this.clock = clock;
  }

  /**
   * Run the drain loop. Returns the total number of messages acked.
   *
   * @throws PulsarClientException if the consumer throws
   */
  public long run() throws PulsarClientException {
    long startTime = clock.nanoTime();
    long lastMessageNanos = startTime;
    AtomicLong total = new AtomicLong(0);
    long batchIndex = 0;

    while (total.get() < config.maxMessages()) {
      long remaining = config.maxMessages() - total.get();
      Messages<byte[]> messages = consumer.batchReceive();
      batchIndex++;

      if (messages.size() == 0) {
        long idleMs = (clock.nanoTime() - lastMessageNanos) / 1_000_000L;
        if (idleMs >= config.idleTimeout().toMillis()) {
          break;
        }
        continue;
      }

      // Cap-aware truncation: never ack or count beyond the user cap.
      long toTake = Math.min(messages.size(), remaining);
      long taken = 0;
      for (Message<byte[]> msg : messages) {
        if (taken >= toTake) {
          break;
        }
        consumer.acknowledge(msg);
        taken++;
      }
      total.addAndGet(taken);
      lastMessageNanos = clock.nanoTime();
      Thread.yield();
    }

    return total.get();
  }
}
