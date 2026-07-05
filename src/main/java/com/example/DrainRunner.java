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
 * <p>The loop calls {@link Consumer#batchReceive()} repeatedly. For each message it invokes the
 * injected {@link Processor}:
 *
 * <ul>
 *   <li>{@link ProcessorResult#OK} → {@link Consumer#acknowledge(Message)} and continue.
 *   <li>{@link ProcessorResult#FAILED} or {@link ProcessorResult#POISON} →
 *       {@link Consumer#negativeAcknowledge(Message)}, then stop the run immediately. The broker
 *       re-delivers the failed message on the next run because the subscription cursor is not
 *       advanced past an unacked message.
 * </ul>
 *
 * <p>The run stops when ANY of:
 *
 * <ul>
 *   <li>the cumulative cap is reached (truncated exactly, never exceeded),
 *   <li>the topic has been idle for at least {@link Config#idleTimeout()}, or
 *   <li>a message fails processing (nacked, will be redelivered next run).
 * </ul>
 *
 * <p>Subscription cursor durability: acks (individual or cumulative) are persisted by the broker
 * in BookKeeper. The same subscription name across JVM runs resumes from the last acked position
 * automatically — no local cursor file is needed.
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

  /** Why the run ended. Useful for logging and exit-code decisions in the caller. */
  public enum ExitReason {
    CAP_REACHED,
    IDLE_DRAINED,
    PROCESSING_FAILED
  }

  /** Result of a drain run. */
  public record Result(long acked, ExitReason reason) {}

  private final Consumer<byte[]> consumer;
  private final Config config;
  private final Processor<byte[]> processor;
  private final Clock clock;

  public DrainRunner(Consumer<byte[]> consumer, Config config) {
    this(consumer, config, Processor.NOOP, Clock.system());
  }

  public DrainRunner(
      Consumer<byte[]> consumer, Config config, Processor<byte[]> processor) {
    this(consumer, config, processor, Clock.system());
  }

  DrainRunner(
      Consumer<byte[]> consumer,
      Config config,
      Processor<byte[]> processor,
      Clock clock) {
    this.consumer = consumer;
    this.config = config;
    this.processor = processor;
    this.clock = clock;
  }

  /**
   * Run the drain loop.
   *
   * @return a {@link Result} with the acked count and the {@link ExitReason}
   * @throws PulsarClientException if the consumer throws
   */
  public Result run() throws PulsarClientException {
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
          return new Result(total.get(), ExitReason.IDLE_DRAINED);
        }
        continue;
      }

      // Cap-aware truncation: never ack or count beyond the user cap.
      long toTake = Math.min(messages.size(), remaining);
      long taken = 0;
      ExitReason failureReason = null;
      for (Message<byte[]> msg : messages) {
        if (taken >= toTake) {
          break;
        }
        ProcessorResult outcome;
        try {
          outcome = processor.process(msg);
        } catch (Exception e) {
          // Processor threw — treat as transient failure and nack for redelivery.
          System.err.println("Processor threw on message " + msg.getMessageId() + ": " + e);
          outcome = ProcessorResult.FAILED;
        }
        if (outcome == ProcessorResult.OK) {
          consumer.acknowledge(msg);
          taken++;
        } else {
          // FAILED or POISON: nack for redelivery and stop the batch immediately. Already-acked
          // prior messages stay acked (subscription cursor is per-message on Exclusive/Failover).
          consumer.negativeAcknowledge(msg);
          System.err.println(
              "Message " + msg.getMessageId() + " nacked (" + outcome + "); ending run.");
          failureReason = ExitReason.PROCESSING_FAILED;
          break;
        }
      }
      total.addAndGet(taken);
      lastMessageNanos = clock.nanoTime();
      if (failureReason != null) {
        return new Result(total.get(), failureReason);
      }
      Thread.yield();
    }

    return new Result(total.get(), ExitReason.CAP_REACHED);
  }
}
