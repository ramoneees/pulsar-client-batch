package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class DrainRunnerTest {

  /** Build a Config with the given cap and a 60s idle timeout (so idle never trips in tests). */
  private static Config configWithCap(long max) {
    return new Config(
        "pulsar://localhost:6650",
        "persistent://public/default/t",
        "sub",
        SubscriptionType.Exclusive,
        SubscriptionInitialPosition.Earliest,
        max,
        Duration.ofMillis(60_000));
  }

  /** Build a Config that will trip idle after the given number of "idle" nano-time ticks. */
  private static Config configWithIdle(long idleMs) {
    return new Config(
        "pulsar://localhost:6650",
        "persistent://public/default/t",
        "sub",
        SubscriptionType.Exclusive,
        SubscriptionInitialPosition.Earliest,
        100_000L,
        Duration.ofMillis(idleMs));
  }

  /** Create a Mockito mock of Messages containing n byte[] messages. */
  @SuppressWarnings("unchecked")
  private static Messages<byte[]> mockMessages(int n) {
    // Build all mocks FIRST, then stub. Calling mock() between when() and thenReturn()
    // leaves Mockito's stubbing context "unfinished" (UnfinishedStubbing exception).
    Messages<byte[]> msgs = mock(Messages.class, Answers.RETURNS_SMART_NULLS);
    List<Message<byte[]>> list = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Message<byte[]> m = (Message<byte[]>) mock(Message.class);
      list.add(m);
    }
    when(msgs.size()).thenReturn(n);
    when(msgs.iterator()).thenReturn(list.iterator());
    return msgs;
  }

  @Test
  void run_drainsExactlyToCapWhenBatchEqual() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> b1 = mockMessages(1000);
    Messages<byte[]> b2 = mockMessages(1000);
    Messages<byte[]> b3 = mockMessages(1000);
    Messages<byte[]> b4 = mockMessages(1000);
    Messages<byte[]> b5 = mockMessages(1000);
    Messages<byte[]> b6 = mockMessages(1000);
    Messages<byte[]> b7 = mockMessages(1000);
    Messages<byte[]> b8 = mockMessages(1000);
    Messages<byte[]> b9 = mockMessages(1000);
    Messages<byte[]> b10 = mockMessages(1000);
    when(consumer.batchReceive()).thenReturn(b1, b2, b3, b4, b5, b6, b7, b8, b9, b10);

    Config config = configWithCap(10_000);
    DrainRunner.Result result = new DrainRunner(consumer, config).run();

    assertEquals(10_000L, result.acked());
    assertEquals(DrainRunner.ExitReason.CAP_REACHED, result.reason());
    verify(consumer, times(10_000)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_truncatesLastBatchToCap() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> b1 = mockMessages(1000);
    Messages<byte[]> b2 = mockMessages(1000);
    Messages<byte[]> b3 = mockMessages(1000);
    when(consumer.batchReceive()).thenReturn(b1, b2, b3);

    Config config = configWithCap(2_500);
    DrainRunner.Result result = new DrainRunner(consumer, config).run();

    assertEquals(2_500L, result.acked());
    assertEquals(DrainRunner.ExitReason.CAP_REACHED, result.reason());
    verify(consumer, times(2_500)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_stopsOnIdleTimeout() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> b1 = mockMessages(100);
    Messages<byte[]> b2 = mockMessages(0);
    when(consumer.batchReceive()).thenReturn(b1, b2);

    AtomicLong tick = new AtomicLong(0);
    DrainRunner.Clock clock =
        () -> {
          long v = tick.get();
          tick.addAndGet(10_000_000_000L);
          return v;
        };

    Config config = configWithIdle(5_000);
    DrainRunner.Result result = new DrainRunner(consumer, config, Processor.NOOP, clock).run();

    assertEquals(100L, result.acked());
    assertEquals(DrainRunner.ExitReason.IDLE_DRAINED, result.reason());
    verify(consumer, times(100)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_emptyTopicFromStartExitsAtIdleTimeoutWithZeroAcks() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> empty = mockMessages(0);
    when(consumer.batchReceive()).thenReturn(empty);

    AtomicLong tick = new AtomicLong(0);
    DrainRunner.Clock clock =
        () -> {
          long v = tick.get();
          tick.addAndGet(20_000_000_000L);
          return v;
        };

    Config config = configWithIdle(5_000);
    DrainRunner.Result result = new DrainRunner(consumer, config, Processor.NOOP, clock).run();

    assertEquals(0L, result.acked());
    assertEquals(DrainRunner.ExitReason.IDLE_DRAINED, result.reason());
    verify(consumer, times(0)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_singleMessageCap() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> big = mockMessages(50);
    when(consumer.batchReceive()).thenReturn(big);

    Config config = configWithCap(1);
    DrainRunner.Result result = new DrainRunner(consumer, config).run();

    assertEquals(1L, result.acked());
    verify(consumer, times(1)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_propagatesPulsarClientException() throws Exception {
    Consumer<byte[]> consumer = mock(Consumer.class);
    when(consumer.batchReceive())
        .thenThrow(new PulsarClientException.AlreadyClosedException("closed"));

    Config config = configWithCap(100);
    assertThrows(
        PulsarClientException.class,
        () -> new DrainRunner(consumer, config).run());
  }

  // --- Processor integration tests ---

  @Test
  void run_processorFailedNacksAndStopsBatchImmediately() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    // 5-message batch. Processor fails on the 3rd message.
    Messages<byte[]> batch = mockMessages(5);
    when(consumer.batchReceive()).thenReturn(batch);

    // Track which messages we see by their position in the iteration. The mock Messages iterator
    // returns the same Message instances each call, so we can use identity.
    List<Message<byte[]>> seen = new ArrayList<>();
    Processor<byte[]> failsThird =
        msg -> {
          seen.add(msg);
          return seen.size() == 3 ? ProcessorResult.FAILED : ProcessorResult.OK;
        };

    Config config = configWithCap(100); // cap won't trigger; failure stops the run
    DrainRunner.Result result = new DrainRunner(consumer, config, failsThird).run();

    // First two acked, third nacked, run stopped.
    assertEquals(2L, result.acked());
    assertEquals(DrainRunner.ExitReason.PROCESSING_FAILED, result.reason());
    verify(consumer, times(2)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
    verify(consumer, times(1)).negativeAcknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_processorPoisonTreatedAsFailure() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> batch = mockMessages(3);
    when(consumer.batchReceive()).thenReturn(batch);

    Processor<byte[]> allPoison = msg -> ProcessorResult.POISON;

    Config config = configWithCap(100);
    DrainRunner.Result result = new DrainRunner(consumer, config, allPoison).run();

    assertEquals(0L, result.acked());
    assertEquals(DrainRunner.ExitReason.PROCESSING_FAILED, result.reason());
    verify(consumer, never()).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
    verify(consumer, times(1)).negativeAcknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_processorExceptionTreatedAsFailureAndNacks() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> batch = mockMessages(2);
    when(consumer.batchReceive()).thenReturn(batch);

    Processor<byte[]> throwsOnFirst =
        msg -> {
          throw new RuntimeException("downstream DB down");
        };

    Config config = configWithCap(100);
    DrainRunner.Result result = new DrainRunner(consumer, config, throwsOnFirst).run();

    assertEquals(0L, result.acked());
    assertEquals(DrainRunner.ExitReason.PROCESSING_FAILED, result.reason());
    verify(consumer, never()).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
    verify(consumer, times(1)).negativeAcknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_processorOkContinuesUntilCap() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> b1 = mockMessages(500);
    Messages<byte[]> b2 = mockMessages(500);
    when(consumer.batchReceive()).thenReturn(b1, b2);

    Processor<byte[]> countingNoop = msg -> ProcessorResult.OK;

    Config config = configWithCap(1_000);
    DrainRunner.Result result = new DrainRunner(consumer, config, countingNoop).run();

    assertEquals(1_000L, result.acked());
    assertEquals(DrainRunner.ExitReason.CAP_REACHED, result.reason());
    verify(consumer, times(1_000)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
    verify(consumer, never()).negativeAcknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }
}
