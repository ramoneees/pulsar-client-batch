package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
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
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class DrainRunnerTest {

  /** Build a Config with the given cap and a 60s idle timeout (so idle never trips in tests). */
  private static Config configWithCap(long max) {
    return new Config(
        "pulsar://localhost:6650",
        "persistent://public/default/t",
        "sub",
        org.apache.pulsar.client.api.SubscriptionType.Shared,
        max,
        Duration.ofMillis(60_000));
  }

  /** Build a Config that will trip idle after the given number of "idle" nano-time ticks. */
  private static Config configWithIdle(long idleMs) {
    return new Config(
        "pulsar://localhost:6650",
        "persistent://public/default/t",
        "sub",
        org.apache.pulsar.client.api.SubscriptionType.Shared,
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
    // 10 batches of 1000 messages = 10_000 exactly = cap. Build all stubs up-front
    // (calling mockMessages() inside a when() chain triggers UnfinishedStubbing).
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
    long total = new DrainRunner(consumer, config).run();

    assertEquals(10_000L, total);
    verify(consumer, times(10_000)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_truncatesLastBatchToCap() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    // Cap is 2_500. First two batches give 1000 each (2_000 running).
    // Third batch gives 1000 but we should only ack 500, then exit.
    Messages<byte[]> b1 = mockMessages(1000);
    Messages<byte[]> b2 = mockMessages(1000);
    Messages<byte[]> b3 = mockMessages(1000);
    when(consumer.batchReceive()).thenReturn(b1, b2, b3);

    Config config = configWithCap(2_500);
    long total = new DrainRunner(consumer, config).run();

    assertEquals(2_500L, total);
    verify(consumer, times(2_500)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_stopsOnIdleTimeout() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> b1 = mockMessages(100); // batch 1: 100 msgs at t=0
    Messages<byte[]> b2 = mockMessages(0);   // batch 2: empty at t=10_000ms (idle)
    when(consumer.batchReceive()).thenReturn(b1, b2);

    // Clock: returns 0, then 10_000ms, then 10_000ms (idle >= 5_000ms threshold)
    AtomicLong tick = new AtomicLong(0);
    DrainRunner.Clock clock =
        () -> {
          long v = tick.get();
          tick.addAndGet(10_000_000_000L); // +10s in nanos each call
          return v;
        };

    Config config = configWithIdle(5_000);
    long total = new DrainRunner(consumer, config, clock).run();

    assertEquals(100L, total);
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
          tick.addAndGet(20_000_000_000L); // +20s each call, exceeds 5s idle
          return v;
        };

    Config config = configWithIdle(5_000);
    long total = new DrainRunner(consumer, config, clock).run();

    assertEquals(0L, total);
    verify(consumer, times(0)).acknowledge(org.mockito.ArgumentMatchers.<Message<byte[]>>any());
  }

  @Test
  void run_singleMessageCap() throws PulsarClientException {
    Consumer<byte[]> consumer = mock(Consumer.class);
    Messages<byte[]> big = mockMessages(50); // big batch, but cap=1
    when(consumer.batchReceive()).thenReturn(big);

    Config config = configWithCap(1);
    long total = new DrainRunner(consumer, config).run();

    assertEquals(1L, total);
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
}
