package com.example;

import org.apache.pulsar.client.api.Message;

/**
 * Processes a single Pulsar message. The drain loop calls this for each message it pulls from the
 * batch, and uses the returned {@link ProcessorResult} to decide whether to ack, nack, or stop.
 *
 * <p>Implementations should be idempotent if at-least-once redelivery is unacceptable — a
 * {@link ProcessorResult#FAILED} result triggers redelivery of the same message on the next run.
 *
 * @param <T> message payload type (byte[] for the default raw consumer)
 */
@FunctionalInterface
public interface Processor<T> {
  ProcessorResult process(Message<T> message) throws Exception;

  /** A no-op processor that always succeeds. Useful for tests and as a default. */
  Processor<byte[]> NOOP = message -> ProcessorResult.OK;
}
