package com.example;

/**
 * Outcome of processing a single message, returned by {@link Processor}.
 *
 * <ul>
 *   <li>{@link #OK} — message processed successfully. The drain loop acks it and continues.
 *   <li>{@link #FAILED} — transient failure (downstream timeout, DB error, etc.). The drain loop
 *       nacks the message, stops the batch, and ends the run. The broker re-delivers the message
 *       on the next run because the subscription cursor is not advanced past it.
 *   <li>{@link #POISON} — permanent failure (malformed payload, schema mismatch, etc.). Behaves
 *       like {@link #FAILED} in this build (no DLQ wired). A real deployment would route these to
 *       a dead-letter topic instead of redelivering forever.
 * </ul>
 */
public enum ProcessorResult {
  OK,
  FAILED,
  POISON
}
