# pulsar-consumer

A self-contained Apache Pulsar Java client that drains **up to 10,000 messages per run** from a topic, prints progress, then exits cleanly.

Built with `pulsar-client` 4.0.11 (LTS), Java 17, and a shaded fat-jar output.

## Prerequisites

- **JDK 17+** (Pulsar 4.x client requires Java 17). Corretto 17 verified.
- **Apache Maven 3.6+**. If `mvn` is not installed:
  - macOS: `brew install mvn`
  - Linux: `apt install maven` / `dnf install maven`
  - Or use the Maven Wrapper: drop `mvnw` from https://maven.apache.org/wrapper/
- **A running Pulsar broker** (only needed at runtime, not build time).

## Build

```bash
mvn clean package
# Fat jar: target/pulsar-consumer-1.0.0.jar
```

To compile without packaging:

```bash
mvn -q clean compile
```

## Tests

Unit tests cover env-var parsing (`Config`) and the drain loop (`DrainRunner`) with a mocked
Pulsar `Consumer` — no broker required.

```bash
mvn test
```

> **JDK note:** Mockito's inline mock maker cannot mock the Pulsar `Consumer` interface under
> JDK 25+/26. Run tests on JDK 17 or 21:
>
> ```bash
> JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test    # macOS Corretto 17
> ```


## Run

```bash
java -jar target/pulsar-consumer-1.0.0.jar
```

All configuration is via environment variables (see reference below). Defaults assume a local standalone broker on `pulsar://localhost:6650`.

### Point at a real cluster

```bash
PULSAR_SERVICE_URL=pulsar+ssl://broker.example.com:6651 \
PULSAR_TOPIC=persistent://tenant/namespace/my-topic \
PULSAR_SUBSCRIPTION=batch-drain-sub \
PULSAR_MAX_MESSAGES=10000 \
java -jar target/pulsar-consumer-1.0.0.jar
```

## Local development with Pulsar standalone

The fastest path to a local broker for testing:

```bash
# 1. Fetch Pulsar 4.0.x
wget https://archive.apache.org/dist/pulsar/pulsar-4.0.11/apache-pulsar-4.0.11-bin.tar.gz
tar xzf apache-pulsar-4.0.11-bin.tar.gz
cd apache-pulsar-4.0.11

# 2. Start standalone ( listens on pulsar://localhost:6650, HTTP on :8080 )
bin/pulsar standalone
```

In another terminal, seed some messages:

```bash
# Using the bundled CLI
bin/pulsar-client produce persistent://public/default/my-topic \
  --num 5000 \
  --messages "hello-pulsar"
```

Then run the consumer:

```bash
java -jar target/pulsar-consumer-1.0.0.jar
```

### Tiny seed program (alternative to CLI)

If you want to seed from Java instead, here is the minimum viable producer:

```java
try (var client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
     var producer = client.newProducer().topic("persistent://public/default/my-topic").create()) {
  for (int i = 0; i < 5_000; i++) {
    producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
  }
}
```

## Configuration reference

| Env var | Default | Description |
|---|---|---|
| `PULSAR_SERVICE_URL` | `pulsar://localhost:6650` | Broker service URL. Use `pulsar+ssl://...:6651` for TLS. |
| `PULSAR_TOPIC` | `persistent://public/default/my-topic` | Fully-qualified topic name. |
| `PULSAR_SUBSCRIPTION` | `my-subscription` | Subscription name. Persistent across runs unless deleted. |
| `PULSAR_SUBSCRIPTION_TYPE` | `Shared` | One of `Exclusive`, `Failover`, `Shared`, `Key_Shared` (case-insensitive, also accepts `Key_Shared` / `Key-Shared`). |
| `PULSAR_INITIAL_POSITION` | `Earliest` | Where a brand-new subscription starts reading. `Earliest` = head of backlog (use this for draining). `Latest` = only messages published after connect. Ignored if the subscription already exists (the broker resumes from its persisted cursor). |
| `PULSAR_MAX_MESSAGES` | `10000` | Per-run cap. Stops the run exactly at this count. |
| `PULSAR_RECEIVE_TIMEOUT_MS` | `10000` | Idle timeout. If `batchReceive()` returns empty for this many ms, the topic is considered drained and the run ends. |

## Behavior

The main loop calls `consumer.batchReceive()` with a per-batch policy of:

- `maxNumMessages = 1000`
- `maxNumBytes = 10 MiB`
- `timeout = 200 ms`

The run ends when **any** of:

1. The cumulative message count reaches `PULSAR_MAX_MESSAGES` (truncated exactly — never exceeded), or
2. The topic has been idle (no messages returned by `batchReceive()`) for at least `PULSAR_RECEIVE_TIMEOUT_MS`, or
3. A message fails processing (nacked; see "Processing & error handling" below).

Per-message individual acks are used (`consumer.acknowledge(msg)`) on every successfully processed message. This is the only mode that works correctly for `Shared` and `Key_Shared` subscriptions; it is also valid for `Exclusive`/`Failover`.

## Paginated draining (drain a large backlog across multiple runs)

For draining a large backlog — e.g. **106k messages, 10k per run** — run the jar repeatedly with the
**same `PULSAR_SUBSCRIPTION` name**. The broker persists the subscription cursor in BookKeeper on
every ack, so each run resumes exactly where the prior run stopped. **No local cursor file is
needed** — the named subscription IS the cursor.

Recommended settings:

```bash
PULSAR_SUBSCRIPTION=drain-106k-backlog \
PULSAR_SUBSCRIPTION_TYPE=Exclusive \
PULSAR_INITIAL_POSITION=Earliest \
PULSAR_MAX_MESSAGES=10000 \
java -jar target/pulsar-consumer-1.0.0.jar
```

- `Exclusive` — required for ordered reading; only one consumer per subscription name at a time.
- `Earliest` — first run reads from the head of the backlog (subsequent runs ignore this; the
  persisted cursor wins).
- `PULSAR_MAX_MESSAGES=10000` — 10k per run; 106k backlog → ~11 runs to drain.
- Same `PULSAR_SUBSCRIPTION` every run — the broker remembers position.

**Caveat — subscription type is immutable.** If you previously created the subscription as
`Shared`, switching to `Exclusive` requires deleting it first:

```bash
bin/pulsar-admin persistent unsubscribe \
  --subscription drain-106k-backlog persistent://public/default/my-topic
```

Or just use a new subscription name.

## Processing & error handling

By default the consumer treats every received message as successfully processed and acks it. To plug
in your own logic (DB write, API call, transformation, etc.), implement `com.example.Processor` and
pass it to `new DrainRunner(consumer, config, yourProcessor)`:

```java
public enum ProcessorResult { OK, FAILED, POISON }

@FunctionalInterface
public interface Processor<T> {
  ProcessorResult process(Message<T> message) throws Exception;
}
```

- **`OK`** — message acked, loop continues.
- **`FAILED` (transient)** — message nacked via `consumer.negativeAcknowledge(msg)`, run stops
  immediately. The broker re-delivers the same message on the next run because the subscription
  cursor is not advanced past an unacked message.
- **`POISON` (permanent)** — same behavior as `FAILED` in this build (no dead-letter queue wired).
  A real deployment would route these to a DLQ topic instead of redelivering forever.
- **Processor throws** — caught and treated as `FAILED` (nack + stop).

Idempotency: because `FAILED` triggers redelivery, your `Processor` should be idempotent on
re-runs if at-least-once semantics are unacceptable for your downstream effect.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Normal completion (cap reached or topic drained). |
| `2` | Configuration error (bad env var value). |
| `3` | Runtime error (`PulsarClientException`, including `AlreadyClosedException`). |

### Shutdown handling

A `Runtime.addShutdownHook` closes the consumer on SIGINT (Ctrl-C). The outer try-with-resources then closes the `PulsarClient`. Both resources are also `Closeable` for the normal-exit path.

## Output format

```
Connecting to Pulsar:
  service-url : pulsar://localhost:6650
  topic       : persistent://public/default/my-topic
  subscription: my-subscription (Shared)
  max-messages: 10000
  idle-timeout: 10000 ms
[batch 1] received=1000 (truncated from 1000), total=1000/10000, 4823 msgs/s
[batch 2] received=1000 (truncated from 1000), total=2000/10000, 5212 msgs/s
...
[batch 10] received=1000 (truncated from 1000), total=10000/10000, 5108 msgs/s
--------
Done. total=10000, elapsed=1.957s, avg=5109 msgs/s
```

## Project layout

```
pulsar-consumer/
├── pom.xml
├── README.md
└── src/main/java/com/example/
    └── PulsarConsumerApp.java
```

## Constraints honored

- Basic `byte[]` consumer — no schema registry, no transactions, no OAuth.
- Java 17 idioms (records, switch expressions, try-with-resources on `Consumer`).
- Single-file main class, no extra features beyond the spec.
