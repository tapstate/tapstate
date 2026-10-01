package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A faster sink cannot move the durable resume position past a second sink that has not written the
 * same changes yet.
 *
 * <p>The harness connector holds one target write at its real connector boundary and publishes a signal
 * only after the batch reaches it. The other sink then writes three separately observed changes. This
 * creates the ordering the defect needs without sleeping or relying on processor scheduling: one writer
 * has acknowledged a ring position and the other still stands at the pipeline's initial position.
 *
 * <p>No public read face exposes each sink writer's private acknowledgement. The case therefore reads the
 * real Mongo control-store document independently, as the other cursor and reclamation witnesses do. It
 * asserts both halves of the contract: the per-writer positions have diverged, while the existing shared
 * resume position stays at their minimum; after release, both targets and that minimum advance.
 */
class ASlowerSinkHoldsTheResumePositionIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String SOURCE = "two_sink_source";
    private static final String FAST_TARGET = "fast_target";
    private static final String HELD_TARGET = "held_target";
    private static final String PIPELINE = "two_sink_pipeline";
    private static final String TABLE = "orders";
    /** Each sink runs one writer, so each writer stands for one sink. */
    private static final String FAST_WRITER = "serve.fast#0";
    private static final String HELD_WRITER = "serve.held#0";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void theResumePositionWaitsForEverySinkWriter(Tiers tier, @TempDir Path temporary)
            throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Path fastTarget = Files.createDirectory(temporary.resolve("fast-target"));
        Path heldTarget = Files.createDirectory(temporary.resolve("held-target"));
        Path signals = Files.createDirectory(temporary.resolve("held-write"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress fastAddress = EndpointAddress.uri(fastTarget.toString());
        EndpointAddress heldAddress = EndpointAddress.uri(heldTarget.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(sourceAddress, TABLE, SeedRows.generated(0));

        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String store = SharedMongo.replicaSetUrl("two_sink_floor_" + suffix);
        try (ServerHandle server = tier.launch(store); StoreDocuments documents = StoreDocuments.at(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
            control.apply(workspace(source, fastTarget, heldTarget, signals));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            String chain = Await.answered("the pipeline's one mining chain", TIMEOUT, () -> {
                if (documents.miningChainIds().size() != 1) {
                    return Optional.empty();
                }
                return Optional.of(documents.miningChainIds().iterator().next());
            });
            Await.until("the pipeline's initial shared resume position", TIMEOUT,
                    () -> hasAggregateRingDone(documents, chain),
                    () -> String.valueOf(documents.consumerOffset(chain, PIPELINE)));
            Progress initial = progress(documents, chain);

            try {
                for (int expected = 1; expected <= 3; expected++) {
                    files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                    int count = expected;
                    Await.until("the fast sink to write change " + expected, TIMEOUT,
                            () -> files.count(fastAddress, TABLE) == count,
                            () -> "fast=" + files.count(fastAddress, TABLE)
                                    + ", held=" + files.count(heldAddress, TABLE)
                                    + ", state=" + control.state(PIPELINE)
                                    + ", logs=" + control.logs(PIPELINE));
                }
                Await.until("the held sink to be inside its first write", TIMEOUT,
                        () -> Files.exists(signals.resolve("waiting")),
                        () -> "signals=" + directoryEntries(signals));

                Await.until("one writer to advance without moving the shared resume floor", TIMEOUT,
                        () -> oneWriterIsAhead(progress(documents, chain), initial.aggregateRingDone()),
                        () -> progress(documents, chain).toString());
                Progress divided = progress(documents, chain);
                assertThat(divided.aggregateRingDone())
                        .as("the shared resume floor while one target write is held")
                        .isEqualTo(initial.aggregateRingDone());

                Files.writeString(signals.resolve("release"), "");
                Await.until("the held sink to catch up", TIMEOUT,
                        () -> files.count(heldAddress, TABLE) == 3,
                        () -> "fast=" + files.count(fastAddress, TABLE)
                                + ", held=" + files.count(heldAddress, TABLE)
                                + ", progress=" + progress(documents, chain)
                                + ", state=" + control.state(PIPELINE)
                                + ", logs=" + control.logs(PIPELINE));

                // One more position closes the lag-by-one acknowledgement for the batch that caught up.
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                Await.until("both sinks and their shared resume floor to advance", TIMEOUT,
                        () -> files.count(fastAddress, TABLE) == 4
                                && files.count(heldAddress, TABLE) == 4
                                && progress(documents, chain).aggregateRingDone()
                                        > initial.aggregateRingDone(),
                        () -> "fast=" + files.count(fastAddress, TABLE)
                                + ", held=" + files.count(heldAddress, TABLE)
                                + ", progress=" + progress(documents, chain)
                                + ", state=" + control.state(PIPELINE)
                                + ", logs=" + control.logs(PIPELINE));
                Await.until("the shared resume floor to stand where the slower writer stands", TIMEOUT,
                        () -> {
                            Progress caughtUp = progress(documents, chain);
                            return caughtUp.aggregateRingDone() == Math.min(
                                    caughtUp.writerAt(FAST_WRITER, initial.aggregateRingDone()),
                                    caughtUp.writerAt(HELD_WRITER, initial.aggregateRingDone()));
                        },
                        () -> progress(documents, chain).toString());
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
            } finally {
                Files.writeString(signals.resolve("release"), "");
            }
        }
    }

    private static boolean oneWriterIsAhead(Progress progress, long initial) {
        return progress.writerAt(HELD_WRITER, initial) == initial
                && progress.writerAt(FAST_WRITER, initial) > initial
                && progress.aggregateRingDone() == initial;
    }

    private static boolean hasAggregateRingDone(StoreDocuments documents, String chain) {
        Document consumer = documents.consumerOffset(chain, PIPELINE);
        return consumer != null
                && consumer.get("perTableRingDone") instanceof Document rings
                && rings.get(TABLE) instanceof Number;
    }

    /**
     * The aggregate and how far each writer of the current run is durable on the table, read from one real
     * consumer record. A writer that has reported nothing yet has no entry.
     */
    private static Progress progress(StoreDocuments documents, String chain) {
        Document consumer = documents.consumerOffset(chain, PIPELINE);
        if (consumer == null) {
            return new Progress(-1L, Map.of());
        }
        long aggregate = numberIn(consumer.get("perTableRingDone", Document.class), TABLE, -1L);
        Map<String, Long> positions = new TreeMap<>();
        if (consumer.get("writerRun") instanceof Document run
                && run.get("progress") instanceof Document byTable
                && byTable.get(TABLE) instanceof Document writers) {
            for (Object raw : writers.values()) {
                if (raw instanceof Document writer
                        && writer.get("writer") instanceof String id
                        && writer.get("durableSeq") instanceof Number position) {
                    positions.put(id, position.longValue());
                }
            }
        }
        return new Progress(aggregate, positions);
    }

    private static long numberIn(Document document, String field, long absent) {
        return document != null && document.get(field) instanceof Number number
                ? number.longValue()
                : absent;
    }

    private static List<String> directoryEntries(Path directory) {
        try (var entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static Map<String, String> workspace(
            Path source, Path fastTarget, Path heldTarget, Path signals) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, E2eConnectorJar.CONNECTOR_ID, source, TABLE));
        resources.put(FAST_TARGET + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(FAST_TARGET, E2eConnectorJar.CONNECTOR_ID, fastTarget));
        resources.put(HELD_TARGET + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s", hold_writes: "%s" }
                """.formatted(HELD_TARGET, E2eConnectorJar.CONNECTOR_ID, heldTarget, signals));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: cdc_only }
                serve:
                  from: %s
                  sync:
                    - { id: fast, source: %s, execution: { parallelism: 1 } }
                    - { id: held, source: %s, execution: { parallelism: 1 } }
                """.formatted(PIPELINE, SOURCE, TABLE, FAST_TARGET, HELD_TARGET));
        return resources;
    }

    private record Progress(long aggregateRingDone, Map<String, Long> writerRingDone) {

        /** How far {@code writer} is durable on the table, or {@code initial} while it has reported nothing. */
        long writerAt(String writer, long initial) {
            return writerRingDone.getOrDefault(writer, initial);
        }
    }
}
