package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
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

        try (files; PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                ServerHandle server = tier.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
            control.apply(workspace(source, fastTarget, heldTarget, signals));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            String chain = Await.answered("the pipeline's one mining chain", TIMEOUT, () -> {
                Document member = documents.pipelineMembership(PIPELINE, SOURCE);
                return member == null ? Optional.empty()
                        : Optional.of(member.get("_id", Document.class).getString("chain"));
            });
            Await.until("the pipeline's initial shared resume position", TIMEOUT,
                    () -> hasAggregateRingDone(documents, chain),
                    () -> String.valueOf(documents.consumerOffset(chain, SrsConsumerId.of(PIPELINE, SOURCE).value())));
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
                        () -> oneWriterIsAhead(progress(documents, chain), initial),
                        () -> progress(documents, chain).toString());
                Progress divided = progress(documents, chain);
                assertThat(divided.runId()).isEqualTo(initial.runId());
                assertThat(divided.epoch()).isEqualTo(initial.epoch());
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
                            return initial.runId().equals(caughtUp.runId()) && initial.epoch() == caughtUp.epoch()
                                    && caughtUp.aggregateRingDone() == Math.min(
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

    private static boolean oneWriterIsAhead(Progress progress, Progress initial) {
        return initial.runId().equals(progress.runId()) && initial.epoch() == progress.epoch()
                && progress.writerAt(HELD_WRITER, initial.aggregateRingDone()) == initial.aggregateRingDone()
                && progress.writerAt(FAST_WRITER, initial.aggregateRingDone()) > initial.aggregateRingDone()
                && progress.aggregateRingDone() == initial.aggregateRingDone();
    }

    private static boolean hasAggregateRingDone(NativeSrsDocuments documents, String chain) {
        Document consumer = documents.consumerOffset(chain, SrsConsumerId.of(PIPELINE, SOURCE).value());
        return consumer != null && documents.chain(chain).getLong("epoch") > 0
                && consumer.get("writerRun") instanceof Document run
                && run.get("runId") instanceof String
                && consumer.getList("ringDoneThrough", Document.class).stream()
                        .anyMatch(row -> TABLE.equals(row.getString("table")) && row.get("sequence") instanceof Number);
    }

    /** The aggregate and individual writer positions from one original native consumer cursor. */
    private static Progress progress(NativeSrsDocuments documents, String chain) {
        Document consumer = documents.consumerOffset(chain, SrsConsumerId.of(PIPELINE, SOURCE).value());
        if (consumer == null || !(consumer.get("writerRun") instanceof Document run)) {
            return new Progress(-1L, Map.of(), null, -1L);
        }
        long aggregate = consumer.getList("ringDoneThrough", Document.class).stream()
                .filter(row -> TABLE.equals(row.getString("table")))
                .mapToLong(row -> row.get("sequence", Number.class).longValue()).findFirst().orElse(-1L);
        Map<String, Long> positions = new TreeMap<>();
        long epoch = documents.chain(chain).getLong("epoch");
        for (Document table : run.getList("progress", Document.class)) {
            if (!TABLE.equals(table.getString("table"))) continue;
            for (Document writer : table.getList("writers", Document.class)) {
                Document through = writer.get("durableThrough", Document.class);
                long reportedEpoch = through.get("epoch", Number.class).longValue();
                if (epoch != reportedEpoch)
                    throw new AssertionError("writer positions belong to different source epochs");
                positions.put(writer.getString("writer"), through.get("sequence", Number.class).longValue());
            }
        }
        return new Progress(aggregate, positions, run.getString("runId"), epoch);
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

    private record Progress(long aggregateRingDone, Map<String, Long> writerRingDone, String runId, long epoch) {

        /** How far {@code writer} is durable on the table, or {@code initial} while it has reported nothing. */
        long writerAt(String writer, long initial) {
            return writerRingDone.getOrDefault(writer, initial);
        }
    }
}
