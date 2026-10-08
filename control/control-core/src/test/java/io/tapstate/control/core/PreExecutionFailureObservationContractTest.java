package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PreExecutionFailure;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaim;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;

class PreExecutionFailureObservationContractTest {
    private static final Instant AT = Instant.parse("2026-10-08T02:00:00Z");
    private final Resource pipeline = new DslParser().parse("""
            version: tapstate/v1
            kind: pipeline
            id: orders
            source: src_x
            view:
              from: src_x
              primary_key: id
            """);
    private String incarnation = "inc-current";
    private DesiredState intent = new DesiredState("orders", PipelineState.RUNNING, CanonicalHash.of(pipeline));
    private CheckpointDoc checkpoint = new CheckpointDoc("orders", StateJson.of(PipelineState.FAILED), 4L, AT);
    private long generation;
    private Runnable onRead = () -> { };
    private ObservationStore.Stored stored;

    @ParameterizedTest
    @ValueSource(longs = {0, 41})
    void actualRefusalIsCurrentWithoutAllocatingAnExecution(long frontier) {
        CurrentObservationReader reader = fixture(frontier);
        Observation observed = stored.observation();

        assertThat(reader.read("orders")).contains(observed);
        assertThat(observed.state()).isEqualTo(PipelineState.FAILED);
        assertThat(observed.failure().code()).isEqualTo("actuation.source-schema-not-discovered");
        assertThat(observed.metrics()).isEmpty();
        assertThat(observed.snapshot()).isEmpty();
        assertThat(observed.positions()).isEmpty();
        assertThat(observed.facts()).isEmpty();
        assertThat(generation).isEqualTo(frontier);
    }

    private CurrentObservationReader fixture(long frontier) {
        generation = frontier;
        Map<String, String> lineage = ObservationArtifactLineage.hashes("orders", List.of(pipeline));
        var attempt = new PreExecutionFailure.Attempt("orders", "cluster-a", incarnation,
                new CheckpointDoc("orders", StateJson.of(PipelineState.NEW), 3L, AT.minusSeconds(1)), intent,
                lineage, frontier(), null);
        var receipt = attempt.failed(checkpoint);
        stored = new ObservationStore.Stored(new Observation("orders", PipelineState.FAILED,
                Map.of(), Map.of(), Map.of(), new ObservationFailure("actuation.source-schema-not-discovered",
                        Map.of("source", "src_x")), AT, List.of()), Optional.empty(), Optional.of(receipt.owner()));
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<Resource> resources) { throw new AssertionError("read-only"); }
            @Override public Optional<Resource> get(String id) { return id.equals("orders") ? Optional.of(pipeline) : Optional.empty(); }
            @Override public List<Resource> list() { return List.of(pipeline); }
            @Override public Optional<String> pipelineIncarnationId(String id) { return Optional.of(incarnation); }
        };
        ExecutionGenerationStore generations = new ExecutionGenerationStore() {
            @Override public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim claim, long revision) {
                throw new AssertionError("a diagnostic never advances");
            }
            @Override public OptionalLong advanceStandalone(String cluster, String id) {
                throw new AssertionError("a diagnostic never advances");
            }
            @Override public OptionalLong currentGeneration(String cluster, String id) { return frontier(); }
        };
        ObservationStore observations = new ObservationStore() {
            @Override public void save(Observation frame) { throw new AssertionError("read-only"); }
            @Override public Optional<Observation> read(String id) { throw new AssertionError("no legacy fallback"); }
            @Override public Optional<Stored> readStored(String id) { onRead.run(); return Optional.of(stored); }
            @Override public boolean isCurrentPreExecutionFailure(PreExecutionFailure.Owner owner) {
                return owner.pipelineIncarnationId().equals(incarnation) && owner.generationFrontier().equals(frontier())
                        && checkpoint != null && owner.checkpointDigest().equals(PreExecutionFailure.checkpointDigest(checkpoint))
                        && intent != null && owner.desiredDigest().equals(PreExecutionFailure.desiredDigest(intent))
                        && owner.artifactLineageDigest().equals(PreExecutionFailure.lineageDigest(
                                ObservationArtifactLineage.hashes("orders", List.of(pipeline))));
            }
            @Override public void delete(String id) { throw new AssertionError("read-only"); }
        };
        StateStore states = new StateStore() {
            @Override public Optional<CheckpointDoc> read(String id) { return Optional.ofNullable(checkpoint); }
            @Override public void create(String id, String json, Instant at) { throw new AssertionError("read-only"); }
            @Override public CasOutcome compareAndSwap(String id, long epoch, String json, Instant at) {
                throw new AssertionError("read-only");
            }
            @Override public void delete(String id) { throw new AssertionError("read-only"); }
        };
        DesiredStore desired = new DesiredStore() {
            @Override public Optional<DesiredState> read(String id) { return Optional.ofNullable(intent); }
            @Override public void save(DesiredState value) { throw new AssertionError("read-only"); }
            @Override public List<String> pipelineIds() { return List.of("orders"); }
            @Override public void delete(String id) { throw new AssertionError("read-only"); }
        };
        return new CurrentObservationReader(artifacts, generations, observations, "cluster-a");
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 41})
    void admissionBetweenTheFirstFrontierReadAndPayloadReadMakesTheDiagnosisPending(long frontier) {
        CurrentObservationReader reader = fixture(frontier);
        onRead = () -> generation++;
        assertThat(reader.read("orders")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 41})
    void sameCodeCannotSurviveAnIntentOrCheckpointOrIncarnationChange(long frontier) {
        CurrentObservationReader reader = fixture(frontier);
        DesiredState original = intent;
        intent = new DesiredState("orders", original.targetState(), original.revision(), original.purgeState(),
                "changed-assembly", true, 4L);
        assertThat(reader.read("orders")).isEmpty();
        intent = original;
        checkpoint = new CheckpointDoc("orders", StateJson.of(PipelineState.FAILED), 5L, AT);
        assertThat(reader.read("orders")).isEmpty();
        checkpoint = new CheckpointDoc("orders", StateJson.of(PipelineState.FAILED), 4L, AT);
        incarnation = "inc-recreated";
        assertThat(reader.read("orders")).isEmpty();
    }

    private OptionalLong frontier() { return generation == 0 ? OptionalLong.empty() : OptionalLong.of(generation); }
}
