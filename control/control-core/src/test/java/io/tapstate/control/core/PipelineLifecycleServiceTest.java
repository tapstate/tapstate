package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.DesiredStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The pipeline lifecycle write verbs: start / stop / pause / resume translate a user's intent into a
 * desired-state write, guarded by the four-verb state machine and a minimal revision-compatibility
 * check, and audited like every state-mutating control operation. Until the converge side writes the
 * actual state, the pipeline's current state is read back from its last desired intent (a NEW pipeline
 * has none), so the four verbs are reachable end-to-end before a runtime exists.
 *
 * <p>Collaborators are in-memory fakes: a store holding applied artifacts (whose canonical form is the
 * revision), a desired store, and an audit store that records or fails on demand.
 */
class PipelineLifecycleServiceTest {

    private static final Instant FIXED = Instant.parse("2026-07-11T10:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED, ZoneOffset.UTC);

    private final FakeArtifactStore artifacts = new FakeArtifactStore();
    private final FakeDesiredStore desired = new FakeDesiredStore();
    private final RecordingAuditStore audit = new RecordingAuditStore();
    private final PipelineLifecycleService service =
            new PipelineLifecycleService(new ArtifactQueryService(artifacts), desired, new AuditGate(audit, FIXED_CLOCK));

    @Test
    void startFromNewWritesRunningDesiredAtTheLatestRevision() {
        artifacts.save(PIPELINE_V1);

        DesiredState written = service.start("alice", "pl1");

        assertThat(written).isEqualTo(new DesiredState("pl1", PipelineState.RUNNING, revisionOf(PIPELINE_V1)));
        assertThat(desired.read("pl1")).contains(written);
    }

    @Test
    void anIllegalTransitionIsRefusedWithACodeAndWritesNothing() {
        artifacts.save(PIPELINE_V1);

        // A NEW pipeline (no desired doc yet) cannot be paused — pause is legal only from RUNNING.
        TapstateException thrown = catchThrowableOfType(TapstateException.class, () -> service.pause("alice", "pl1"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("lifecycle.illegal-transition");
        assertThat(thrown.args()).containsEntry("from", PipelineState.NEW).containsEntry("verb", "pause");
        assertThat(desired.read("pl1")).as("a refused verb writes no desired state").isEmpty();
        assertThat(audit.records).as("a refused verb leaves no audit record").isEmpty();
    }

    @Test
    void aVerbOnAnUnknownPipelineIsRefusedWithACode() {
        // Nothing applied under this id.
        TapstateException thrown = catchThrowableOfType(TapstateException.class, () -> service.start("alice", "ghost"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("lifecycle.unknown-pipeline");
        assertThat(thrown.args()).containsEntry("pipeline", "ghost");
        assertThat(desired.read("ghost")).isEmpty();
        assertThat(audit.records).isEmpty();
    }

    @Test
    void resumeAtAStaleRevisionIsRefusedWithTheRequestedAndLatest() {
        artifacts.save(PIPELINE_V1);
        service.start("alice", "pl1"); // desired RUNNING at v1
        service.pause("alice", "pl1"); // desired PAUSED at v1
        // The pipeline artifact is re-applied while paused: the latest revision moves to v2.
        artifacts.save(PIPELINE_V2);

        TapstateException thrown = catchThrowableOfType(TapstateException.class, () -> service.resume("alice", "pl1"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("lifecycle.incompatible-revision");
        assertThat(thrown.args())
                .containsEntry("requested", revisionOf(PIPELINE_V1))
                .containsEntry("latest", revisionOf(PIPELINE_V2));
        assertThat(desired.read("pl1"))
                .as("the refused resume leaves the paused desired state untouched")
                .contains(new DesiredState("pl1", PipelineState.PAUSED, revisionOf(PIPELINE_V1)));
    }

    @Test
    void aWriteIsAuditedUnderItsVerbsOperation() {
        artifacts.save(PIPELINE_V1);

        service.start("alice", "pl1");

        assertThat(audit.records).hasSize(1);
        AuditRecord record = audit.records.get(0);
        assertThat(record.operationId()).isEqualTo("pipeline.start");
        assertThat(record.principal()).isEqualTo("alice");
        assertThat(record.resourceId()).isEqualTo("pl1");
        assertThat(record.timestamp()).isEqualTo(FIXED);
    }

    @Test
    void aFailedAuditRefusesTheWriteSoNoDesiredStateIsPersisted() {
        artifacts.save(PIPELINE_V1);
        PipelineLifecycleService gated = new PipelineLifecycleService(
                new ArtifactQueryService(artifacts), desired, new AuditGate(new FailingAuditStore(), FIXED_CLOCK));

        TapstateException thrown = catchThrowableOfType(TapstateException.class, () -> gated.start("alice", "pl1"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code())
                .as("no audit, no execute: a failed audit write refuses the desired-state write")
                .isEqualTo("control.audit-blocked");
        assertThat(desired.read("pl1")).as("the desired state is not written when its audit record could not be").isEmpty();
    }

    @Test
    void startPauseResumeStopIsReachableThroughTheDesiredProxy() {
        artifacts.save(PIPELINE_V1);
        String rev = revisionOf(PIPELINE_V1);

        assertThat(service.start("alice", "pl1")).isEqualTo(new DesiredState("pl1", PipelineState.RUNNING, rev));
        assertThat(service.pause("alice", "pl1")).isEqualTo(new DesiredState("pl1", PipelineState.PAUSED, rev));
        assertThat(service.resume("alice", "pl1")).isEqualTo(new DesiredState("pl1", PipelineState.RUNNING, rev));
        assertThat(service.stop("alice", "pl1", false))
                .isEqualTo(new DesiredState("pl1", PipelineState.STOPPED, rev, false));
        assertThat(desired.read("pl1")).contains(new DesiredState("pl1", PipelineState.STOPPED, rev, false));
        // each verb is audited under its own operation — pins the whole verb->operation map, not just start.
        assertThat(audit.records).extracting(AuditRecord::operationId)
                .containsExactly("pipeline.start", "pipeline.pause", "pipeline.resume", "pipeline.stop");
    }

    @Test
    void stopFromPausedWritesStoppedAtThePausedRevision() {
        artifacts.save(PIPELINE_V1);
        String rev = revisionOf(PIPELINE_V1);
        service.start("alice", "pl1");
        service.pause("alice", "pl1");

        DesiredState written = service.stop("alice", "pl1", false);

        assertThat(written).isEqualTo(new DesiredState("pl1", PipelineState.STOPPED, rev, false));
        assertThat(desired.read("pl1")).contains(written);
        assertThat(audit.records.get(audit.records.size() - 1).operationId()).isEqualTo("pipeline.stop");
    }

    @Test
    void theAnswerAStopWasGivenIsWhatGetsWrittenDown() {
        artifacts.save(PIPELINE_V1);
        String rev = revisionOf(PIPELINE_V1);
        service.start("alice", "pl1");

        DesiredState written = service.stop("alice", "pl1", true);

        // The intent is the only place this answer survives the call that gave it: the converge side runs
        // later, in another process, and reads it from here. Written wrong, the pipeline is either cleared
        // against its owner's wishes or left holding state they asked to be rid of, and both look like a
        // normal stop until the next run either continues or reads its whole source again.
        assertThat(written).isEqualTo(new DesiredState("pl1", PipelineState.STOPPED, rev, true));
        assertThat(desired.read("pl1")).contains(written);
    }

    @Test
    void everyOtherVerbWritesAnIntentThatClearsNothing() {
        artifacts.save(PIPELINE_V1);

        // Asserted over all three rather than trusting the field's default, because they are the verbs a
        // pipeline passes through on its way to a stop: an intent left saying "clear" by a pause would be
        // acted on by whichever stop came next, and nothing in between would say so.
        assertThat(service.start("alice", "pl1").purgeState()).isFalse();
        assertThat(service.pause("alice", "pl1").purgeState()).isFalse();
        assertThat(service.resume("alice", "pl1").purgeState()).isFalse();
    }

    // ---- fixtures ----

    private static Resource parse(String dsl) {
        return new DslParser().parse(dsl);
    }

    /** The revision of an artifact is the content hash of its canonical form — the same value apply stamps. */
    private static String revisionOf(String dsl) {
        return CanonicalHash.of(new CanonicalWriter().write(parse(dsl)));
    }

    private static final String PIPELINE_V1 = """
            version: tapstate/v1
            kind: pipeline
            id: pl1
            source: src_x
            settings: { read_mode: snapshot_and_cdc }
            serve:
              from: /.*/
              sync:
                - id: sink1
                  source: tgt_x
                  write_mode: upsert
                  ddl: apply
            """;

    /** The same pipeline re-applied with a changed setting, so its canonical form — and revision — differ. */
    private static final String PIPELINE_V2 = """
            version: tapstate/v1
            kind: pipeline
            id: pl1
            source: src_x
            settings: { read_mode: cdc_only }
            serve:
              from: /.*/
              sync:
                - id: sink1
                  source: tgt_x
                  write_mode: upsert
                  ddl: apply
            """;

    /** An in-memory artifact store holding resources by id; reads reconstruct through the query service. */
    private static final class FakeArtifactStore implements ArtifactStore {
        private final Map<String, Resource> byId = new HashMap<>();

        void save(String dsl) {
            Resource r = parse(dsl);
            byId.put(r.id(), r);
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(r -> byId.put(r.id(), r));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }

    /** An in-memory desired store, last write wins per pipeline id. */
    private static final class FakeDesiredStore implements DesiredStore {
        private final Map<String, DesiredState> byId = new HashMap<>();

        @Override
        public void save(DesiredState desired) {
            byId.put(desired.pipelineId(), desired);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            return Optional.ofNullable(byId.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return List.copyOf(byId.keySet());
        }

        @Override
        public void delete(String pipelineId) {
            byId.remove(pipelineId);
        }
    }

    /** An audit store that captures every record written through it. */
    private static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

    /** An audit store that always fails, standing in for an unavailable audit backend. */
    private static final class FailingAuditStore implements AuditStore {
        @Override
        public void record(AuditRecord record) {
            throw new IllegalStateException("audit backend down");
        }
    }
}
