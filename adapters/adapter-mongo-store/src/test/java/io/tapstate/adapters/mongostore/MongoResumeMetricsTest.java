package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MongoResumeMetricsTest {
    private static final String ID = "orders";
    private static final String FIELD = "resumeMetrics";
    private static final long JOB = 101L;
    private static final int LIMIT = 1024 * 1024;
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 2, "cache", ID, 3);
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, ID);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimFence ORIGINAL = new WorkloadClaimFence(KEY, OWNER, 4, 5, 6);
    private static final WorkloadClaimFence NEXT = new WorkloadClaimFence(KEY, OWNER, 4, 6, 6);
    private static final MetricFact SAVED = count("table", 7);
    private final MongoDatabase database = mock(MongoDatabase.class);
    private final ClientSession session = mock(ClientSession.class);
    private final MongoCachePipelineAccess access = new MongoCachePipelineAccess(
            mock(MongoClient.class), database, SCOPE, ORIGINAL, () -> {});

    @Test void pauseFreezesThePublishedCountAndOnlyTheBoundExecutionCanCarryIt() {
        var paused = pause(SAVED, count("table", 10));
        var baseline = new ResumeMetrics.Baseline(2, 1, ORIGINAL, JOB, List.of(SAVED), false);
        assertThat(MongoResumeMetrics.read(paused)).isEqualTo(new ResumeMetrics.Handoff(null, baseline));
        var resumed = rebuild(paused, JOB);
        assertThat(MongoResumeMetrics.read(resumed).applied()).isEqualTo(new ResumeMetrics.Binding(baseline, 3, ORIGINAL));

        var advanced = MongoResumeMetrics.advance(resumed, ORIGINAL, NEXT, 3);
        assertThat(MongoResumeMetrics.read(state(advanced, 3)).applied()).isEqualTo(new ResumeMetrics.Binding(baseline, 3, NEXT));
        assertThat(MongoResumeMetrics.advance(resumed, NEXT, ORIGINAL, 3)).containsEntry(FIELD, null);
        assertThat(MongoResumeMetrics.advance(resumed, ORIGINAL, NEXT, 4)).containsEntry(FIELD, null);
        assertThat(rebuild(paused, JOB + 1)).containsEntry(FIELD, null);
    }

    @Test void oversizedFactsRetainTheIdentityButMarkTheBaselineUnavailable() {
        var huge = count("t".repeat(LIMIT), 7);
        var paused = pause(huge, huge);
        var expected = new ResumeMetrics.Baseline(2, 1, ORIGINAL, JOB, List.of(), true);
        assertThat(MongoResumeMetrics.read(paused).candidate()).isEqualTo(expected);
        assertThat(MongoResumeMetrics.read(rebuild(paused, JOB)).applied())
                .isEqualTo(new ResumeMetrics.Binding(expected, 3, ORIGINAL));
    }

    @Test void oversizedFenceCarriesOnlyTheUnavailableRevisionThroughTheMatchingResume() {
        var resumed = rebuild(pause(SAVED, SAVED), JOB);
        var hugeFence = new WorkloadClaimFence(KEY, new WorkloadOwner("node", "b".repeat(LIMIT)), 4, 6, 6);
        var compact = MongoResumeMetrics.advance(resumed, ORIGINAL, hugeFence, 3);
        assertThat(compact.get(FIELD)).isEqualTo(new Document("unavailableRevision", 3L).append("jobId", JOB));
        var row = state(compact, 3);
        assertThat(MongoResumeMetrics.unavailableRevision(row)).isEqualTo(3);
        assertThat(MongoResumeMetrics.advance(row, ORIGINAL, NEXT, 3)).isEmpty();
        assertThat(MongoResumeMetrics.advance(row, ORIGINAL, NEXT, 4)).containsEntry(FIELD, null);

        var paused = transition(row, ResumeMetrics.Action.PAUSE, 4, JOB + 1, List.of());
        assertThat(MongoResumeMetrics.unavailableRevision(paused)).isEqualTo(4);
        assertThat(MongoResumeMetrics.unavailableRevision(transition(paused, ResumeMetrics.Action.REBUILD, 5, JOB + 1, List.of())))
                .isEqualTo(5);
        assertThat(transition(paused, ResumeMetrics.Action.REBUILD, 5, JOB, List.of())).containsEntry(FIELD, null);
    }

    @Test void checkpointsWithoutHandoffsAreReadableAndMalformedHandoffsReportTheStorageError() {
        var legacy = state(new Document(), 1);
        assertThat(MongoResumeMetrics.read(legacy)).isEqualTo(ResumeMetrics.Handoff.EMPTY);
        assertThat(MongoResumeMetrics.unavailableRevision(legacy)).isZero();
        var invalid = legacy.append(FIELD, new Document("unavailableRevision", 2L).append("jobId", "bad"));
        assertThatThrownBy(() -> MongoResumeMetrics.unavailableRevision(invalid))
                .isInstanceOfSatisfying(TapstateException.class, ex -> assertThat(ex.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        invalid.put(FIELD, new Document("candidate", new Document("revision", "bad")));
        assertThatThrownBy(() -> MongoResumeMetrics.read(invalid))
                .isInstanceOfSatisfying(TapstateException.class, ex -> assertThat(ex.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void theCompleteWindowFitsItsBoundAndMalformedOrUnknownVersionsAreUnreadable() {
        var slots = new java.util.EnumMap<ResumeMetrics.Producer, ResumeMetrics.Slot>(ResumeMetrics.Producer.class);
        for (var producer : ResumeMetrics.Producer.values()) slots.put(producer, new ResumeMetrics.Slot(Long.MIN_VALUE, true));
        var window = new ResumeMetrics.Window(Long.MAX_VALUE, Long.MIN_VALUE, slots);
        var encoded = MongoResumeWindow.bounded(window, ID);
        var row = new Document(MongoResumeWindow.FIELD, encoded);
        assertThat(new org.bson.RawBsonDocument(row, new org.bson.codecs.DocumentCodec()).getByteBuffer().remaining())
                .isLessThanOrEqualTo(256);
        assertThat(MongoResumeWindow.decode(row, ID)).isEqualTo(window);
        assertThat(MongoResumeWindow.decode(new Document(), ID)).isNull();
        encoded.put("formatVersion", 2);
        assertThatThrownBy(() -> MongoResumeWindow.decode(row, ID)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        encoded.put("formatVersion", 1);
        encoded.get("in", Document.class).remove("carry");
        assertThatThrownBy(() -> MongoResumeWindow.decode(row, ID)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void recoveryDistinguishesAnAbsentObservationFromLegacyTotalsAndARecordedWindow() {
        var resumed = rebuild(pause(SAVED, SAVED), JOB);
        var state = state(MongoResumeMetrics.advance(resumed, ORIGINAL, NEXT, 3), 3).append("touchMillis", 0L);
        state.append(MongoResumeWindow.PRIOR, List.of());
        assertThat(MongoResumeWindow.checkpoint(access, state).prior()).isEqualTo(ResumeMetrics.Prior.ABSENT);

        var id = MongoCachePipelineObservationStore.keyOfFence(access, 3, NEXT);
        var row = new Document("_id", id).append("pipelineId", ID)
                .append("flatKeys", List.of("bytes.in", "records.out"));
        state.put(MongoResumeWindow.PRIOR, List.of(row));
        assertThat(MongoResumeWindow.checkpoint(access, state).prior()).isEqualTo(
                new ResumeMetrics.Prior(true, null, null, Set.of(ResumeMetrics.Producer.IN, ResumeMetrics.Producer.OUT)));

        var window = new ResumeMetrics.Window(3, JOB + 1, Map.of(ResumeMetrics.Producer.STAGE, new ResumeMetrics.Slot(42, false)));
        row.remove("flatKeys");
        row.append("cumulative", List.of("stage")).append("observedAt", Date.from(Instant.EPOCH))
                .append(MongoResumeWindow.FIELD, MongoResumeWindow.encode(window));
        assertThat(MongoResumeWindow.checkpoint(access, state).prior()).isEqualTo(
                new ResumeMetrics.Prior(true, Instant.EPOCH, window, Set.of(ResumeMetrics.Producer.STAGE)));

        row.put("_id", MongoCachePipelineObservationStore.keyOfFence(access, 3, ORIGINAL));
        assertThatThrownBy(() -> MongoResumeWindow.checkpoint(access, state))
                .isInstanceOfSatisfying(TapstateException.class, ex -> assertThat(ex.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void aLatePublicationCannotReplaceOrRecreateTheObservationItSampled() {
        var sink = new MongoCachePipelineObservationStore(access);
        var window = new ResumeMetrics.Window(3, JOB + 1, Map.of(ResumeMetrics.Producer.OUT, new ResumeMetrics.Slot(0, true)));
        var prior = new ResumeMetrics.Prior(true, Instant.EPOCH, window, Set.of(ResumeMetrics.Producer.OUT));
        var key = new ResumeMetrics.ObservationKey(SCOPE, 3, ORIGINAL);
        var publication = new ResumeMetrics.Publication(key, prior, window);
        var reading = new Observation(ID, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, Instant.EPOCH);
        assertThat(sink.saveResumed(reading, 3, publication)).isFalse();
        verifyNoInteractions(database);

        var later = new Observation(ID, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, Instant.EPOCH.plusSeconds(1));
        assertThatThrownBy(() -> sink.saveResumed(later, 4, publication))
                .isInstanceOfSatisfying(TapstateException.class, ex -> assertThat(ex.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        verifyNoInteractions(database);

        MongoCollection<Document> collection = mock();
        when(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database)).thenReturn(collection);
        when(collection.withWriteConcern(any())).thenReturn(collection);
        when(collection.updateOne(any(Bson.class), anyList(), any(UpdateOptions.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        assertThat(sink.saveResumed(later, 3, publication)).isFalse();
        var expected = new Document("_id", MongoCachePipelineObservationStore.keyOfFence(access, 3, ORIGINAL))
                .append("pipelineId", new Document("$exists", true)).append("observedAt", Date.from(Instant.EPOCH))
                .append(MongoResumeWindow.FIELD, MongoResumeWindow.encode(window));
        verify(collection).updateOne(eq(expected), anyList(), argThat(options -> !options.isUpsert()));
    }

    private Document pause(MetricFact saved, MetricFact sampled) {
        var key = new Document("scope", MongoPipelinePreparationFacts.scope(SCOPE)).append("epoch", 1L)
                .append("claimGeneration", 4L).append("executionGeneration", 5L).append("topologyRevision", 6L)
                .append("node", "node").append("boot", "boot");
        var row = new Document("_id", key).append("pipelineId", ID).append("state", "RUNNING")
                .append("facts", List.of(MongoObservationStore.toDocument(saved)));
        MongoCollection<Document> collection = mock();
        FindIterable<Document> query = mock();
        when(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database)).thenReturn(collection);
        when(collection.find(session, new Document("_id", key))).thenReturn(query);
        when(query.projection(new Document("pipelineId", 1).append("state", 1).append("facts", 1))).thenReturn(query);
        when(query.first()).thenReturn(row);
        return transition(state(new Document(), 1), ResumeMetrics.Action.PAUSE, 2, JOB, List.of(sampled));
    }

    private Document rebuild(Document paused, long job) {
        return transition(paused, ResumeMetrics.Action.REBUILD, 3, job, List.of());
    }

    private Document transition(Document row, ResumeMetrics.Action action, long revision, long job, List<MetricFact> raw) {
        var values = new Document();
        MongoResumeMetrics.transition(access, session, row, values, new ResumeMetrics.Transition(action, revision, job, raw));
        return state(values, row.getLong("epoch") + 1).append("stateJson", action == ResumeMetrics.Action.PAUSE ? "PAUSED" : "RUNNING");
    }

    private static Document state(Document values, long epoch) {
        return new Document(values).append("_id", ID).append("epoch", epoch).append("stateJson", "RUNNING");
    }

    private static MetricFact count(String table, long rows) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}", MetricPoint.accumulated(
                Map.of(MetricAttributes.TABLE_ID, table, "direction", "out"), Instant.EPOCH, Instant.EPOCH, rows));
    }
}
