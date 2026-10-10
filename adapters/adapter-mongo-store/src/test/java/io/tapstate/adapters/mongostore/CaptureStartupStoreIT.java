package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Replica-set witnesses for atomic prepared facts, reader versions and exact execution authority. */
@RequiresDocker
class CaptureStartupStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void anchorAloneIsUnprovenAndLaterCheckpointMovementDoesNotChangeThePreparedRequest() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.CDC_ONLY);
            CaptureReadAttempt attempt = fixture.begin("original");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, attempt, false)).isTrue();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            assertThat(fixture.meta.recordCaptureAnchor(attempt, "original")).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isEmpty();
            assertThat(fixture.meta.recordCaptureFirstDelivery(attempt)).isTrue();
            fixture.meta.advanceCaptureCheckpoint("chain", new ChainPosition(new SourceOrder(fixture.epoch, 200), "later"), List.of("orders"));
            var proof = fixture.meta.captureStartupProof(fixture.pipelineFence(), witness).orElseThrow();
            assertThat(proof.requestedPosition().position().token()).isEqualTo("original");
            try (var session = fixture.client.startSession()) {
                assertThat(session.withTransaction(() -> fixture.meta.guardPreparedStartup(session, fixture.pipelineFence(),
                        witness, proof.requestedPosition()))).isTrue();
            }
            assertThat(fixture.workloads.release(fixture.capture)).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isEmpty();
        }
    }

    @Test
    void oldWideningCallbacksCannotCertifyAReplacementReaderUnderTheSameClaimAndEpoch() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.CDC_ONLY);
            CaptureReadAttempt old = fixture.begin("original");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, old, false)).isTrue();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            assertThat(fixture.meta.recordCaptureAnchor(old, "original")).isTrue();
            CaptureReadAttempt next = fixture.begin("original");
            assertThat(next.version()).isGreaterThan(old.version());
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, next, false)).isTrue();
            assertThat(fixture.meta.recordCaptureFirstDelivery(old)).isFalse();
            assertThat(fixture.meta.recordCaptureReadFailure(old, "connector.capture-failed")).isFalse();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isEmpty();
            assertThat(fixture.meta.recordCaptureAnchor(next, "original")).isTrue();
            assertThat(fixture.meta.recordCaptureFirstDelivery(next)).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isPresent();
        }
    }

    @Test
    void anExistingProducerDoesNotCertifyAnUnopenedConsumerSnapshot() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.SNAPSHOT_AND_CDC);
            CaptureReadAttempt producer = fixture.begin("original");
            assertThat(fixture.meta.recordCaptureAnchor(producer, "original")).isTrue();
            assertThat(fixture.meta.recordCaptureFirstDelivery(producer)).isTrue();
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, producer, true)).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isEmpty();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isEmpty();
            fixture.meta.setCdcStart("chain", fixture.consumer, "own-sampled-seam", fixture.epoch);
            assertThat(fixture.meta.recordSnapshotStartup(fixture.pipelineFence(), witness)).isTrue();
            var proof = fixture.meta.captureStartupProof(fixture.pipelineFence(), witness).orElseThrow();
            assertThat(proof.acceptedPosition().position().token()).isEqualTo("own-sampled-seam");
            assertThat(proof.readerState().resolvedAnchor()).isEqualTo("original");
        }
    }

    @Test
    void movedTruthRefusesPreparationAndReleasedPipelineCannotConsumeAcceptedMarker() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness prior = fixture.witness(ReadMode.CDC_ONLY);
            fixture.meta.advanceCaptureCheckpoint("chain", new ChainPosition(new SourceOrder(fixture.epoch, 20), "changed"), List.of("orders"));
            assertThat(fixture.meta.prepareCaptureResume(fixture.pipelineFence(), prior,
                    prior.requestedPosition("capture").orElseThrow(), Set.of("source"))).isFalse();
            CaptureResumeWitness current = fixture.prepare(ReadMode.CDC_ONLY);
            CaptureReadAttempt attempt = fixture.begin("changed");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), current, attempt, false)).isTrue();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), current, fixture.epoch)).isTrue();
            assertThat(fixture.meta.recordCaptureAnchor(attempt, "changed")).isTrue();
            assertThat(fixture.meta.recordCaptureFirstDelivery(attempt)).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), current)).isPresent();
            assertThat(fixture.workloads.release(fixture.pipeline)).isTrue();
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), current)).isEmpty();
        }
    }

    @Test
    void anAnchorCommittedBeforeCheckpointWriteRemainsAResumeFloor() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getDatabase(fixture.database).getCollection("meta").updateOne(new Document("_id", "chain"),
                    new Document("$unset", new Document("sourceReadOffset", "").append("sourceReadEpoch", "")
                            .append("sourceReadSeq", "").append("sourceReadDurable", "").append("sourceReadAt", "")));
            CaptureResumeWitness initial = fixture.witness(ReadMode.CDC_ONLY);
            assertThat(initial.requestedPosition("capture")).isEmpty();
            assertThat(fixture.meta.prepareCaptureResume(fixture.pipelineFence(), initial, null, Set.of("source"))).isTrue();
            CaptureReadAttempt reader = fixture.meta.beginCaptureReadAttempt("chain", fixture.epoch, List.of("orders"),
                    CaptureReadAttempt.Kind.PRESENT, null, null, WorkloadClaimFence.from(fixture.capture)).orElseThrow();
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), initial, reader, false)).isTrue();
            assertThat(fixture.meta.recordCaptureAnchor(reader, "resolved-t0")).isTrue();
            assertThat(fixture.meta.read("chain").orElseThrow().sourceRead()).isNull();
            assertThat(fixture.meta.captureReadState("chain").orElseThrow().accepted()).isFalse();
            CaptureResumeWitness resumed = fixture.witness(ReadMode.CDC_ONLY);
            assertThat(resumed.requestedPosition("capture")).isPresent();
            assertThat(resumed.requestedPosition("capture").orElseThrow().position().token()).isEqualTo("resolved-t0");
        }
    }

    @Test
    void aSharedReaderFailureAfterAttachmentPreservesCodeArgumentsAndOriginalPoint() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.CDC_ONLY);
            CaptureReadAttempt reader = fixture.begin("original");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, reader, true)).isTrue();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            Map<String, Object> params = Map.of("requested", "old-point", "earliest", "current-head", "retention", "2h");
            assertThat(fixture.meta.recordCaptureReadFailure(reader, "capture.start-from-outside-window", params,
                    "stop-and-clear-source-state")).isTrue();
            var failure = fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness).orElseThrow();
            assertThat(failure.code()).isEqualTo("capture.start-from-outside-window");
            assertThat(failure.params()).isEqualTo(params);
            assertThat(failure.requestedPosition().position().token()).isEqualTo("original");
            assertThat(failure.disposition()).isEqualTo("stop-and-clear-source-state");
            boolean mismatchedSources = fixture.profiles.transaction(session -> fixture.meta.guardStartupFailure(session, failure,
                    fixture.pipelineFence(), Set.of("different-source")));
            boolean matchingSources = fixture.profiles.transaction(session -> fixture.meta.guardStartupFailure(session, failure,
                    fixture.pipelineFence(), Set.of("source")));
            assertThat(mismatchedSources).isFalse();
            assertThat(matchingSources).isTrue();
            assertThat(fixture.meta.recordCaptureReadFailure(reader, "connector.capture-failed", Map.of(), "retry-source-start")).isFalse();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            assertThat(fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness).orElseThrow().code()).isEqualTo(failure.code());
        }
    }

    @Test
    void aFailureBeforeThisAttachmentOrFromAnOlderReaderCannotPoisonItsNewAttempt() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.CDC_ONLY);
            CaptureReadAttempt old = fixture.begin("original");
            assertThat(fixture.meta.recordCaptureReadFailure(old, "connector.capture-failed", Map.of("detail", "old-reader"),
                    "retry-source-start")).isTrue();
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            assertThat(fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness)).isEmpty();
            CaptureReadAttempt current = fixture.begin("original");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, current, true)).isTrue();
            assertThat(fixture.meta.recordCaptureReadFailure(old, "connector.capture-failed", Map.of(), "retry-source-start")).isFalse();
            assertThat(fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness)).isEmpty();
            assertThat(fixture.meta.recordCaptureReadFailure(current, "connector.capture-failed", Map.of("detail", "new-reader"),
                    "retry-source-start")).isTrue();
            var failure = fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness).orElseThrow();
            assertThat(failure.params()).containsEntry("detail", "new-reader");
            assertThat(failure.disposition()).isEqualTo("retry-source-start");
            assertThat(fixture.workloads.release(fixture.pipeline)).isTrue();
            assertThat(fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness)).isEmpty();
        }
    }

    @Test
    void anOwnedFailureRecordedWhileCaptureWasLiveSurvivesItsStartupCleanup() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.CDC_ONLY);
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            CaptureReadAttempt reader = fixture.begin("original");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, reader, false)).isTrue();
            Map<String, Object> params = Map.of("requested", "original", "earliest", "retained-head", "retention", "2h");
            assertThat(fixture.meta.recordCaptureReadFailure(reader, "capture.start-from-outside-window", params,
                    "stop-and-clear-source-state")).isTrue();
            var original = fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness).orElseThrow();
            assertThat(fixture.workloads.release(fixture.capture)).isTrue();
            assertThat(fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness)).contains(original);
            assertThat(fixture.meta.captureStartupProof(fixture.pipelineFence(), witness)).isEmpty();
            assertThat(fixture.workloads.release(fixture.pipeline)).isTrue();
            assertThat(fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness)).isEmpty();
        }
    }

    @Test
    void aMalformedFailureClaimProofCannotCertifyAReaderFailure() {
        try (Fixture fixture = new Fixture()) {
            CaptureResumeWitness witness = fixture.prepare(ReadMode.CDC_ONLY);
            assertThat(fixture.meta.recordCaptureAttachment(fixture.pipelineFence(), witness, fixture.epoch)).isTrue();
            CaptureReadAttempt reader = fixture.begin("original");
            assertThat(fixture.meta.bindCaptureReadAttempt(fixture.pipelineFence(), witness, reader, false)).isTrue();
            assertThat(fixture.meta.recordCaptureReadFailure(reader, "capture.start-from-outside-window",
                    Map.of("requested", "original", "earliest", "head", "retention", "2h"), "stop-and-clear-source-state")).isTrue();
            fixture.client.getDatabase(fixture.database).getCollection("meta").updateOne(new Document("_id", "chain"),
                    new Document("$set", new Document("captureReadAttempt.failureClaimProven", "true")));
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.meta.captureStartupFailure(fixture.pipelineFence(), witness))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE));
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        private final String database = "source_start_" + UUID.randomUUID().toString().replace("-", "");
        private final MongoCollection<Document> claims = client.getDatabase(database).getCollection("workload_claims");
        private final MongoClusterProfileStore profiles = new MongoClusterProfileStore(client,
                client.getDatabase(database).getCollection("profiles"), claims, client.getDatabase(database).getCollection("nodes"));
        private final MongoWorkloadClaimStore workloads = new MongoWorkloadClaimStore(claims, profiles);
        private final MongoSrsMetaStore meta = new MongoSrsMetaStore(client, client.getDatabase(database).getCollection("meta"),
                client.getDatabase(database).getCollection("consumers"), claims);
        private final WorkloadClaim pipeline;
        private final WorkloadClaim capture;
        private final String consumer = SrsConsumerId.of("pipeline", "source").value();
        private final long epoch;

        private Fixture() {
            Duration ttl = Duration.ofMinutes(5);
            WorkloadOwner owner = new WorkloadOwner("node", "boot");
            assertThat(profiles.reserve("east", owner, URI.create("http://node:8080"), new ExecutionProfile(1,
                    Map.of("buildVersion", "test")), ttl).acquired()).isTrue();
            WorkloadClaim acquired = workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION,
                    "pipeline"), owner, 1, ttl).claim();
            pipeline = workloads.advanceExecution(acquired, 1, Set.of("node")).orElseThrow();
            capture = workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "capture"), owner, 1, ttl).claim();
            meta.create("chain", null);
            epoch = meta.openEpoch("chain");
            meta.advanceConsumerReadSeq("chain", consumer, "orders", -1);
            assertThat(meta.publishCaptureTables("chain", epoch, List.of("orders"))).isTrue();
            meta.advanceCaptureCheckpoint("chain", new ChainPosition(new SourceOrder(epoch, 10), "original"), List.of("orders"));
        }

        private WorkloadClaimFence pipelineFence() { return WorkloadClaimFence.from(pipeline); }
        private CaptureResumeWitness witness(ReadMode mode) {
            return meta.resumeWitness("source", "mongo", "chain", consumer, mode, true, List.of("orders"));
        }
        private CaptureResumeWitness prepare(ReadMode mode) {
            CaptureResumeWitness witness = witness(mode);
            assertThat(meta.prepareCaptureResume(pipelineFence(), witness, witness.requestedPosition("capture").orElseThrow(),
                    Set.of("source"))).isTrue();
            return witness;
        }
        private CaptureReadAttempt begin(String token) {
            return meta.beginCaptureReadAttempt("chain", epoch, List.of("orders"), CaptureReadAttempt.Kind.RESUME,
                    token, null, WorkloadClaimFence.from(capture)).orElseThrow();
        }
        @Override public void close() { client.getDatabase(database).drop(); client.close(); }
    }
}
