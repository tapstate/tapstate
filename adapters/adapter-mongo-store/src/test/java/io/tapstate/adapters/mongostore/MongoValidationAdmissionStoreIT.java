package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationReportAdmission;
import io.tapstate.spi.store.ValidationReportAdmission.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/** Real CONFIG transactions over explicitly prepared fixture rows, not a production schema-upgrade receipt. */
@RequiresDocker
class MongoValidationAdmissionStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test void twoClientsReserveAndCreateExactlyOneOriginalReport() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            var request = f.start(ValidationMode.FULL, 1, "one"); var gate = new CountDownLatch(1);
            var a = workers.submit(() -> { gate.await(); return f.first.reserve(request); });
            var b = workers.submit(() -> { gate.await(); return f.second.reserve(request); }); gate.countDown();
            assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(Reservation.RESERVED, Reservation.DUPLICATE);
            var opens = new CountDownLatch(1);
            var c = workers.submit(() -> { opens.await(); return f.first.open(request); });
            var d = workers.submit(() -> { opens.await(); return f.second.open(request); }); opens.countDown();
            assertThat(List.of(c.get(10, TimeUnit.SECONDS), d.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(Birth.CREATED, Birth.DUPLICATE);
            assertThat(f.reports().countDocuments()).isOne();
            assertThat(f.second.close(request)).isEqualTo(Closure.CLOSED);
            assertThat(f.reports().find().first().getBoolean("closed")).isTrue();
        }
    }

    @Test void cancellationBeforeBirthRevokesDelayedCreateAndCannotAffectTheSuccessor() {
        try (var f = new Fixture()) {
            var old = f.start(ValidationMode.FULL, 1, "old");
            assertThat(f.first.reserve(old)).isEqualTo(Reservation.RESERVED);
            assertThat(f.second.close(old)).isEqualTo(Closure.CLOSED);
            assertThat(f.first.open(old)).isEqualTo(Birth.CLOSED); assertThat(f.reports().countDocuments()).isZero();
            var next = f.start(ValidationMode.FULL, 2, "new");
            assertThat(f.second.reserve(next)).isEqualTo(Reservation.RESERVED);
            assertThat(f.second.open(next)).isEqualTo(Birth.CREATED);
            assertThat(f.first.open(old)).isEqualTo(Birth.STALE);
            assertThat(f.first.close(old)).isEqualTo(Closure.STALE);
            assertThat(f.reports().find().first().getBoolean("closed")).isFalse();
        }
    }

    @Test void birthAndCancellationRaceCannotLeaveAnOpenReportAfterKnownClosure() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            for (int n = 1; n <= 8; n++) {
                var request = f.start(ValidationMode.FULL, n, "race-" + n);
                assertThat(f.first.reserve(request)).isEqualTo(Reservation.RESERVED);
                var gate = new CountDownLatch(1);
                var open = workers.submit(() -> { gate.await(); return f.first.open(request); });
                var close = workers.submit(() -> { gate.await(); return f.second.close(request); }); gate.countDown();
                assertThat(open.get(10, TimeUnit.SECONDS)).isIn(Birth.CREATED, Birth.CLOSED);
                assertThat(close.get(10, TimeUnit.SECONDS)).isEqualTo(Closure.CLOSED);
                assertThat(f.first.open(request)).isEqualTo(Birth.CLOSED);
            }
            assertThat(f.reports().countDocuments(new Document("closed", false))).isZero();
        }
    }

    @Test void missingBornReportIsLossAndNotPermissionToRecreateOrDeclareClosure() {
        try (var f = new Fixture()) {
            var request = f.start(ValidationMode.FULL, 1, "lost"); f.first.reserve(request); f.first.open(request);
            f.reports().deleteOne(new Document("_id", MongoValidationAdmissionStore.reportId(request.run())));
            var before = f.tasks().find().first();
            assertThatThrownBy(() -> f.second.open(request)).isInstanceOf(TapstateException.class).hasMessageContaining("original-report");
            assertThatThrownBy(() -> f.second.close(request)).isInstanceOf(TapstateException.class).hasMessageContaining("original-report");
            assertThat(f.tasks().find().first()).isEqualTo(before); assertThat(f.reports().countDocuments()).isZero();
        }
    }

    @Test void historyCleanupNeverRestoresAnAlreadyConsumedBirthPermission() {
        try (var f = new Fixture()) {
            var request = f.start(ValidationMode.FULL, 1, "expired"); f.first.reserve(request); f.first.open(request); f.first.close(request);
            f.reports().deleteOne(new Document("_id", MongoValidationAdmissionStore.reportId(request.run())));
            assertThat(f.second.reserve(request)).isEqualTo(Reservation.CLOSED);
            assertThat(f.second.open(request)).isEqualTo(Birth.CLOSED);
            assertThat(f.second.close(request)).isEqualTo(Closure.ALREADY_CLOSED); assertThat(f.reports().countDocuments()).isZero();
        }
    }

    @Test void modesAreIndependentAndAnOpenPermissionBlocksItsOwnSuccessor() {
        try (var f = new Fixture()) {
            var full = f.start(ValidationMode.FULL, 1, "full"); var incremental = f.start(ValidationMode.INCREMENTAL, 1, "incremental");
            assertThat(f.first.reserve(full)).isEqualTo(Reservation.RESERVED);
            assertThat(f.second.reserve(incremental)).isEqualTo(Reservation.RESERVED);
            assertThat(f.second.reserve(f.start(ValidationMode.FULL, 2, "too-soon"))).isEqualTo(Reservation.MODE_BUSY);
            assertThat(f.second.reserve(f.start(ValidationMode.FULL, 1, "conflicting-replay"))).isEqualTo(Reservation.STALE);
            f.first.open(full); f.second.open(incremental); assertThat(f.reports().countDocuments()).isEqualTo(2);
        }
    }

    @Test void deletedAndRecreatedTaskDoesNotInheritOldPermissionOrSequence() {
        try (var f = new Fixture()) {
            var old = f.start(ValidationMode.FULL, 1, "old"); f.first.reserve(old); f.first.close(old);
            f.tasks().deleteOne(new Document("_id", MongoValidationAdmissionStore.taskId(f.task)));
            assertThat(f.second.open(old)).isEqualTo(Birth.STALE);
            f.task = new Task("cluster", "pipeline", "task", UUID.randomUUID()); f.seed();
            assertThat(f.second.reserve(old)).isEqualTo(Reservation.STALE);
            assertThat(f.second.close(old)).isEqualTo(Closure.STALE);
            var next = f.start(ValidationMode.FULL, 1, "new"); f.second.reserve(next);
            assertThat(f.second.open(next)).isEqualTo(Birth.CREATED);
        }
    }

    @Test void failedLocalLifecycleProofRollsBackBothBirthWrites() {
        try (var f = new Fixture()) {
            var request = f.start(ValidationMode.FULL, 1, "rollback"); f.first.reserve(request);
            var calls = new AtomicInteger();
            var guarded = new MongoValidationAdmissionStore(f.client, f.database, "cluster", () -> {
                if (calls.incrementAndGet() == 3) throw new IllegalStateException("scope ended");
            });
            assertThatThrownBy(() -> guarded.open(request)).isInstanceOf(IllegalStateException.class).hasMessage("scope ended");
            assertThat(f.reports().countDocuments()).isZero(); assertThat(f.second.open(request)).isEqualTo(Birth.CREATED);
        }
    }

    @Test void incompleteClosedPermissionCannotAuthorizeASuccessorOrHideItsLoss() {
        try (var f = new Fixture()) {
            var original = f.start(ValidationMode.FULL, 1, "original"); f.first.reserve(original); f.first.close(original);
            var saved = f.tasks().find().first();
            for (String missing : List.of("commandId", "taskGeneration", "configurationRevision", "run", "mode", "specHash", "keyMappingRevision", "comparisonRulesVersion", "owner")) {
                var identity = MongoValidationAdmissionStore.identity(original); identity.remove(missing);
                f.tasks().updateOne(new Document("_id", saved.get("_id")), new Document("$set", new Document("modes.full.permission.identity", identity)));
                var before = f.tasks().find().first();
                assertThatThrownBy(() -> f.second.reserve(f.start(ValidationMode.FULL, 2, "next"))).as(missing).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> f.second.close(original)).as(missing).isInstanceOf(TapstateException.class);
                assertThat(f.tasks().find().first()).isEqualTo(before); assertThat(f.reports().countDocuments()).isZero();
                f.tasks().replaceOne(new Document("_id", saved.get("_id")), saved);
            }
        }
    }

    @Test void aClosedPermissionMustStillMatchItsTaskModeAndTypedIdentity() {
        try (var f = new Fixture()) {
            var original = f.start(ValidationMode.FULL, 1, "original"); f.first.reserve(original); f.first.close(original);
            var saved = f.tasks().find().first(); var candidates = new java.util.ArrayList<Document>();
            var foreign = MongoValidationAdmissionStore.identity(original); foreign.get("run", Document.class).put("pipelineId", "foreign"); candidates.add(foreign);
            var mode = MongoValidationAdmissionStore.identity(original); mode.put("mode", "INCREMENTAL"); candidates.add(mode);
            var generation = MongoValidationAdmissionStore.identity(original); generation.put("taskGeneration", UUID.randomUUID().toString()); candidates.add(generation);
            var owner = MongoValidationAdmissionStore.identity(original); owner.get("owner", Document.class).put("bootId", ""); candidates.add(owner);
            var precision = MongoValidationAdmissionStore.identity(original); precision.put("keyMappingRevision", 1.0); candidates.add(precision);
            var future = MongoValidationAdmissionStore.identity(original); future.put("configurationRevision", 2L); candidates.add(future);
            var extra = MongoValidationAdmissionStore.identity(original); extra.put("unknown", true); candidates.add(extra);
            for (var identity : candidates) {
                f.tasks().updateOne(new Document("_id", saved.get("_id")), new Document("$set", new Document("modes.full.permission.identity", identity)));
                var before = f.tasks().find().first();
                assertThatThrownBy(() -> f.second.reserve(f.start(ValidationMode.FULL, 2, "next"))).isInstanceOf(TapstateException.class);
                assertThat(f.tasks().find().first()).isEqualTo(before); assertThat(f.reports().countDocuments()).isZero();
                f.tasks().replaceOne(new Document("_id", saved.get("_id")), saved);
            }
        }
    }

    @Test void anIndependentlySealedReportCannotBeReturnedAsAnOpenBirthReplay() {
        try (var f = new Fixture()) {
            var request = f.start(ValidationMode.FULL, 1, "sealed"); f.first.reserve(request); f.first.open(request);
            f.reports().updateOne(new Document("_id", MongoValidationAdmissionStore.reportId(request.run())),
                    new Document("$set", new Document("closed", true)));
            assertThat(f.second.open(request)).isEqualTo(Birth.CLOSED);
            assertThat(f.second.close(request)).isEqualTo(Closure.CLOSED);
        }
    }

    @Test void ordinaryAccessDoesNotCreateMissingCollections() {
        try (var f = new Fixture()) {
            var request = f.start(ValidationMode.FULL, 1, "missing"); f.reports().drop();
            assertThatThrownBy(() -> f.first.reserve(request)).isInstanceOf(TapstateException.class).hasMessageContaining("validation-collection");
            assertThat(f.database.listCollectionNames()).doesNotContain("validation_reports");
        }
    }

    @Test void taskClosureFlagSharesTheOriginalAdmissionMetadataBudget() {
        for (int excess : List.of(0, 1)) {
            try (var f = new Fixture()) {
                f.tasks().updateOne(new Document("_id", MongoValidationAdmissionStore.taskId(f.task)),
                        new Document("$set", new Document("closed", false)));
                var full = largeStart(f, ValidationMode.FULL, 0);
                for (int length = 1; length < 510; length++) {
                    var candidate = largeStart(f, ValidationMode.FULL, length);
                    if (bsonBytes(MongoValidationAdmissionStore.identity(candidate)) > 8 * 1024) break;
                    full = candidate;
                }
                Start incremental = null;
                for (int length = 0; length < 510; length++) {
                    var candidate = largeStart(f, ValidationMode.INCREMENTAL, length);
                    var fullFinal = permission(full); fullFinal.get("permission", Document.class).put("phase", "CLOSED");
                    var incrementalFinal = permission(candidate); incrementalFinal.get("permission", Document.class).put("phase", "CLOSED");
                    var modes = new Document("full", fullFinal).append("incremental", incrementalFinal);
                    var metadata = new Document("taskGeneration", f.task.generation().toString()).append("modes", modes).append("closed", false);
                    if (bsonBytes(metadata) == 16 * 1024 + excess) { incremental = candidate; break; }
                }
                assertThat(incremental).as("valid descriptors at the exact BSON boundary").isNotNull();
                assertThat(bsonBytes(MongoValidationAdmissionStore.identity(incremental))).isLessThanOrEqualTo(8 * 1024);
                assertThat(f.first.reserve(full)).isEqualTo(Reservation.RESERVED);
                var original = f.tasks().find().first(); var second = incremental;
                if (excess == 0) {
                    assertThat(f.second.reserve(second)).isEqualTo(Reservation.RESERVED);
                    var task = f.tasks().find().first();
                    assertThat(bsonBytes(new Document("taskGeneration", task.get("taskGeneration"))
                            .append("modes", task.get("modes")).append("closed", task.get("closed")))).isEqualTo(16 * 1024 - 4);
                    var first = full;
                    assertThat(f.first.close(first)).isEqualTo(Closure.CLOSED);
                    assertThat(f.second.close(second)).isEqualTo(Closure.CLOSED);
                    var ended = f.tasks().find().first();
                    assertThat(bsonBytes(new Document("taskGeneration", ended.get("taskGeneration"))
                            .append("modes", ended.get("modes")).append("closed", ended.get("closed")))).isEqualTo(16 * 1024);
                } else {
                    assertThatThrownBy(() -> f.second.reserve(second)).isInstanceOf(TapstateException.class)
                            .hasMessageContaining("admission-bytes");
                    assertThat(f.tasks().find().first()).isEqualTo(original);
                    var oversized = new Document(original); var modes = new Document(original.get("modes", Document.class));
                    modes.put("incremental", permission(second)); oversized.put("modes", modes);
                    f.tasks().replaceOne(new Document("_id", original.get("_id")), oversized);
                    var first = full;
                    assertThatThrownBy(() -> f.first.close(first)).isInstanceOf(TapstateException.class)
                            .hasMessageContaining("admission-bytes");
                    assertThat(f.tasks().find().first()).isEqualTo(oversized);
                }
            }
        }
    }

    private static Start largeStart(Fixture f, ValidationMode mode, int length) {
        var wide = "\u20ac".repeat(512);
        var owner = new ValidationOwner("cluster", "pipeline", wide, wide, 1, 1, 1);
        return new Start(f.task, 1, 1, wide, new ValidationRunIdentity(owner, "task", "run" + "r".repeat(length), mode,
                wide, 1, "\u20ac".repeat(256) + "r".repeat(256)));
    }
    private static Document permission(Start start) {
        return new Document("nextStartSequence", 2L).append("permission", new Document("sequence", 1L)
                .append("phase", "OPEN").append("identity", MongoValidationAdmissionStore.identity(start)));
    }
    private static int bsonBytes(Document value) {
        try (var output = new org.bson.io.BasicOutputBuffer(); var writer = new org.bson.BsonBinaryWriter(output)) {
            new org.bson.codecs.DocumentCodec().encode(writer, value, org.bson.codecs.EncoderContext.builder().build());
            return output.getSize();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoClient other = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoDatabase database = client.getDatabase("validation_admission_" + UUID.randomUUID().toString().replace("-", ""));
        Task task = new Task("cluster", "pipeline", "task", UUID.randomUUID());
        final MongoValidationAdmissionStore first = new MongoValidationAdmissionStore(client, database, "cluster", () -> {});
        final MongoValidationAdmissionStore second = new MongoValidationAdmissionStore(other, other.getDatabase(database.getName()), "cluster", () -> {});
        Fixture() {
            try (var session = client.startSession()) {
                SystemCollections.VALIDATION_TASKS.prepareCollectionOn(database, session);
                SystemCollections.VALIDATION_REPORTS.prepareCollectionOn(database, session);
            }
            seed();
        }
        void seed() {
            tasks().insertOne(new Document("_id", MongoValidationAdmissionStore.taskId(task)).append("formatVersion", 1)
                    .append("taskGeneration", task.generation().toString()).append("revision", 1L)
                    .append("modes", new Document("full", new Document("nextStartSequence", 1L))
                            .append("incremental", new Document("nextStartSequence", 1L))));
        }
        Start start(ValidationMode mode, long sequence, String id) {
            var owner = new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 1);
            return new Start(task, 1, sequence, id, new ValidationRunIdentity(owner, "task", id, mode, "spec", 1, "rules"));
        }
        com.mongodb.client.MongoCollection<Document> tasks() { return SystemCollections.VALIDATION_TASKS.on(database); }
        com.mongodb.client.MongoCollection<Document> reports() { return SystemCollections.VALIDATION_REPORTS.on(database); }
        @Override public void close() { try { database.drop(); } finally { other.close(); client.close(); } }
    }
}
