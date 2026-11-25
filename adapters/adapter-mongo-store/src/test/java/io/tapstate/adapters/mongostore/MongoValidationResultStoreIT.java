package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationReportAdmission;
import io.tapstate.spi.store.ValidationResultStore;
import io.tapstate.spi.store.ValidationResultStore.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** Prepared fixture domains verify CONFIG transaction semantics independently of production bootstrap. */
@RequiresDocker
class MongoValidationResultStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test void completeRootReplacementIsIdempotentAndRejectsConflictingOrOlderObservations() {
        try (var f = new Fixture()) {
            var first = f.write(7, 1, false);
            assertThat(f.first.publish(first)).isEqualTo(Publication.APPLIED);
            assertThat(f.second.publish(f.write(7, 1, false))).isEqualTo(Publication.DUPLICATE);
            assertThatThrownBy(() -> f.second.publish(f.write(7, 1, true))).isInstanceOf(TapstateException.class).hasMessageContaining("conflictingReplay");
            assertThat(f.second.publish(f.write(7, 2, true))).isEqualTo(Publication.APPLIED);
            assertThat(f.first.publish(first)).isEqualTo(Publication.STALE_OBSERVATION);
            assertThat(f.differences().countDocuments()).isOne();
            var stored = f.differences().find().first(); assertThat(stored.getLong("revision")).isEqualTo(2);
            assertThat(stored.get("payload", Document.class).getString("confirmation")).isEqualTo("LOCAL_OBSERVATION");
            assertThat(f.report().getLong("rootCount")).isOne(); assertThat(f.report().getLong("resultBytes")).isEqualTo(stored.getLong("bytes"));
        }
    }

    @Test void equivalentFindingsMapsAreIdempotentRegardlessOfInsertionOrder() {
        try (var f = new Fixture()) {
            var first = multipleFindings(f.start.run(), false); var reverse = multipleFindings(f.start.run(), true);
            assertThat(first.findings()).isEqualTo(reverse.findings());
            assertThat(f.first.publish(new RootWrite(f.start.report(), first))).isEqualTo(Publication.APPLIED);
            assertThat(f.second.publish(new RootWrite(f.start.report(), reverse))).isEqualTo(Publication.DUPLICATE);
        }
    }

    @Test void collectionProbeFailuresRemainCodedWithoutLeakingDriverExceptions() {
        try (var f = new Fixture()) {
            var admin = f.client.getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("listCollections")).append("errorCode", 13)));
            try {
                assertThatThrownBy(() -> f.first.publish(f.write(1, 1, true))).isInstanceOfSatisfying(TapstateException.class,
                        failure -> { assertThat(failure.code()).isEqualTo(io.tapstate.spi.store.IoError.STORE_UNAVAILABLE); assertThat(failure.getCause()).isNull(); });
            } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            assertThat(f.differences().countDocuments()).isZero(); assertThat(f.report().getLong("rootCount")).isZero();
        }
    }

    @Test void invalidUtf16FactsNeverBecomeReplacementTextInStoredEvidence() {
        try (var f = new Fixture()) {
            var malformed = new String(new char[]{(char) 0xd800});
            var write = new RootWrite(f.start.report(), check(f.start.run(), 7, 1, true, Map.of("value", malformed)));
            assertThatThrownBy(() -> f.first.publish(write)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_TYPE_UNSUPPORTED));
            var badName = new RootWrite(f.start.report(), check(f.start.run(), 7, 1, true, Map.of(malformed, "value")));
            assertThatThrownBy(() -> f.first.publish(badName)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_TYPE_UNSUPPORTED));
            assertThat(f.differences().countDocuments()).isZero();
        }
    }

    private static ValidationRootCheck multipleFindings(ValidationRunIdentity run, boolean reverse) {
        var root = new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(7))), List.of());
        try (var result = ValidationRootCheck.builder(new ValidationObservation(run, root, 1), ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, ValidationRootCheck.TargetPresence.PRESENT,
                new ValidationRootCheck.Limits(65536, 10, 10, 10, 0))) {
            result.read(ValidationRootCheck.Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "v1", Instant.EPOCH, Instant.EPOCH));
            result.read(ValidationRootCheck.Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "v1", Instant.EPOCH, Instant.EPOCH));
            for (String field : reverse ? List.of("b", "a") : List.of("a", "b"))
                result.finding(new ValidationRootCheck.Finding(ValidationRootCheck.Kind.TARGET_VALUE, new ValidationRootCheck.Location(root, List.of(field))));
            for (var kind : ValidationRootCheck.Check.values()) result.checked(kind);
            return result.finish();
        }
    }

    @Test void reportSealingClosesTheBirthPermissionAndRejectsAllLaterPublications() {
        try (var f = new Fixture()) {
            f.first.publish(f.write(7, 1, true));
            assertThat(f.second.seal(f.start.report())).isEqualTo(Seal.SEALED);
            assertThat(f.first.seal(f.start.report())).isEqualTo(Seal.ALREADY_SEALED);
            assertThat(f.first.publish(f.write(7, 2, false))).isEqualTo(Publication.CLOSED_OR_EXPIRED);
            assertThat(f.admission.open(f.start)).isEqualTo(ValidationReportAdmission.Birth.CLOSED);
            assertThat(f.differences().find().first().getLong("revision")).isEqualTo(1);
            f.start(2); assertThat(f.first.publish(f.write(7, 1, false))).isEqualTo(Publication.APPLIED);
        }
    }

    @Test void concurrentRootCommitsRetainBothRootsAndExactByteAccounting() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            var gate = new CountDownLatch(1);
            var a = workers.submit(() -> { gate.await(); return f.first.publish(f.write(1, 1, true)); });
            var b = workers.submit(() -> { gate.await(); return f.second.publish(f.write(2, 1, true)); }); gate.countDown();
            assertThat(a.get(10, TimeUnit.SECONDS)).isEqualTo(Publication.APPLIED);
            assertThat(b.get(10, TimeUnit.SECONDS)).isEqualTo(Publication.APPLIED);
            long bytes = 0; for (var row : f.differences().find()) bytes += row.getLong("bytes");
            assertThat(f.report().getLong("rootCount")).isEqualTo(2); assertThat(f.report().getLong("resultBytes")).isEqualTo(bytes);
        }
    }

    @Test void publishAndSealCompeteOnTheSameOriginalReportInRealTransactions() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            for (int n = 1; n <= 8; n++) {
                if (n > 1) f.start(n);
                var write = f.write(n, 1, true); var expected = f.start.report(); var gate = new CountDownLatch(1);
                var a = workers.submit(() -> { gate.await(); return f.first.publish(write); });
                var b = workers.submit(() -> { gate.await(); return f.second.seal(expected); }); gate.countDown();
                assertThat(a.get(10, TimeUnit.SECONDS)).isIn(Publication.APPLIED, Publication.CLOSED_OR_EXPIRED);
                assertThat(b.get(10, TimeUnit.SECONDS)).isEqualTo(Seal.SEALED);
                assertThat(f.first.publish(f.write(n, 2, false))).isEqualTo(Publication.CLOSED_OR_EXPIRED);
                assertThat(f.report().getBoolean("closed")).isTrue();
            }
        }
    }

    @Test void wrongOwnerOrOpenVersionCannotBorrowAnExistingReport() {
        try (var f = new Fixture()) {
            var original = f.start.run(); var owner = original.owner();
            var other = new ValidationRunIdentity(new ValidationOwner(owner.clusterId(), owner.pipelineId(), "other", "new", 2, 2, 1),
                    original.taskId(), original.runId(), original.mode(), original.specHash(), original.keyMappingRevision(), original.comparisonRulesVersion());
            var wrong = new Report(other, 1);
            assertThat(f.first.publish(new RootWrite(wrong, check(other, 7, 1, true, null)))).isEqualTo(Publication.CLOSED_OR_EXPIRED);
            assertThat(f.first.seal(wrong)).isEqualTo(Seal.CLOSED_OR_EXPIRED);
            assertThat(f.first.publish(new RootWrite(new Report(original, 2), check(original, 7, 1, true, null)))).isEqualTo(Publication.CLOSED_OR_EXPIRED);
            assertThat(f.report().getBoolean("closed")).isFalse(); assertThat(f.differences().countDocuments()).isZero();
        }
    }

    @Test void typedSamplesPreserveExactNumbersNanosecondsBinaryAndFieldNames() {
        try (var f = new Fixture()) {
            var fields = new LinkedHashMap<String, Object>(); fields.put("a.b", new BigDecimal("9007199254740993.000100"));
            fields.put("$field", null); fields.put("validUnicode", "nul" + (char)0 + new String(Character.toChars(0x1f680))); fields.put("time", Instant.ofEpochSecond(23, 987654321));
            fields.put("binary", new ValidationValues.Binary(7, new byte[]{0, -1, 2})); fields.put("negativeZero", -0.0d);
            fields.put("array", List.of(new ValidationValues.Typed("decimal", "numeric", "custom", "custom-to", new BigDecimal("1.20"))));
            var write = new RootWrite(f.start.report(), check(f.start.run(), 9007199254740993L, 1, true, fields));
            assertThat(f.first.publish(write)).isEqualTo(Publication.APPLIED);
            assertThat(f.second.publish(new RootWrite(f.start.report(), check(f.start.run(), 9007199254740993L, 1, true, fields)))).isEqualTo(Publication.DUPLICATE);
            var sample = f.differences().find().first().get("payload", Document.class).getList("samples", Document.class).getFirst();
            var row = sample.get("source", Document.class).get("row", Document.class);
            var values = new HashMap<String, Document>(); for (var entry : row.getList("entries", Document.class)) values.put(entry.getString("name"), entry.get("value", Document.class));
            assertThat(values.get("a.b").getString("unscaled")).isEqualTo("9007199254740993000100");
            assertThat(values.get("a.b").getInteger("scale")).isEqualTo(6);
            assertThat(values.get("validUnicode").getString("value")).isEqualTo(fields.get("validUnicode"));
            assertThat(values.get("$field").getString("kind")).isEqualTo("NULL"); assertThat(values).doesNotContainKey("missing");
            assertThat(values.get("time").get("value", Document.class).getInteger("nanos")).isEqualTo(987654321);
            assertThat(values.get("binary").getInteger("subtype")).isEqualTo(7);
            assertThat(values.get("binary").get("value", org.bson.types.Binary.class).getData()).containsExactly(0, -1, 2);
            assertThat(values.get("negativeZero").getLong("value")).isEqualTo(Double.doubleToRawLongBits(-0.0d));
            assertThat(values.get("array").getList("values", Document.class).getFirst().getString("codecBinding")).isEqualTo("custom");
        }
    }

    @Test void lossAfterRootInsertRollsBackAccountingAndPayloadTogether() {
        try (var f = new Fixture()) {
            var active = new java.util.concurrent.atomic.AtomicBoolean(true);
            var settings = com.mongodb.MongoClientSettings.builder().applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(new com.mongodb.event.CommandListener() {
                        @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                            if (event.getCommandName().equals("insert") && event.getCommand().getString("insert").getValue().equals("validation_differences")) active.set(false);
                        }
                    }).build();
            var before = f.report();
            try (var client = MongoClients.create(settings)) {
                var guarded = new MongoValidationResultStore(client, client.getDatabase(f.database.getName()), "cluster", () -> {
                    if (!active.get()) throw new IllegalStateException("owner ended");
                });
                assertThatThrownBy(() -> guarded.publish(f.write(1, 1, true))).isInstanceOf(IllegalStateException.class).hasMessage("owner ended");
            }
            assertThat(f.report()).isEqualTo(before); assertThat(f.differences().countDocuments()).isZero();
            assertThat(f.first.publish(f.write(1, 1, true))).isEqualTo(Publication.APPLIED);
        }
    }

    @Test void uncertainCommitRetryCannotDuplicateTheRootOrItsByteCharge() {
        try (var f = new Fixture()) {
            var admin = f.client.getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("commitTransaction")).append("errorCode", 91)
                            .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
            try { assertThat(f.first.publish(f.write(7, 1, true))).isEqualTo(Publication.APPLIED); }
            finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            assertThat(f.differences().countDocuments()).isOne(); assertThat(f.report().getLong("rootCount")).isOne();
            assertThat(f.report().getLong("resultBytes")).isEqualTo(f.differences().find().first().getLong("bytes"));
        }
    }

    @Test void aTooLargeRootFailsBeforeAnyPersistentCounterOrPartialObservation() {
        try (var f = new Fixture()) {
            var before = f.report(); var write = new RootWrite(f.start.report(), check(f.start.run(), 7, 1, true, Map.of("large", "x".repeat(150000))));
            assertThatThrownBy(() -> f.first.publish(write)).isInstanceOf(TapstateException.class).hasMessageContaining("result-bytes");
            assertThat(f.report()).isEqualTo(before); assertThat(f.differences().countDocuments()).isZero();
        }
    }

    @Test void absentReportAndMissingCollectionAreNeverCreatedByPublish() {
        try (var f = new Fixture()) {
            SystemCollections.VALIDATION_REPORTS.on(f.database).deleteOne(new Document("_id", MongoValidationAdmissionStore.reportId(f.start.run())));
            assertThat(f.first.publish(f.write(1, 1, true))).isEqualTo(Publication.CLOSED_OR_EXPIRED);
            assertThat(f.differences().countDocuments()).isZero(); f.differences().drop();
            assertThatThrownBy(() -> f.first.publish(f.write(1, 1, true))).isInstanceOf(TapstateException.class).hasMessageContaining("validation-differences");
            assertThat(f.database.listCollectionNames()).doesNotContain("validation_differences");
        }
    }

    static ValidationRootCheck check(ValidationRunIdentity run, long key, long revision, boolean wrong, Map<String, Object> fields) {
        var root = new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(key))), List.of());
        var observation = new ValidationObservation(run, root, revision);
        try (var result = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                ValidationRootCheck.TargetPresence.PRESENT, new ValidationRootCheck.Limits(2 * 1024 * 1024, 10, 10, 10, 1))) {
            result.read(ValidationRootCheck.Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "v1", Instant.EPOCH, Instant.EPOCH));
            result.read(ValidationRootCheck.Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "v1", Instant.EPOCH, Instant.EPOCH));
            if (wrong) {
                var finding = new ValidationRootCheck.Finding(ValidationRootCheck.Kind.TARGET_VALUE, new ValidationRootCheck.Location(root, List.of("amount")));
                if (fields == null) result.finding(finding);
                else result.finding(finding, Optional.of(new ValidationRootCheck.SampledSide(
                        new ValidationPhysicalIdentity("source", "db", null, "orders", List.of(List.of("id")), root.rootKey()),
                        ValidationValues.ownRow(fields, new ValidationValues.Limits(1024 * 1024, 128, 16)), "source")), Optional.empty());
            }
            for (var kind : ValidationRootCheck.Check.values()) result.checked(kind);
            return result.finish();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl()), other = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoDatabase database = client.getDatabase("validation_results_" + UUID.randomUUID().toString().replace("-", ""));
        final ValidationReportAdmission.Task task = new ValidationReportAdmission.Task("cluster", "pipeline", "task", UUID.randomUUID());
        final MongoValidationAdmissionStore admission = new MongoValidationAdmissionStore(client, database, "cluster", () -> {});
        final MongoValidationResultStore first = new MongoValidationResultStore(client, database, "cluster", () -> {});
        final MongoValidationResultStore second = new MongoValidationResultStore(other, other.getDatabase(database.getName()), "cluster", () -> {});
        ValidationReportAdmission.Start start;
        Fixture() {
            try (var session = client.startSession()) {
                for (var table : List.of(SystemCollections.VALIDATION_TASKS, SystemCollections.VALIDATION_REPORTS, SystemCollections.VALIDATION_DIFFERENCES)) {
                    table.prepareCollectionOn(database, session);
                    for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
                }
            }
            SystemCollections.VALIDATION_TASKS.on(database).insertOne(new Document("_id", MongoValidationAdmissionStore.taskId(task)).append("formatVersion", 1)
                    .append("taskGeneration", task.generation().toString()).append("revision", 1L)
                    .append("modes", new Document("full", new Document("nextStartSequence", 1L)).append("incremental", new Document("nextStartSequence", 1L))));
            start(1);
        }
        void start(long sequence) {
            var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 1), "task", "run-" + sequence,
                    ValidationMode.FULL, "spec", 1, "rules");
            start = new ValidationReportAdmission.Start(task, 1, sequence, "command-" + sequence, run);
            assertThat(admission.reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
            assertThat(admission.open(start)).isEqualTo(ValidationReportAdmission.Birth.CREATED);
        }
        RootWrite write(long key, long revision, boolean wrong) { return new RootWrite(start.report(), check(start.run(), key, revision, wrong, null)); }
        Document report() { return SystemCollections.VALIDATION_REPORTS.on(database).find(new Document("_id", MongoValidationAdmissionStore.reportId(start.run()))).first(); }
        com.mongodb.client.MongoCollection<Document> differences() { return SystemCollections.VALIDATION_DIFFERENCES.on(database); }
        @Override public void close() { try { database.drop(); } finally { other.close(); client.close(); } }
    }
}
