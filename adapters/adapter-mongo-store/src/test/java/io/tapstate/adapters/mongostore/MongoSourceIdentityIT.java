package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceIdentityPreparation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoSourceIdentityIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    @Test void aMatchedButUnchangedKeyRowCannotAuthorizeTheSourceBindingFence() {
        withConfiguration(false, fixture -> {
            fixture.domain().artifacts().create(source("orders", null, "first"));
            var captured = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            SystemCollections.CLUSTER_KEYS.on(fixture.database()).updateOne(new Document(),
                    new Document("$set", new Document("sourceWriteFence", Double.POSITIVE_INFINITY)));
            assertThatThrownBy(() -> transaction(fixture, session -> fixture.originalClientStore().fenceSourceBinding(session, captured.identity())))
                    .isInstanceOf(TapstateException.class);
            assertThat(header(fixture, "orders").writeFence()).isZero();
        });
    }

    @Test void sourceLifetimeSurvivesEditsButNotDeletionAndSameContentRecreation() {
        withConfiguration(false, fixture -> {
            var source = source("orders", null, "first");
            assertThat(fixture.domain().artifacts().create(source)).isEqualTo(ArtifactMutation.CREATED);
            var first = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            var display = source("orders", new Metadata(Map.of("team", "other"), "Updated display"), "first");
            assertThat(fixture.domain().artifacts().replace("orders", first.contentHash(), display)).isEqualTo(ArtifactMutation.REPLACED);
            var renamed = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            assertThat(renamed.identity()).isEqualTo(first.identity());
            fixture.domain().artifacts().save(source("orders", null, "second"));
            assertThat(fixture.domain().artifacts().sourceBinding("orders").orElseThrow().identity().bindingRevision()).isEqualTo(2);
            fixture.domain().artifacts().save(source);
            var reverted = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            assertThat(reverted.contentHash()).isEqualTo(first.contentHash());
            assertThat(reverted.identity().bindingRevision()).isEqualTo(3);
            assertThat(reverted.identity().incarnation()).isEqualTo(first.identity().incarnation());
            assertThat(fixture.domain().artifacts().delete("orders", reverted.contentHash())).isEqualTo(ArtifactMutation.DELETED);
            assertThat(fixture.domain().artifacts().sourceBinding("orders")).isEmpty();
            fixture.domain().artifacts().create(source);
            var recreated = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            assertThat(recreated.identity().incarnation()).isNotEqualTo(first.identity().incarnation());
            assertThat(recreated.contentHash()).isEqualTo(first.contentHash());
        });
    }

    @Test void aFenceIsAtomicWithItsCallerAndDoesNotChangeTheCapturedBinding() {
        withConfiguration(false, fixture -> {
            fixture.domain().artifacts().create(source("orders", null, "first"));
            var captured = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            var store = fixture.originalClientStore();
            assertThat(MongoSourceIdentityIT.<Boolean>transaction(fixture, session -> store.fenceSourceBinding(session, captured.identity()))).isTrue();
            assertThat(header(fixture, "orders").writeFence()).isEqualTo(1);
            assertThat(fixture.domain().artifacts().sourceBinding("orders").orElseThrow().identity()).isEqualTo(captured.identity());
            assertThatThrownBy(() -> transaction(fixture, session -> {
                assertThat(store.fenceSourceBinding(session, captured.identity())).isTrue(); throw new InterruptedCommit();
            })).isInstanceOf(InterruptedCommit.class);
            assertThat(header(fixture, "orders").writeFence()).isEqualTo(1);
            fixture.domain().artifacts().save(source("orders", null, "second"));
            assertThat(header(fixture, "orders").writeFence()).isEqualTo(1);
            assertThat(MongoSourceIdentityIT.<Boolean>transaction(fixture, session -> store.fenceSourceBinding(session, captured.identity()))).isFalse();
        });
    }

    @Test void usedIdentityLossRefusesReadReplaceAndDeleteWithoutMintingAnotherUuid() {
        withConfiguration(false, fixture -> {
            var source = source("orders", null, "first"); fixture.domain().artifacts().create(source);
            var original = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            SystemCollections.SOURCES.on(fixture.database()).updateOne(new Document("_id", "orders"),
                    new Document("$unset", new Document("sourceIdentity", "")));
            assertCode(() -> fixture.domain().artifacts().sourceBinding("orders"), IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> fixture.domain().artifacts().replace("orders", original.contentHash(), source), IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> fixture.domain().artifacts().delete("orders", original.contentHash()), IoError.DOCUMENT_UNREADABLE);
            assertThat(row(fixture, "orders")).doesNotContainKey("sourceIdentity");
            assertThat(fixture.domain().artifacts().get("orders")).contains(source);
        });
    }

    @Test void aConcurrentDeleteInvalidatesTheOriginalSnapshotBeforeItsFenceCanCommit() {
        withConfiguration(false, fixture -> {
            fixture.domain().artifacts().create(source("orders", null, "first"));
            var captured = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            var snapshotRead = new CountDownLatch(1);
            var resume = new CountDownLatch(1);
            var firstAttempt = new AtomicBoolean(true);
            try (var worker = Executors.newSingleThreadExecutor()) {
                var pending = worker.submit(() -> transaction(fixture, session -> {
                    SystemCollections.SOURCES.on(fixture.database()).find(session, new Document("_id", "orders")).first();
                    if (firstAttempt.getAndSet(false)) { snapshotRead.countDown(); await(resume); }
                    return fixture.originalClientStore().fenceSourceBinding(session, captured.identity());
                }));
                try {
                    await(snapshotRead);
                    assertThat(fixture.domain().artifacts().delete("orders", captured.contentHash())).isEqualTo(ArtifactMutation.DELETED);
                    fixture.domain().artifacts().create(captured.source());
                } finally { resume.countDown(); }
                assertThat(pending.get(10, TimeUnit.SECONDS)).isFalse();
                assertThat(header(fixture, "orders").binding().incarnation()).isNotEqualTo(captured.identity().incarnation());
                assertThat(header(fixture, "orders").writeFence()).isZero();
            } catch (Exception failure) { throw new AssertionError(failure); }
        });
    }

    @Test void systemDefaultSourceHasOneStableIdentityThroughTheSeparateBindingWriter() {
        withConfiguration(false, fixture -> {
            var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null);
            var master = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
            var scope = new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1);
            fixture.domain().bindings().commit(scope, StorageBinding.Mode.OP, cache, master);
            var first = fixture.domain().artifacts().sourceBinding("views").orElseThrow();
            fixture.domain().bindings().commit(scope, StorageBinding.Mode.OP, cache, master);
            assertThat(fixture.domain().artifacts().sourceBinding("views").orElseThrow().identity()).isEqualTo(first.identity());
            assertThat(row(fixture, "views")).containsKey("systemRole");
        });
    }

    @Test void legacyAccessDoesNotInventIdentityAndBoundedPreparationResumesTheOriginalOperation() {
        withConfiguration(true, fixture -> {
            Map<String, Document> before = new LinkedHashMap<>();
            for (int number = 0; number < 23; number++) {
                String id = "source-" + String.format(java.util.Locale.ROOT, "%02d", number);
                fixture.domain().artifacts().create(source(id, null, "original")); before.put(id, row(fixture, id));
            }
            assertCode(() -> fixture.domain().artifacts().sourceBinding("source-00"), IoError.SOURCE_IDENTITY_UNAVAILABLE);
            assertThat(fixture.domain().sourceIdentityPreparation()).isEmpty();
            var firstPass = fixture.domain().prepareSourceIdentities("source-upgrade-a");
            assertThat(firstPass.phase()).isEqualTo(SourceIdentityPreparation.Phase.PREPARING);
            assertThat(firstPass.preparedCount()).isEqualTo(20);
            var firstIdentity = header(fixture, "source-00").binding();
            assertCode(() -> fixture.domain().artifacts().create(source("new-source", null, "other")), IoError.SOURCE_IDENTITY_UNAVAILABLE);
            assertCode(() -> fixture.domain().artifacts().sourceBinding("source-00"), IoError.SOURCE_IDENTITY_UNAVAILABLE);
            assertCode(() -> fixture.domain().prepareSourceIdentities("other-operation"), IoError.DOCUMENT_UNREADABLE);
            try (var reopened = MongoConfigurationDomain.open(fixture.settings(), "cluster-a")) {
                var completed = reopened.prepareSourceIdentities("source-upgrade-a");
                assertThat(completed.phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
                assertThat(completed.preparedCount()).isEqualTo(23);
                assertThat(reopened.prepareSourceIdentities("does-not-replace-the-winner").operationId()).isEqualTo("source-upgrade-a");
            }
            assertThat(header(fixture, "source-00").binding()).isEqualTo(firstIdentity);
            for (var entry : before.entrySet()) {
                Document after = row(fixture, entry.getKey());
                assertThat(after.get("body")).isEqualTo(entry.getValue().get("body"));
                assertThat(after.get("contentHash")).isEqualTo(entry.getValue().get("contentHash"));
                assertThat(fixture.domain().artifacts().sourceBinding(entry.getKey())).isPresent();
            }
        });
    }

    @Test void unmarkedPartialHeadersCannotBeAdoptedAsANewPreparation() {
        withConfiguration(true, fixture -> {
            fixture.domain().artifacts().create(source("orders", null, "original"));
            SystemCollections.SOURCES.on(fixture.database()).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("sourceIdentity", MongoSourceIdentity.encode(MongoSourceIdentity.create("orders")))));
            var before = row(fixture, "orders").get("sourceIdentity");
            assertCode(() -> fixture.domain().prepareSourceIdentities("cannot-adopt"), IoError.DOCUMENT_UNREADABLE);
            assertThat(fixture.domain().sourceIdentityPreparation()).isEmpty();
            assertThat(row(fixture, "orders").get("sourceIdentity")).isEqualTo(before);
        });
    }

    @Test void maintenanceUsesDatabaseIdOrderForSupplementaryUnicodeIds() {
        withConfiguration(true, fixture -> {
            String first = "\ue000";
            String second = "\ud800\udc00";
            fixture.domain().artifacts().create(source(first, null, "first"));
            fixture.domain().artifacts().create(source(second, null, "second"));
            var result = fixture.domain().prepareSourceIdentities("unicode-order");
            assertThat(result.phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
            assertThat(result.preparedCount()).isEqualTo(2);
            assertThat(fixture.domain().artifacts().sourceBinding(first)).isPresent();
            assertThat(fixture.domain().artifacts().sourceBinding(second)).isPresent();
        });
    }

    @Test void existingLongIdsRemainWritableAndCanBePreparedWithoutAnUnboundedCursor() {
        List<String> ids = List.of("a".repeat(300), "b".repeat(4096), "\ud83d\ude80".repeat(100));
        withConfiguration(false, fixture -> {
            for (String id : ids) {
                assertThat(fixture.domain().artifacts().create(source(id, null, "original"))).isEqualTo(ArtifactMutation.CREATED);
                assertThat(fixture.domain().artifacts().sourceBinding(id)).isPresent();
            }
        });
        withConfiguration(true, fixture -> {
            for (String id : ids) { fixture.domain().artifacts().create(source(id, null, "original")); }
            var completed = fixture.domain().prepareSourceIdentities("long-ids");
            assertThat(completed.phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
            assertThat(completed.preparedCount()).isEqualTo(3);
            for (String id : ids) { assertThat(fixture.domain().artifacts().sourceBinding(id)).isPresent(); }
            var marker = SystemCollections.SCHEMA_MIGRATIONS.on(fixture.database()).find(new Document("_id", "config")).first();
            assertThat(MongoSourceIdentityPreparation.size(new Document("sourceIdentity", marker.get("sourceIdentity"))))
                    .isLessThanOrEqualTo(MongoSourceIdentitySchema.MAX_CONTROL_BYTES);
        });
    }

    @Test void aCapturedBindingCannotFenceAfterTheActualOriginalKeyIsLostOrCorrupt() {
        withConfiguration(false, fixture -> {
            fixture.domain().artifacts().create(source("orders", null, "original"));
            var captured = fixture.domain().artifacts().sourceBinding("orders").orElseThrow();
            var keys = SystemCollections.CLUSTER_KEYS.on(fixture.database());
            Document original = keys.find(new Document("_id", "cluster")).first();
            keys.deleteOne(new Document("_id", "cluster"));
            assertThatThrownBy(() -> transaction(fixture, session -> fixture.originalClientStore().fenceSourceBinding(session, captured.identity())))
                    .isInstanceOf(TapstateException.class);
            keys.insertOne(new Document(original).append("material", "invalid-original-key"));
            assertThatThrownBy(() -> transaction(fixture, session -> fixture.originalClientStore().fenceSourceBinding(session, captured.identity())))
                    .isInstanceOf(TapstateException.class);
            assertThat(header(fixture, "orders").writeFence()).isZero();
        });
    }

    @Test void anUnknownEnrollmentCommitResumesWithoutReplacingCommittedIdentitiesOrDoubleCounting() {
        withConfiguration(true, fixture -> {
            for (int number = 0; number < 21; number++) {
                fixture.domain().artifacts().create(source("source-" + String.format(java.util.Locale.ROOT, "%02d", number), null, "original"));
            }
            assertThat(fixture.domain().prepareSourceIdentities("unknown-commit").preparedCount()).isEqualTo(20);
            var first = header(fixture, "source-00");
            var admin = fixture.client().getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("commitTransaction"))
                            .append("writeConcernError", new Document("code", 64).append("errmsg", "test commit reply lost"))
                            .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
            try {
                var completed = fixture.domain().prepareSourceIdentities("unknown-commit");
                assertThat(completed.phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
                assertThat(completed.preparedCount()).isEqualTo(21);
                var last = header(fixture, "source-20");
                assertThat(fixture.domain().prepareSourceIdentities("unknown-commit")).isEqualTo(completed);
                assertThat(header(fixture, "source-20")).isEqualTo(last);
                assertThat(header(fixture, "source-00")).isEqualTo(first);
            } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
        });
    }

    @Test void invalidNextDefinitionLeavesTheCommittedCursorAndItsOriginalCiphertextAvailableForRecovery() {
        withConfiguration(true, fixture -> {
            fixture.domain().artifacts().create(source("a-valid", null, "original"));
            fixture.domain().artifacts().create(source("b-invalid", null, "original"));
            Document original = row(fixture, "b-invalid");
            SystemCollections.SOURCES.on(fixture.database()).updateOne(new Document("_id", "b-invalid"),
                    new Document("$set", new Document("contentHash", "0".repeat(64))));
            assertThatThrownBy(() -> fixture.domain().prepareSourceIdentities("definition-check"))
                    .isInstanceOf(TapstateException.class);
            var progress = fixture.domain().sourceIdentityPreparation().orElseThrow();
            assertThat(progress.phase()).isEqualTo(SourceIdentityPreparation.Phase.PREPARING);
            assertThat(progress.preparedCount()).isEqualTo(1);
            var first = header(fixture, "a-valid");
            assertThat(progress.lastSourceIncarnation()).isEqualTo(first.binding().incarnation());
            assertThat(row(fixture, "b-invalid")).doesNotContainKey("sourceIdentity");
            SystemCollections.SOURCES.on(fixture.database()).replaceOne(new Document("_id", "b-invalid"), original);
            assertThat(fixture.domain().prepareSourceIdentities("definition-check").phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
            assertThat(header(fixture, "a-valid")).isEqualTo(first);
            assertThat(row(fixture, "b-invalid").get("body")).isEqualTo(original.get("body"));
        });
    }

    @Test void theVerificationByteBudgetYieldsBeforeTheSourceCountLimitAndResumes() {
        withConfiguration(true, fixture -> {
            String value = "v".repeat(9 * 1024 * 1024);
            for (int index = 0; index < 3; index++) {
                fixture.domain().artifacts().create(new SourceResource("large-" + index, null, "mongodb",
                        Map.of("value", value), null, null, null, null));
            }
            assertThat(MongoSourceIdentityPreparation.size(row(fixture, "large-0")))
                    .isGreaterThan((int) (MongoSourceIdentityPreparation.MAX_READ_BYTES / 3));
            var first = fixture.domain().prepareSourceIdentities("byte-bounded");
            assertThat(first.phase()).isEqualTo(SourceIdentityPreparation.Phase.PREPARING);
            assertThat(first.preparedCount()).isEqualTo(2);
            var original = header(fixture, "large-0");
            assertThat(fixture.domain().prepareSourceIdentities("byte-bounded").phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
            assertThat(header(fixture, "large-0")).isEqualTo(original);
        });
    }

    @Test void aLostCommittedCursorRefusesInsteadOfRemintingItsPreviouslyPublishedIdentity() {
        withConfiguration(true, fixture -> {
            for (int index = 0; index < 21; index++) {
                fixture.domain().artifacts().create(source("source-" + String.format(java.util.Locale.ROOT, "%02d", index), null, "original"));
            }
            var before = fixture.domain().prepareSourceIdentities("lost-cursor");
            assertThat(before.preparedCount()).isEqualTo(20);
            SystemCollections.SOURCES.on(fixture.database()).updateOne(new Document("_id", "source-19"),
                    new Document("$unset", new Document("sourceIdentity", "")));
            assertCode(() -> fixture.domain().prepareSourceIdentities("lost-cursor"), IoError.DOCUMENT_UNREADABLE);
            assertThat(fixture.domain().sourceIdentityPreparation()).contains(before);
            assertThat(row(fixture, "source-19")).doesNotContainKey("sourceIdentity");
            assertThat(row(fixture, "source-20")).doesNotContainKey("sourceIdentity");
        });
    }

    @Test void cursorAndTransactionProjectionReadsShareTheSameByteAllowanceAsDefinitionValidation() {
        withConfiguration(true, fixture -> {
            List<String> ids = java.util.stream.IntStream.range(0, 10)
                    .mapToObj(index -> String.format(java.util.Locale.ROOT, "%02d", index) + "a".repeat(1024 * 1024)).toList();
            for (String id : ids) { fixture.domain().artifacts().create(source(id, null, "original")); }
            var first = fixture.domain().prepareSourceIdentities("projection-budget");
            assertThat(first.phase()).isEqualTo(SourceIdentityPreparation.Phase.PREPARING);
            assertThat(first.preparedCount()).isBetween(1L, 9L);
            var original = header(fixture, ids.getFirst());
            var completed = fixture.domain().prepareSourceIdentities("projection-budget");
            assertThat(completed.phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
            assertThat(completed.preparedCount()).isEqualTo(10);
            assertThat(header(fixture, ids.getFirst())).isEqualTo(original);
        });
    }

    @Test void aLargeOriginalCursorStillAllowsTheNextValidBsonSourceToMakeProgress() {
        withConfiguration(true, fixture -> {
            // The original replica-set oplog entry must also fit when it repeats the document key.
            String first = "a".repeat(5 * 1024 * 1024);
            String second = "b".repeat(5 * 1024 * 1024);
            var codec = new EncryptedArtifactCodec(fixture.cipher());
            for (String id : List.of(first, second)) {
                Document legacy = codec.encode(source(id, null, "original"));
                assertThat(MongoSourceIdentityPreparation.size(legacy)).isLessThan(16 * 1024 * 1024);
                SystemCollections.SOURCES.on(fixture.database()).insertOne(legacy);
            }
            var initial = fixture.domain().prepareSourceIdentities("large-cursor");
            assertThat(initial.preparedCount()).isEqualTo(1);
            var original = header(fixture, first);
            var next = fixture.domain().prepareSourceIdentities("large-cursor");
            assertThat(next.preparedCount()).isEqualTo(2);
            assertThat(fixture.domain().prepareSourceIdentities("large-cursor").phase()).isEqualTo(SourceIdentityPreparation.Phase.READY);
            assertThat(header(fixture, first)).isEqualTo(original);
            assertThat(header(fixture, second).binding().incarnation()).isNotEqualTo(original.binding().incarnation());
        });
    }

    @Test void counterBoundsRefuseWithoutWrappingOrChangingTheDefinition() {
        withConfiguration(false, fixture -> {
            var source = source("orders", null, "first"); fixture.domain().artifacts().create(source);
            var identity = fixture.domain().artifacts().sourceBinding("orders").orElseThrow().identity();
            SystemCollections.SOURCES.on(fixture.database()).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("sourceIdentity.writeFence", Long.MAX_VALUE)));
            assertCode(() -> transaction(fixture, session -> fixture.originalClientStore().fenceSourceBinding(session, identity)),
                    IoError.SOURCE_IDENTITY_EXHAUSTED);
            SystemCollections.SOURCES.on(fixture.database()).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("sourceIdentity.bindingRevision", Long.MAX_VALUE)));
            assertCode(() -> fixture.domain().artifacts().save(source("orders", null, "different")), IoError.SOURCE_IDENTITY_EXHAUSTED);
            assertThat(fixture.domain().artifacts().get("orders")).contains(source);
            assertThat(header(fixture, "orders").writeFence()).isEqualTo(Long.MAX_VALUE);
        });
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoConfigurationDomain domain,
            MongoConnectionSettings settings, SourceConfigCipher cipher) {
        MongoConfigurationArtifactStore originalClientStore() { return new MongoConfigurationArtifactStore(client, database, cipher); }
    }
    private static void withConfiguration(boolean legacy, Consumer<Fixture> scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("source_identity_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try {
                try (var first = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1)) {
                    assertThat(first.clusterId()).isEqualTo("cluster-a");
                }
                if (legacy) {
                    SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(new Document("_id", "config"),
                            new Document("$unset", new Document(MongoSourceIdentitySchema.FIELD, "")));
                }
                byte[] key = Base64.getUrlDecoder().decode(SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material"));
                SourceConfigCipher cipher;
                try { cipher = new SourceConfigCipher(key); } finally { java.util.Arrays.fill(key, (byte) 0); }
                try (var domain = MongoConfigurationDomain.open(settings, "cluster-a")) {
                    scenario.accept(new Fixture(client, database, domain, settings, cipher));
                }
            } finally { database.drop(); }
        }
    }
    private static <T> T transaction(Fixture fixture, Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try (var session = fixture.client().startSession()) {
                return session.withTransaction(() -> action.apply(session), TRANSACTION);
            }
        });
    }
    private static SourceResource source(String id, Metadata metadata, String database) {
        return new SourceResource(id, metadata, "mongodb", Map.of("database", database), null, null, null, null);
    }
    private static Document row(Fixture fixture, String id) { return SystemCollections.SOURCES.on(fixture.database()).find(new Document("_id", id)).first(); }
    private static MongoSourceIdentity.Header header(Fixture fixture, String id) { return MongoSourceIdentity.read(row(fixture, id)); }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(code));
    }
    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static final class InterruptedCommit extends RuntimeException { }
}
