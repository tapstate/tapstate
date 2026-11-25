package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.User;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration transaction witnesses; loopback transport, audit dispatch and live storage tests remain caller concerns. */
@RequiresDocker
class MongoFirstAdminStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final StorageSetupScope SCOPE = new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1);
    private static final StorageBinding.Connection CACHE = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
            "mongodb://cache.invalid/cache", "cache", null);
    private static final User ADMIN = new User("operator", "hashed-password", "admin");

    @Test
    void setupCompletionAndPendingAreAtomicAndIdempotentWhileOrdinaryUserSaveCannotBootstrap() {
        withConfiguration((domain, database) -> {
            assertThat(domain.firstAdmin().load()).isEmpty();
            assertThatThrownBy(() -> domain.bindings().complete(SCOPE)).satisfies(MongoFirstAdminStoreIT::refused);
            bind(domain, StorageBinding.Mode.OP);
            assertThat(domain.firstAdmin().load()).isEmpty();
            assertThatThrownBy(() -> domain.firstAdmin().create(SCOPE, ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
            domain.bindings().complete(SCOPE);
            var pending = domain.firstAdmin().load().orElseThrow();
            assertThat(pending.phase()).isEqualTo(FirstAdminStore.Phase.PENDING);
            assertThat(pending.username()).isNull();
            assertThat(pending.scope()).isEqualTo(SCOPE);
            assertThat(domain.users().isEmpty()).isTrue();
            assertThatThrownBy(() -> domain.users().save(ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
            assertThat(revision(database)).isEqualTo(2);
            domain.bindings().complete(SCOPE);
            assertThat(revision(database)).isEqualTo(2);
            assertThat(SystemCollections.OP_BOOTSTRAP.on(database).countDocuments()).isOne();
        });
    }

    @Test
    void firstAdminAndPermanentCompletionSurviveReopenWithoutCacheAndNoReplayChangesThePassword() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP);
            domain.bindings().complete(SCOPE);
            var receipt = domain.firstAdmin().create(SCOPE, ADMIN);
            assertThat(receipt.phase()).isEqualTo(FirstAdminStore.Phase.COMPLETE);
            assertThat(receipt.username()).isEqualTo(ADMIN.username());
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                assertThat(reopened.firstAdmin().load()).contains(receipt);
                assertThat(reopened.users().find(ADMIN.username())).contains(ADMIN);
                assertThatThrownBy(() -> reopened.firstAdmin().create(SCOPE, new User(ADMIN.username(), "replacement-hash", "admin")))
                        .satisfies(MongoFirstAdminStoreIT::refused);
                assertThat(reopened.users().find(ADMIN.username())).contains(ADMIN);
            }
            assertThat(revision(database)).isEqualTo(3);
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain("sessions", "op_sessions", "cloud_sessions", "workload_claims", "pipeline_state");
        });
    }

    @Test
    void twoIndependentClientsCanOnlyInsertOneFirstAdministrator() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE);
            try (var other = MongoConfigurationDomain.open(settings(database), "cluster-a");
                    var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> createWhenReleased(domain, ADMIN, ready, start));
                var b = executor.submit(() -> createWhenReleased(other, new User("another", "other-hash", "admin"), ready, start));
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                var outcomes = java.util.Arrays.asList(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS));
                assertThat(outcomes.stream().filter(java.util.Objects::isNull).count()).isOne();
                outcomes.stream().filter(java.util.Objects::nonNull).forEach(MongoFirstAdminStoreIT::refused);
                assertThat(SystemCollections.CONFIG_USERS.on(database).countDocuments()).isOne();
                assertThat(revision(database)).isEqualTo(3);
                var winner = domain.firstAdmin().load().orElseThrow();
                assertThat(domain.users().find(winner.username())).isPresent();
            }
        });
    }

    @Test
    void cloudCompletionNeverCreatesLocalFirstAdminOrUserFacts() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.CLOUD); domain.bindings().complete(SCOPE);
            assertThat(domain.firstAdmin().load()).isEmpty();
            assertThatThrownBy(() -> domain.firstAdmin().create(SCOPE, ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
            assertThatThrownBy(() -> domain.users().save(ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
            assertThatThrownBy(() -> domain.users().find(ADMIN.username())).satisfies(MongoFirstAdminStoreIT::refused);
            assertThat(SystemCollections.OP_BOOTSTRAP.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.CONFIG_USERS.on(database).countDocuments()).isZero();
            assertThat(revision(database)).isEqualTo(2);
        });
    }

    @Test
    void losingTheOriginalGuardBeforePublicationRollsBackUserCompletionAndRevision() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE);
            var calls = new AtomicInteger();
            var guarded = domain.firstAdmin().guardedBy(() -> {
                if (calls.incrementAndGet() == 2) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.create(SCOPE, ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
            assertThat(calls.get()).isEqualTo(2);
            assertThat(domain.firstAdmin().load().orElseThrow().phase()).isEqualTo(FirstAdminStore.Phase.PENDING);
            assertThat(domain.users().isEmpty()).isTrue();
            assertThat(revision(database)).isEqualTo(2);
        });
    }

    @Test
    void aRealTransactionRetryRechecksTheOriginalFirstAdminGuardBeforeAnySecondWrite() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE);
            var started = new AtomicInteger(); var failed = new AtomicInteger(); var checks = new AtomicInteger();
            CommandListener witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")) started.incrementAndGet();
                }
                @Override public void commandFailed(CommandFailedEvent event) {
                    if (event.getCommandName().equals("update")) failed.incrementAndGet();
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(witness).build());
                    var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
                var cipher = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                var observedDatabase = observed.getDatabase(database.getName());
                var bindings = new MongoStorageBindingStore(observed, observedDatabase, cipher);
                var firstAdmin = new MongoFirstAdminStore(observed, observedDatabase, bindings).guardedBy(() -> {
                    if (checks.incrementAndGet() > 1) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
                });
                admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1)).append("data", new Document("failCommands", List.of("update"))
                                .append("errorCode", 112).append("errorLabels", List.of("TransientTransactionError"))));
                try {
                    assertThatThrownBy(() -> firstAdmin.create(SCOPE, ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
                } finally {
                    admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                assertThat(checks.get()).isEqualTo(2);
                assertThat(failed.get()).isOne();
                assertThat(started.get()).isOne();
                assertThat(domain.users().isEmpty()).isTrue();
                assertThat(domain.firstAdmin().load().orElseThrow().phase()).isEqualTo(FirstAdminStore.Phase.PENDING);
                assertThat(revision(database)).isEqualTo(2);
            }
        });
    }

    @Test
    void losingSetupCompletionGuardDoesNotPublishPendingOrAdvancePhase() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP);
            var calls = new AtomicInteger();
            var guarded = domain.bindings().guardedBy(() -> {
                if (calls.incrementAndGet() == 2) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.complete(SCOPE)).satisfies(MongoFirstAdminStoreIT::refused);
            assertThat(SystemCollections.STORAGE_SETUP.on(database).find().first()).containsEntry("phase", "STORAGE_BOUND");
            assertThat(SystemCollections.OP_BOOTSTRAP.on(database).countDocuments()).isZero();
            assertThat(revision(database)).isOne();
        });
    }

    @Test
    void wrongOperationGenerationOrRoleCannotConsumePendingFirstAdmin() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE);
            for (StorageSetupScope wrong : List.of(new StorageSetupScope("cluster-b", "installation-a", "setup-a", 1),
                    new StorageSetupScope("cluster-a", "installation-b", "setup-a", 1),
                    new StorageSetupScope("cluster-a", "installation-a", "setup-b", 1),
                    new StorageSetupScope("cluster-a", "installation-a", "setup-a", 2))) {
                assertThatThrownBy(() -> domain.firstAdmin().create(wrong, ADMIN)).satisfies(MongoFirstAdminStoreIT::refused);
            }
            assertThatThrownBy(() -> domain.firstAdmin().create(SCOPE, new User("reader", "hash", "read")))
                    .satisfies(MongoFirstAdminStoreIT::refused);
            assertThat(domain.users().isEmpty()).isTrue();
            assertThat(revision(database)).isEqualTo(2);
        });
    }

    @Test
    void completedRecordOrAdministratorLossNeverReopensInitialization() {
        for (String loss : List.of("bootstrap", "user", "phase", "hash")) {
            withConfiguration((domain, database) -> {
                bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE); domain.firstAdmin().create(SCOPE, ADMIN);
                switch (loss) {
                    case "bootstrap" -> SystemCollections.OP_BOOTSTRAP.on(database).deleteMany(new Document());
                    case "user" -> SystemCollections.CONFIG_USERS.on(database).deleteMany(new Document());
                    case "phase" -> SystemCollections.OP_BOOTSTRAP.on(database).updateOne(new Document(),
                            new Document("$set", new Document("phase", "unknown-secret-sentinel")));
                    case "hash" -> SystemCollections.CONFIG_USERS.on(database).updateOne(new Document(),
                            new Document("$set", new Document("passwordHash", Map.of("secret", "unknown-secret-sentinel"))));
                    default -> throw new AssertionError(loss);
                }
                assertThatThrownBy(() -> domain.firstAdmin().load()).satisfies(MongoFirstAdminStoreIT::unreadable);
                assertThatThrownBy(() -> domain.firstAdmin().create(SCOPE, ADMIN)).satisfies(MongoFirstAdminStoreIT::unreadable);
                assertThatThrownBy(() -> domain.users().save(ADMIN)).satisfies(MongoFirstAdminStoreIT::unreadable);
                assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a"))
                        .satisfies(MongoFirstAdminStoreIT::unreadable);
                assertThat(revision(database)).isEqualTo(3);
            });
        }
    }

    @Test
    void pendingWithAnUnexpectedUserRefusesRatherThanCompletingOrReplacingIt() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE);
            SystemCollections.CONFIG_USERS.on(database).insertOne(MongoUserStore.toDocument(ADMIN));
            assertThatThrownBy(() -> domain.firstAdmin().create(SCOPE, ADMIN)).satisfies(MongoFirstAdminStoreIT::unreadable);
            assertThat(SystemCollections.OP_BOOTSTRAP.on(database).find().first()).containsEntry("phase", "PENDING");
            assertThat(revision(database)).isEqualTo(2);
        });
    }

    @Test
    void ordinaryUserWritesOnlyOpenAfterFirstAdminAndShareTheConfigurationRevision() {
        withConfiguration((domain, database) -> {
            bind(domain, StorageBinding.Mode.OP); domain.bindings().complete(SCOPE); domain.firstAdmin().create(SCOPE, ADMIN);
            var user = new User("another", "new-hash", "admin");
            domain.users().save(user);
            assertThat(domain.users().find("another")).contains(user);
            assertThat(revision(database)).isEqualTo(4);
            SystemCollections.CONFIGURATION_VERSION.on(database).deleteMany(new Document());
            assertThatThrownBy(() -> domain.users().save(new User("another", "replacement-hash", "admin")))
                    .satisfies(MongoFirstAdminStoreIT::unreadable);
            assertThat(domain.users().find("another")).contains(user);
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(database).countDocuments()).isZero();
        });
    }

    private static Throwable createWhenReleased(MongoConfigurationDomain domain, User user, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start was not released");
        try { domain.firstAdmin().create(SCOPE, user); return null; } catch (TapstateException refusal) { return refusal; }
    }
    private static void bind(MongoConfigurationDomain domain, StorageBinding.Mode mode) {
        String id = StorageBinding.defaultSourceId(mode);
        var source = new SourceResource(id, null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
        domain.bindings().commit(SCOPE, mode, CACHE, source);
    }
    private static void refused(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED));
    }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getMessage()).doesNotContain("unknown-secret-sentinel", "hashed-password");
            assertThat(error.getCause()).isNull();
        });
    }
    private static long revision(MongoDatabase database) {
        return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision");
    }
    private static MongoConnectionSettings settings(MongoDatabase database) {
        return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
    }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_first_admin_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                scenario.run(domain, database);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database) throws Exception; }
}
