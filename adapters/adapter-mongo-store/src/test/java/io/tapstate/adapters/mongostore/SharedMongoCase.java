package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

/** Shares only a plain replica set; every test invocation owns its databases and clients. */
public final class SharedMongoCase implements BeforeEachCallback, AfterEachCallback {
    // Explicitly audited suites. Server failpoints, TTL tuning, transaction lock tuning and
    // topology witnesses retain dedicated containers; unknown suites cannot opt in accidentally.
    private static final Set<String> SUITES = Set.of(
            "io.tapstate.adapters.mongostore.ChangingRetentionAltersTheExistingIndexIT",
            "io.tapstate.adapters.mongostore.MongoArtifactStoreIT",
            "io.tapstate.adapters.mongostore.MongoAuditStoreIT",
            "io.tapstate.adapters.mongostore.MongoCatalogStoreIT",
            "io.tapstate.adapters.mongostore.MongoCloudSessionStoreIT",
            "io.tapstate.adapters.mongostore.MongoClusterIdentityStoreIT",
            "io.tapstate.adapters.mongostore.MongoClusterMembershipStoreIT",
            "io.tapstate.adapters.mongostore.MongoConnectionTestResultStoreIT",
            "io.tapstate.adapters.mongostore.MongoConnectorCatalogStoreIT",
            "io.tapstate.adapters.mongostore.MongoConnectorRegistryIT",
            "io.tapstate.adapters.mongostore.MongoConnectorSpecStoreIT",
            "io.tapstate.adapters.mongostore.MongoDataVersionGateIT",
            "io.tapstate.adapters.mongostore.MongoDerivedSchemaStoreIT",
            "io.tapstate.adapters.mongostore.MongoDesiredStoreIT",
            "io.tapstate.adapters.mongostore.MongoKeyedStateStoreBatchReadIT",
            "io.tapstate.adapters.mongostore.MongoKeyedStateStoreIT",
            "io.tapstate.adapters.mongostore.MongoNestDeadLetterStoreIT",
            "io.tapstate.adapters.mongostore.MongoPipelineDraftStoreIT",
            "io.tapstate.adapters.mongostore.MongoRateHistoryStorePagingIT",
            "io.tapstate.adapters.mongostore.MongoSchemaReclamationIT",
            "io.tapstate.adapters.mongostore.MongoSchemaStoreIT",
            "io.tapstate.adapters.mongostore.MongoSessionStoreIT",
            "io.tapstate.adapters.mongostore.MongoSinkAcknowledgementFenceIT",
            "io.tapstate.adapters.mongostore.MongoSrsMetaStoreIT",
            "io.tapstate.adapters.mongostore.MongoStateStoreIT",
            "io.tapstate.adapters.mongostore.MongoStorePortIT",
            "io.tapstate.adapters.mongostore.MongoTokenStoreIT",
            "io.tapstate.adapters.mongostore.MongoUserStoreIT",
            "io.tapstate.adapters.mongostore.SharedMongoCaseIT",
            "io.tapstate.adapters.mongostore.SourceConfigKeyringAcknowledgementIT",
            "io.tapstate.adapters.mongostore.SourceConfigKeyringRestoreIT",
            "io.tapstate.adapters.mongostore.SourceConfigKeyringStoreIT",
            "io.tapstate.adapters.mongostore.SrsConsumerOffsetsCeilingIT",
            "io.tapstate.adapters.mongostore.SrsSchemaHistoryCeilingIT",
            "io.tapstate.adapters.mongostore.SystemCollectionsReconciliationIT",
            "io.tapstate.adapters.mongostore.WorkloadClaimStoreIT",
            "io.tapstate.adapters.mongostore.migration.MigrationRunnerIT",
            "io.tapstate.adapters.mongostore.migration.SchemaModelMigrationIT",
            "io.tapstate.adapters.mongostore.migration.V11RateHistoryKeysetIndexIT",
            "io.tapstate.adapters.mongostore.migration.V12EncryptSourceConfigsIT",
            "io.tapstate.adapters.mongostore.migration.V2StructuredArtifactsIT",
            "io.tapstate.adapters.mongostore.migration.V3RecordedSrsSwitchesIT",
            "io.tapstate.adapters.mongostore.migration.V4DiscardInventedPositionsIT",
            "io.tapstate.adapters.mongostore.migration.V7RepairBlankPipelinesIT",
            "io.tapstate.adapters.mongostore.migration.V8DiscardViewSchemaPoliciesIT"
    );
    private static final ExtensionContext.Namespace ROOT =
            ExtensionContext.Namespace.create(SharedMongoCase.class);
    private record ActiveScope(String owner, Scope scope) { }
    private final AtomicReference<ActiveScope> active = new AtomicReference<>();

    private SharedMongoCase() { }

    public static SharedMongoCase forSuite(Class<?> suite) {
        if (!SUITES.contains(suite.getName())) {
            throw new IllegalArgumentException("This suite has not been audited for a shared replica set: "
                    + suite.getName());
        }
        return new SharedMongoCase();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        String owner = Objects.requireNonNull(context.getUniqueId(), "invocation owner");
        Server server = context.getRoot().getStore(ROOT).getOrComputeIfAbsent(
                "plain-replica-set", ignored -> new Server(), Server.class);
        Scope scope = new Scope(server);
        if (!active.compareAndSet(null, new ActiveScope(owner, scope))) {
            throw new IllegalStateException("Concurrent invocations cannot share a mutable case fixture");
        }
        System.out.println("MONGO_CASE phase=open scope=" + scope.prefix);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        ActiveScope owned = active.get();
        if (owned != null && owned.owner().equals(context.getUniqueId())
                && active.compareAndSet(owned, null)) {
            owned.scope().close();
        }
    }

    public String getReplicaSetUrl() {
        return getReplicaSetUrl("test");
    }

    public String getReplicaSetUrl(String database) {
        return scope().uri(database);
    }

    public String database(String logical) {
        return scope().database(logical);
    }

    Scope additionalScope() {
        return new Scope(scope().server);
    }

    private Scope scope() {
        ActiveScope owned = active.get();
        if (owned == null) throw new IllegalStateException("No test invocation owns this fixture");
        return owned.scope();
    }

    static final class Scope implements AutoCloseable {
        private final Server server;
        private final String prefix = "case_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
        private final Map<String, String> databases = new LinkedHashMap<>();
        private boolean closed;

        private Scope(Server server) {
            this.server = server;
        }

        synchronized String database(String logical) {
            if (closed) throw new IllegalStateException("The database scope has been released");
            if (databases.containsValue(logical)) return logical;
            MongoNamespace.checkDatabaseNameValidity(logical);
            if (Set.of("admin", "config", "local").stream().anyMatch(logical::equalsIgnoreCase)) {
                throw new IllegalArgumentException("Global databases are not case resources");
            }
            String present = databases.get(logical);
            if (present != null) return present;
            String allocated = prefix + "_" + hash(logical);
            if (databases.containsValue(allocated)) {
                throw new IllegalStateException("Two logical databases must not share a physical name");
            }
            databases.put(logical, allocated);
            return allocated;
        }

        String uri(String logical) {
            return server.container.getReplicaSetUrl(database(logical));
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            long started = System.nanoTime();
            RuntimeException failure = null;
            // Test methods and their AfterEach methods close their own clients before this callback.
            // Cleanup uses a new client and only names allocated to this scope, never a database scan.
            MongoClientSettings cleanupSettings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(server.container.getReplicaSetUrl()))
                    .applyToClusterSettings(settings -> settings.serverSelectionTimeout(5, TimeUnit.SECONDS))
                    .applyToSocketSettings(settings -> settings.connectTimeout(5, TimeUnit.SECONDS)
                            .readTimeout(5, TimeUnit.SECONDS))
                    .build();
            boolean completed = false;
            try {
                try (MongoClient client = MongoClients.create(cleanupSettings)) {
                    for (String name : databases.values()) {
                        try {
                            client.getDatabase(name).drop();
                        } catch (RuntimeException error) {
                            if (failure == null) failure = error;
                            else failure.addSuppressed(error);
                        }
                    }
                    // Throw inside the resource scope so a subsequent close failure is suppressed
                    // onto the original drop failure rather than replacing it.
                    if (failure != null) throw failure;
                }
                completed = true;
            } finally {
                System.out.println("MONGO_CASE phase=close scope=" + prefix + " databases=" + databases.size()
                        + " status=" + (completed ? "success" : "failure")
                        + " nanos=" + (System.nanoTime() - started));
            }
        }

        private static String hash(String name) {
            try {
                byte[] digest = MessageDigest.getInstance("SHA-256")
                        .digest(name.getBytes(StandardCharsets.UTF_8));
                return java.util.HexFormat.of().formatHex(digest, 0, 8);
            } catch (NoSuchAlgorithmException error) {
                throw new AssertionError(error);
            }
        }
    }

    private static final class Server implements ExtensionContext.Store.CloseableResource {
        private final MongoDBContainer container =
                new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

        private Server() {
            long started = System.nanoTime();
            try {
                container.start();
            } catch (RuntimeException failure) {
                try { container.stop(); }
                catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            System.out.println("MONGO_BASE phase=start id=" + container.getContainerId()
                    + " nanos=" + (System.nanoTime() - started));
        }

        @Override
        public void close() {
            long started = System.nanoTime();
            container.stop();
            System.out.println("MONGO_BASE phase=stop nanos=" + (System.nanoTime() - started));
        }
    }
}
