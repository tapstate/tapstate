package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/** One explicit native installation per scenario; server restarts reuse it without preparing again. */
final class PreparedNativeStore implements AutoCloseable {
    private final String configurationDatabase;
    private final String cacheDatabase;
    private final String masterDatabase;
    private final String cluster;
    private final String username;
    private final String password;
    private final SourceResource borrowedMaster;
    private final Function<String, String> uriForDatabase;

    private PreparedNativeStore(String cluster, String username, String password, SourceResource borrowedMaster,
            Function<String, String> uriForDatabase) {
        String identity = "e2e_" + UUID.randomUUID().toString().replace("-", "");
        configurationDatabase = identity + "_config";
        cacheDatabase = identity + "_cache";
        masterDatabase = identity + "_master";
        this.cluster = cluster; this.username = username; this.password = password;
        this.borrowedMaster = borrowedMaster;
        this.uriForDatabase = uriForDatabase;
    }

    static PreparedNativeStore create() {
        return create("e2e", "e2e-password");
    }

    /** Uses the scenario's own Mongo server so a fault reaches all three installed domains. */
    static PreparedNativeStore createOn(NetworkedMongo server) {
        return install("e2e-" + UUID.randomUUID(), "e2e", "e2e-password", null, server::uriForThisHost);
    }

    static PreparedNativeStore create(String username, String password) {
        return create("e2e-" + UUID.randomUUID(), username, password);
    }

    static PreparedNativeStore create(String cluster, String username, String password) {
        return install(cluster, username, password, null);
    }

    /** The scenario owns this target, including an endpoint currently unavailable after installation. */
    static PreparedNativeStore withBorrowedMaster(String cluster, String username, String password, SourceResource target) {
        return install(cluster, username, password, java.util.Objects.requireNonNull(target));
    }

    private static PreparedNativeStore install(String cluster, String username, String password, SourceResource target) {
        return install(cluster, username, password, target, SharedMongo::replicaSetUrl);
    }

    private static PreparedNativeStore install(String cluster, String username, String password, SourceResource target,
            Function<String, String> uriForDatabase) {
        var result = new PreparedNativeStore(cluster, username, password, target, uriForDatabase);
        try {
            var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, result.cacheUri(), result.cacheDatabase, null);
            if (target == null) NativeMongoStorageFixture.prepare(result.configurationUri(), cache, result.masterUri(), cluster, username, password);
            else NativeMongoStorageFixture.prepare(result.configurationUri(), cache, target, cluster, username, password);
            return result;
        } catch (RuntimeException | Error failure) {
            try { result.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    String configurationUri() { return uriForDatabase.apply(configurationDatabase); }
    String cacheUri() { return uriForDatabase.apply(cacheDatabase); }
    String masterUri() { requireOwnedMaster(); return uriForDatabase.apply(masterDatabase); }
    String configurationDatabase() { return configurationDatabase; }
    String cacheDatabase() { return cacheDatabase; }
    String masterDatabase() { requireOwnedMaster(); return masterDatabase; }
    private void requireOwnedMaster() {
        if (borrowedMaster != null) throw new IllegalStateException("the scenario owns the borrowed master endpoint");
    }
    String clusterId() { return cluster; }
    String username() { return username; }
    String password() { return password; }
    void loginAndAwaitBusiness(ControlPlane control) {
        control.login(username, password);
        awaitBusiness(control);
    }

    void awaitBusiness(ControlPlane control) {
        var last = new java.util.concurrent.atomic.AtomicReference<java.util.Map<?, ?>>();
        Await.until("the prepared installation to admit business work", () -> {
            var status = control.storageStatus(); last.set(status);
            if (!cluster.equals(status.get("clusterId"))) throw new AssertionError("the server opened a different installation");
            return "READY".equals(status.get("businessPhase"));
        }, () -> String.valueOf(last.get()));
    }

    /** The surrounding try-with-resources must close every server before closing this owner. */
    @Override public void close() {
        try (var client = MongoClients.create(configurationUri())) {
            var owned = borrowedMaster == null ? List.of(configurationDatabase, cacheDatabase, masterDatabase)
                    : List.of(configurationDatabase, cacheDatabase);
            for (String database : owned) client.getDatabase(database).drop();
        }
    }
}
