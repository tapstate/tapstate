package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import java.util.List;
import java.util.UUID;

/** One explicit native installation per scenario; server restarts reuse it without preparing again. */
final class PreparedNativeStore implements AutoCloseable {
    private final String configurationDatabase;
    private final String cacheDatabase;
    private final String masterDatabase;
    private final String cluster;
    private final String username;
    private final String password;
    private final SourceResource borrowedMaster;

    private PreparedNativeStore(String cluster, String username, String password, SourceResource borrowedMaster) {
        String identity = "e2e_" + UUID.randomUUID().toString().replace("-", "");
        configurationDatabase = identity + "_config";
        cacheDatabase = identity + "_cache";
        masterDatabase = identity + "_master";
        this.cluster = cluster; this.username = username; this.password = password;
        this.borrowedMaster = borrowedMaster;
    }

    static PreparedNativeStore create() {
        return create("e2e", "e2e-password");
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
        var result = new PreparedNativeStore(cluster, username, password, target);
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

    String configurationUri() { return SharedMongo.replicaSetUrl(configurationDatabase); }
    String cacheUri() { return SharedMongo.replicaSetUrl(cacheDatabase); }
    String masterUri() { requireOwnedMaster(); return SharedMongo.replicaSetUrl(masterDatabase); }
    String configurationDatabase() { return configurationDatabase; }
    String cacheDatabase() { return cacheDatabase; }
    String masterDatabase() { requireOwnedMaster(); return masterDatabase; }
    private void requireOwnedMaster() {
        if (borrowedMaster != null) throw new IllegalStateException("the scenario owns the borrowed master endpoint");
    }
    String clusterId() { return cluster; }
    void loginAndAwaitBusiness(ControlPlane control) {
        control.login(username, password);
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
