package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
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

    private PreparedNativeStore(String cluster, String username, String password) {
        String identity = "e2e_" + UUID.randomUUID().toString().replace("-", "");
        configurationDatabase = identity + "_config";
        cacheDatabase = identity + "_cache";
        masterDatabase = identity + "_master";
        this.cluster = cluster; this.username = username; this.password = password;
    }

    static PreparedNativeStore create() {
        return create("e2e", "e2e-password");
    }

    static PreparedNativeStore create(String username, String password) {
        return create("e2e-" + UUID.randomUUID(), username, password);
    }

    static PreparedNativeStore create(String cluster, String username, String password) {
        var result = new PreparedNativeStore(cluster, username, password);
        try {
            NativeMongoStorageFixture.prepare(result.configurationUri(),
                    new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, result.cacheUri(), result.cacheDatabase, null),
                    result.masterUri(), cluster, username, password);
            return result;
        } catch (RuntimeException | Error failure) {
            try { result.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    String configurationUri() { return SharedMongo.replicaSetUrl(configurationDatabase); }
    String cacheUri() { return SharedMongo.replicaSetUrl(cacheDatabase); }
    String masterUri() { return SharedMongo.replicaSetUrl(masterDatabase); }
    String configurationDatabase() { return configurationDatabase; }
    String cacheDatabase() { return cacheDatabase; }
    String masterDatabase() { return masterDatabase; }
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
            for (String database : List.of(configurationDatabase, cacheDatabase, masterDatabase)) client.getDatabase(database).drop();
        }
    }
}
