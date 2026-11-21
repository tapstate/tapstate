package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.MongoException;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

@RequiresDocker
class PipelineHistoryCheckpointIsolationIT {
    @Container private static final MongoDBContainer CONFIG_AND_DATA = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();

    private static final String PIPELINE = "orders_history";

    @BeforeAll static void connector() { RealConnectorGate.require("mongodb"); }

    @Test void publishesFreshObservationsWhileAnIndependentTransactionHoldsTheCheckpoint() {
        String cluster = "cache-only-" + java.util.UUID.randomUUID();
        String configDb = "probe_config";
        String configUri = CONFIG_AND_DATA.getReplicaSetUrl(configDb);
        String cacheDb = "probe_cache";
        String cacheUri = CACHE.getReplicaSetUrl(cacheDb);
        String masterDb = "probe_master";
        String masterUri = CONFIG_AND_DATA.getReplicaSetUrl(masterDb);
        NativeMongoStorageFixture.prepare(configUri, new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                cacheUri, cacheDb, null), masterUri, cluster);
        try (var configClient = MongoClients.create(configUri); var cacheClient = MongoClients.create(cacheUri);
             var server = InProcessServer.start(configUri, null, List.of("--tapstate.cluster.id=" + cluster,
                     "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                     "--tapstate.cluster.node-session-ttl=60s", "--tapstate.cluster.node-session-renew-interval=2s",
                     "--tapstate.cluster.workload-claim-ttl=60s", "--tapstate.cluster.workload-claim-renew-interval=2s",
                     "--tapstate.metrics.history.sample-interval=2s", "--logging.level.io.tapstate.app.ConvergenceDriver=WARN"))) {
            var control = new ControlPlane(server.baseUrl());
            var source = configClient.getDatabase(masterDb).getCollection("orders");
            source.insertOne(new Document("_id", 1));
            control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
            Await.until("the native business store to be ready",
                    () -> "READY".equals(control.storageStatus().get("businessPhase")), () -> String.valueOf(control.storageStatus()));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: orders_input
                    connector: mongodb
                    config: { uri: "%s", database: "%s" }
                    mode: cdc
                    tables: [orders]
                    """.formatted(masterUri, masterDb), "pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: orders_history
                    source: orders_input
                    settings: { read_mode: snapshot_and_cdc }
                    view: { id: order_history_view, from: orders, primary_key: _id }
                    """));
            control.discoverSchema("orders_input", "mongodb", Map.of("uri", masterUri,
                    "database", masterDb));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            var cache = cacheClient.getDatabase(cacheDb);
            var history = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(cache);
            var filter = new Document("pipelineId", PIPELINE);
            var landed = configClient.getDatabase(masterDb).getCollection("order_history_view");
            Await.until("a landed order and the first history sample", Duration.ofSeconds(70),
                    () -> landed.countDocuments() == 1 && history.countDocuments(filter) > 0, () -> control.logs(PIPELINE));

            var checkpoints = SystemCollections.CACHE_PIPELINE_STATE.on(cache);
            var initialObservation = control.observedAt(PIPELINE).orElseThrow();
            try (var holder = cacheClient.startSession()) {
                Await.until("an independent transaction to acquire the checkpoint write lock", Duration.ofSeconds(6), () -> {
                    holder.startTransaction();
                    try {
                        return checkpoints.updateOne(holder, new Document("_id", PIPELINE),
                                new Document("$inc", new Document("runtimeWriteFence", 1L))).getModifiedCount() == 1;
                    } catch (MongoException conflict) {
                        holder.abortTransaction();
                        if (!conflict.hasErrorLabel("TransientTransactionError")) throw conflict;
                        return false;
                    }
                }, () -> "checkpoint lock acquisition");
                try {
                    Instant heldSince = Instant.now();
                    var fresh = new Document(filter).append("observedAt", new Document("$gt", java.util.Date.from(heldSince)));
                    Await.until("two fresh history samples and a fresh observation while the checkpoint write lock is held",
                            Duration.ofSeconds(12),
                            () -> history.countDocuments(fresh) >= 2 && control.observedAt(PIPELINE)
                                    .filter(observed -> observed.isAfter(heldSince) && observed.isAfter(initialObservation)).isPresent(),
                            () -> "latest observation " + control.observedAt(PIPELINE) + ", history rows "
                                    + history.countDocuments(fresh));
                } finally {
                    holder.abortTransaction();
                }
            }
        }
    }

    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db", "rw,size=512m", "/data/configdb", "rw,size=16m"))
                .withCommand("--replSet", "docker-rs", "--wiredTigerCacheSizeGB", "0.25", "--oplogSize", "16",
                        "--setParameter", "enableTestCommands=1");
    }
}
