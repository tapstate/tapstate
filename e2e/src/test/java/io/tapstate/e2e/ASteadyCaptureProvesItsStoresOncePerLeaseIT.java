package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A steady change stream proves the capture stores once per lease, not once per source batch.
 *
 * <p>Every source batch is made durable by two fenced store transactions: the append of its events
 * and the checkpoint of the chain. Each proof of the stores (structure, identity, models and the
 * checkpoint's frontier) is a long chain of round trips, and paying it inside every batch is what
 * capped the applied change rate. This case drives thirty changes through a real pipeline one at a
 * time, so each is its own batch, and reads the cache database's profiler: the batches must be
 * there, and only a few of them may carry the proof. With the proof paid per batch again, every one
 * of the thirty carries it.
 *
 * <p>No specification word reaches this: the vocabulary says what crossed and in what state, never
 * what the product's own store spent to carry it.
 */
class ASteadyCaptureProvesItsStoresOncePerLeaseIT {
    private static final String DATABASE = "e2e_lease_source";
    private static final String TARGET_DATABASE = "e2e_lease_target";
    private static final String PIPELINE = "lease_capture";
    /** Single changes, each confirmed at the target before the next is written, so each is one batch. */
    private static final int CHANGES = 30;
    /** The identity row is read only by a proof; a batch inside a standing lease never touches it. */
    private static final String PROOF_MARKER = "cache_identity";
    private static final Set<String> CONSUMER_COLLECTIONS = Set.of("srs_consumers", "srs_consumer_offsets");

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void thirtySingleChangesCarryAtMostAFewProofs() {
        String sourceUri = SharedMongo.replicaSetUrl(DATABASE);
        String targetUri = SharedMongo.replicaSetUrl(TARGET_DATABASE);
        // Surefire also carries the MCP sidecar's non-web configuration; this witness hosts the REST server.
        String previousWebType = System.getProperty("spring.main.web-application-type");
        System.setProperty("spring.main.web-application-type", "servlet");
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                MongoClient observer = MongoClients.create(sourceUri);
                MongoClient cacheObserver = MongoClients.create(storage.cacheUri());
                ServerHandle server = InProcessServer.startPrepared(storage)) {
            var orders = observer.getDatabase(DATABASE).getCollection("orders");
            var copied = observer.getDatabase(TARGET_DATABASE).getCollection("copied_orders");
            orders.insertOne(new Document("_id", "seed").append("value", 0));
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of(
                    "source.tap.yml", sourceYaml("src_mongo", sourceUri, DATABASE) + "mode: cdc\n",
                    "target.tap.yml", sourceYaml("tgt_mongo", targetUri, TARGET_DATABASE),
                    "pipeline.tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: lease_capture
                            source: src_mongo
                            settings: { read_mode: cdc_only }
                            transforms:
                              - { id: changes, from: [orders], type: filter, expr: "op == 'i'" }
                            serve:
                              from: changes
                              sync:
                                - source: tgt_mongo
                                  rename:
                                    map: { orders: copied_orders }
                            """));
            control.discoverSchema("src_mongo", "mongodb", Map.of("uri", sourceUri, "database", DATABASE));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            // The first change opens the stream and pays the first proof outside the measured window.
            writeAndAwait(orders, copied, "warm");

            MongoDatabase cache = cacheObserver.getDatabase(storage.cacheDatabase());
            cache.runCommand(new Document("profile", 1).append("slowms", 0).append("sampleRate", 1.0));
            try {
                for (int i = 1; i <= CHANGES; i++) writeAndAwait(orders, copied, "change_" + i);
            } finally {
                cache.runCommand(new Document("profile", 0));
            }
            var transactions = transactions(cache.getCollection("system.profile").find().into(new ArrayList<>()));
            int appends = 0, provingAppends = 0, checkpoints = 0, provingCheckpoints = 0;
            for (List<Document> operations : transactions.values()) {
                boolean proving = operations.stream().anyMatch(op -> collection(op).equals(PROOF_MARKER));
                if (operations.stream().anyMatch(op -> isUpdate(op, "srs_events"))) {
                    appends++; if (proving) provingAppends++;
                } else if (operations.stream().anyMatch(op -> isUpdate(op, "srs_chains"))
                        && operations.stream().noneMatch(op -> CONSUMER_COLLECTIONS.contains(collection(op)))) {
                    checkpoints++; if (proving) provingCheckpoints++;
                }
            }
            String reading = "appends %d (proving %d), checkpoints %d (proving %d) over %d profiled transactions"
                    .formatted(appends, provingAppends, checkpoints, provingCheckpoints, transactions.size());
            assertThat(appends).as("every change was its own durable batch: " + reading).isGreaterThanOrEqualTo(CHANGES);
            assertThat(checkpoints).as("every batch was checkpointed: " + reading).isGreaterThanOrEqualTo(CHANGES);
            // A proof per lease over a window of a few seconds is a handful; a proof per batch is all thirty.
            assertThat(provingAppends).as("appends proving the stores: " + reading).isLessThanOrEqualTo(CHANGES / 4);
            assertThat(provingCheckpoints).as("checkpoints proving the stores: " + reading).isLessThanOrEqualTo(CHANGES / 4);
        } finally {
            if (previousWebType == null) {
                System.clearProperty("spring.main.web-application-type");
            } else {
                System.setProperty("spring.main.web-application-type", previousWebType);
            }
        }
    }

    private static void writeAndAwait(com.mongodb.client.MongoCollection<Document> orders,
            com.mongodb.client.MongoCollection<Document> copied, String id) {
        orders.insertOne(new Document("_id", id).append("value", 1));
        Await.until("change " + id + " to reach the target", Duration.ofSeconds(30),
                () -> copied.countDocuments(new Document("_id", id)) == 1, () -> "the change has not crossed");
    }

    /** Groups the profiled operations by the transaction that issued them; operations outside one are left out. */
    private static Map<String, List<Document>> transactions(List<Document> profiled) {
        var result = new LinkedHashMap<String, List<Document>>();
        for (Document op : profiled) {
            Document session = op.get("lsid", Document.class);
            Object transaction = op.get("txnNumber");
            if (session == null || transaction == null) continue;
            result.computeIfAbsent(session.get("id") + "/" + transaction, key -> new ArrayList<>()).add(op);
        }
        return result;
    }

    private static boolean isUpdate(Document op, String collection) {
        return "update".equals(op.getString("op")) && collection(op).equals(collection);
    }

    private static String collection(Document op) {
        String namespace = op.getString("ns");
        int dot = namespace == null ? -1 : namespace.indexOf('.');
        return dot < 0 ? "" : namespace.substring(dot + 1);
    }

    private static String sourceYaml(String id, String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                tables: [ orders ]
                """.formatted(id, uri, database);
    }
}
