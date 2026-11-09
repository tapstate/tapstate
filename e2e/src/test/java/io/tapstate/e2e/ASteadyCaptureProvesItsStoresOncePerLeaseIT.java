package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
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
 * there, only a few of the appends may carry the proof, and most of the checkpoints must run on the
 * lease. With the proof paid per batch again, every append carries it and no checkpoint is leased.
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
    private static final Duration OPENING = Duration.ofSeconds(90);
    private static final Duration WRITE_EVERY = Duration.ofMillis(500);
    private static final String PROFILE = "system.profile";
    private static final long PROFILE_BYTES = 256L << 20;
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
            Await.until(PIPELINE + " to run", OPENING, () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE)));
            // A pipeline marked RUNNING is not necessarily on the stream yet, and a cdc-only pipeline never sees a
            // change made before it is: changes keep being written until one crosses. They open the stream and pay
            // the first proof outside the measured window.
            int[] warm = {0};
            long[] lastWrite = {System.nanoTime() - WRITE_EVERY.toNanos()};
            Await.until("a change made after the pipeline started to reach the target", OPENING, () -> {
                if (copied.countDocuments(new Document("_id", new Document("$regex", "^warm_"))) > 0) return true;
                if (System.nanoTime() - lastWrite[0] >= WRITE_EVERY.toNanos()) {
                    orders.insertOne(new Document("_id", "warm_" + (++warm[0])).append("value", 1));
                    lastWrite[0] = System.nanoTime();
                }
                return false;
            }, () -> "warm changes written=" + warm[0] + ", state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));

            MongoDatabase cache = cacheObserver.getDatabase(storage.cacheDatabase());
            // The profiler's own collection is capped at a megabyte by default: a window of thirty changes
            // outgrows it and the first batches fall off the front, so it is remade to hold the whole window,
            // and only the writes and reads the witness counts are kept, not the stream's own commands.
            cache.runCommand(new Document("profile", 0));
            cache.getCollection(PROFILE).drop();
            cache.createCollection(PROFILE, new CreateCollectionOptions().capped(true).sizeInBytes(PROFILE_BYTES));
            cache.runCommand(new Document("profile", 1)
                    .append("filter", new Document("op", new Document("$in", List.of("update", "query")))));
            try {
                for (int i = 1; i <= CHANGES; i++) writeAndAwait(orders, copied, "change_" + i);
            } finally {
                cache.runCommand(new Document("profile", 0));
            }
            var profiled = cache.getCollection(PROFILE).find().into(new ArrayList<>());
            // The writes themselves count ungrouped: the profiler does not tie an update to its transaction.
            long appends = profiled.stream().filter(op -> isUpdate(op, "srs_events")).count();
            long checkpointWrites = profiled.stream().filter(op -> isUpdate(op, "srs_chains")).count();
            // The reads do carry their transaction, and a proof shows in what it reads. An append proof reads
            // the identity row and the ring bounds without reading events; the consumer's own reads, which also
            // run in this window, read its membership and are left out. A leased checkpoint reads the chain root
            // and nothing else.
            int provingAppends = 0, leasedCheckpoints = 0;
            for (List<Document> reads : transactions(profiled).values()) {
                Set<String> read = new java.util.HashSet<>();
                for (Document op : reads) read.add(collection(op));
                if (read.contains(PROOF_MARKER) && read.contains("srs_log_bounds") && !read.contains("srs_events")
                        && CONSUMER_COLLECTIONS.stream().noneMatch(read::contains)) provingAppends++;
                if (read.equals(Set.of("srs_chains"))) leasedCheckpoints++;
            }
            String reading = "appends %d (proving %d), checkpoint writes %d (leased %d) over %d profiled operations"
                    .formatted(appends, provingAppends, checkpointWrites, leasedCheckpoints, profiled.size());
            assertThat(appends).as("every change was its own durable batch: " + reading).isGreaterThanOrEqualTo(CHANGES);
            assertThat(checkpointWrites).as("every batch was checkpointed: " + reading).isGreaterThanOrEqualTo(CHANGES);
            // A proof per lease over a window of a few seconds is a handful; a proof per batch is all thirty.
            assertThat(provingAppends).as("appends proving the stores: " + reading).isLessThanOrEqualTo(CHANGES / 4);
            assertThat(leasedCheckpoints).as("checkpoints within a lease: " + reading).isGreaterThanOrEqualTo(CHANGES / 2);
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

    /**
     * Groups the profiled reads by the transaction that issued them; reads outside one are left out. The
     * profiler records the session and transaction number inside the command it profiled, as the driver sent
     * them, not at the top of the entry.
     */
    private static Map<String, List<Document>> transactions(List<Document> profiled) {
        var result = new LinkedHashMap<String, List<Document>>();
        for (Document op : profiled) {
            Document command = op.get("command", Document.class);
            if (!"query".equals(op.getString("op")) || command == null) continue;
            Document session = command.get("lsid", Document.class);
            Object transaction = command.get("txnNumber");
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
