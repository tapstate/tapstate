package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** A small real join/nest pipeline witnesses the frozen terminal rows' physical Mongo write shape. */
class BenchmarkTerminalTargetChangesIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.benchmark-smoke.jar";
    private static final String BARRIER_COLLECTION = "benchmark_terminal_probe_barriers";
    private static final Duration BOUND = Duration.ofMinutes(3);

    @BeforeAll
    static void requireRealServices() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres", "mongodb");
        String configured = System.getProperty(BOOT_JAR_PROPERTY);
        Assumptions.assumeTrue(configured != null && !configured.isBlank(),
                "no explicit benchmark application JAR");
        assertThat(Files.isRegularFile(Path.of(configured))).isTrue();
    }

    @Test
    void frozenCopyAndStatelessTerminalRowsInsertExactlyTheirExpandedTargetKeys() throws Exception {
        assertThat(simpleTerminalOperations("copy"))
                .containsExactly(OperationType.INSERT);
        assertThat(simpleTerminalOperations("stateless"))
                .containsExactly(OperationType.INSERT, OperationType.INSERT);
    }

    private static List<OperationType> simpleTerminalOperations(String workloadId) throws Exception {
        BenchmarkWorkloadDefinitions.Workload workload = BenchmarkWorkloadDefinitions.byId(workloadId);
        Map<String, Object> sourceSettings = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.settings("benchmark_terminal_shape_" + workloadId + "_source")
                : SharedPostgres.settings("benchmark_terminal_shape_" + workloadId + "_source");
        try (Connection source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.connect(sourceSettings) : SharedPostgres.connect(sourceSettings)) {
            try (Statement statement = source.createStatement()) {
                for (String sql : workload.setupSql()) {
                    if (sql.startsWith("CREATE TABLE ") || sql.startsWith("ALTER TABLE ")) {
                        statement.execute(sql);
                    }
                }
                statement.execute("copy".equals(workloadId)
                        ? "INSERT INTO bench_copy_orders (id,amount,payload) VALUES (1,259,'payload-1')"
                        : "INSERT INTO bench_stateless_orders (id,qty,region,items)"
                                + " VALUES (2,268,'keep',ARRAY['item-2-a','item-2-b'])");
            }

            String namespace = "benchmark_terminal_shape_" + workloadId;
            String storeUri = SharedMongo.replicaSetUrl(namespace + "_store");
            String targetUri = SharedMongo.replicaSetUrl(namespace + "_target");
            String viewsUri = SharedMongo.replicaSetUrl("views");
            String operatorDatabase = namespace + "_operator";
            String operatorUri = SharedMongo.replicaSetUrl(operatorDatabase);
            workload.resetTargets(targetUri, viewsUri, operatorUri);
            try (RealProcessServer server = RealProcessServer.start(storeUri, operatorDatabase,
                    Path.of(System.getProperty(BOOT_JAR_PROPERTY)));
                    MongoClient targetMongo = MongoClients.create(targetUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("benchmark", "benchmark-password");
                String connector = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                        ? "mysql" : "postgres";
                control.registerConnector(connector, ConnectorJars.bytesFor(connector));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                Map<String, String> resources = workload.resources(sourceSettings, targetUri);
                BenchmarkWorkloadDefinitions.SourceChain chain = workload.sourceChains().getFirst();
                String sourceFile = chain.sourceId() + ".tap.yml";
                control.apply(Map.of(sourceFile, resources.get(sourceFile)));
                control.discoverSchema(chain.sourceId(), connector, workload.connectorConfig(sourceSettings));
                control.apply(resources);
                String pipeline = workload.pipelineIds().getFirst();
                control.lifecycle(pipeline, LifecycleVerb.START);
                Await.until(pipeline + " to run", BOUND,
                        () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> String.valueOf(control.state(pipeline)));

                String targetDatabase = new ConnectionString(targetUri).getDatabase();
                String collectionName = "copy".equals(workloadId)
                        ? "bench_copy_orders" : "bench_stateless_orders";
                MongoCollection<Document> target = targetMongo.getDatabase(targetDatabase)
                        .getCollection(collectionName);
                long initialKey = "copy".equals(workloadId) ? 1L : 2L;
                int initialRows = "copy".equals(workloadId) ? 1 : 2;
                Await.until(workloadId + " initial snapshot", BOUND,
                        () -> target.countDocuments(Filters.eq("id", initialKey)) == initialRows,
                        () -> String.valueOf(target.countDocuments(Filters.eq("id", initialKey))));

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes =
                             targetMongo.getDatabase(targetDatabase).watch()
                                     .fullDocument(FullDocument.UPDATE_LOOKUP)
                                     .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                    try (Statement statement = source.createStatement()) {
                        for (String sql : workload.phase("terminal").sql()) {
                            statement.execute(sql);
                        }
                    }
                    long terminalKey = chain.terminalRowId();
                    int terminalRows = "copy".equals(workloadId) ? 1 : 2;
                    Await.until(workloadId + " expanded terminal target", BOUND,
                            () -> target.countDocuments(Filters.eq("id", terminalKey)) == terminalRows,
                            () -> String.valueOf(target.countDocuments(Filters.eq("id", terminalKey))));
                    String barrierId = barrier(targetMongo, targetDatabase);
                    List<OperationType> operations = untilBarrier(changes, targetDatabase,
                            collectionName, "id", terminalKey, barrierId);
                    System.out.printf("benchmark-terminal-shape workload=%s target=%s operations=%s%n",
                            workloadId, collectionName, operations);
                    return operations;
                }
            }
        }
    }

    @Test
    void frozenJoinAndNestTerminalRowsHaveAnObservableTargetWriteShape() throws Exception {
        BenchmarkWorkloadDefinitions.Workload workload = BenchmarkWorkloadDefinitions.byId("stateful");
        Map<String, Object> mysql = SharedMySql.settings("benchmark_terminal_shape_source");
        try (Connection source = SharedMySql.connect(mysql)) {
            try (Statement statement = source.createStatement()) {
                for (String sql : workload.setupSql().subList(0, 4)) {
                    statement.execute(sql);
                }
                statement.execute("INSERT INTO bench_join_customers (id,name) VALUES (1,'customer-1')");
                statement.execute("INSERT INTO bench_join_orders (id,customer_id,qty) VALUES (1,1,1)");
                statement.execute("INSERT INTO bench_nest_orders (id,label) VALUES (1,'root-1')");
                statement.execute("INSERT INTO bench_nest_items (id,order_id,sku) VALUES (1,1,'sku-1')");
            }

            String storeUri = SharedMongo.replicaSetUrl("benchmark_terminal_shape_store");
            String targetUri = SharedMongo.replicaSetUrl("benchmark_terminal_shape_target");
            String viewsUri = SharedMongo.replicaSetUrl("views");
            String operatorUri = SharedMongo.replicaSetUrl("benchmark_terminal_shape_operator");
            workload.resetTargets(targetUri, viewsUri, operatorUri);
            try (RealProcessServer server = RealProcessServer.start(storeUri,
                    "benchmark_terminal_shape_operator", Path.of(System.getProperty(BOOT_JAR_PROPERTY)));
                    MongoClient targetMongo = MongoClients.create(targetUri);
                    MongoClient viewsMongo = MongoClients.create(viewsUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("benchmark", "benchmark-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                Map<String, String> resources = workload.resources(mysql, targetUri);
                Map<String, String> sources = new LinkedHashMap<>();
                for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                    String file = chain.sourceId() + ".tap.yml";
                    sources.put(file, resources.get(file));
                }
                control.apply(sources);
                for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                    control.discoverSchema(chain.sourceId(), "mysql", workload.connectorConfig(mysql));
                }
                control.apply(resources);
                for (String pipeline : workload.pipelineIds()) {
                    control.lifecycle(pipeline, LifecycleVerb.START);
                }
                for (String pipeline : workload.pipelineIds()) {
                    Await.until(pipeline + " to run", BOUND,
                            () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                            () -> String.valueOf(control.state(pipeline)));
                }

                MongoCollection<Document> join = viewsMongo.getDatabase("views")
                        .getCollection("bench_join_output");
                MongoCollection<Document> nest = targetMongo.getDatabase(
                        new ConnectionString(targetUri).getDatabase()).getCollection("bench_nest_orders");
                Await.until("initial join and nest rows", BOUND,
                        () -> join.find(Filters.eq("order_id", 1L)).first() != null
                                && hasItem(nest.find(Filters.eq("id", 1L)).first(), 1L),
                        () -> "join=" + join.countDocuments() + " nest=" + nest.countDocuments());

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> joinChanges =
                                viewsMongo.getDatabase("views").watch()
                                        .fullDocument(FullDocument.UPDATE_LOOKUP)
                                        .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor();
                        MongoChangeStreamCursor<ChangeStreamDocument<Document>> nestChanges =
                                targetMongo.getDatabase(new ConnectionString(targetUri).getDatabase()).watch()
                                        .fullDocument(FullDocument.UPDATE_LOOKUP)
                                        .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                    try (Statement statement = source.createStatement()) {
                        for (String sql : workload.phase("terminal").sql()) {
                            statement.execute(sql);
                        }
                    }
                    Await.until("terminal join and assembled nest rows", BOUND,
                            () -> join.find(Filters.eq("order_id", 900_011L)).first() != null
                                    && hasItem(nest.find(Filters.eq("id", 900_013L)).first(), 900_014L),
                            () -> "join=" + join.find(Filters.eq("order_id", 900_011L)).first()
                                    + " nest=" + nest.find(Filters.eq("id", 900_013L)).first());

                    String joinBarrier = barrier(viewsMongo, "views");
                    String nestDatabase = new ConnectionString(targetUri).getDatabase();
                    String nestBarrier = barrier(targetMongo, nestDatabase);
                    List<OperationType> joinOps = untilBarrier(joinChanges, "views", "bench_join_output",
                            "order_id", 900_011L, joinBarrier);
                    List<OperationType> nestOps = untilBarrier(nestChanges, nestDatabase, "bench_nest_orders",
                            "id", 900_013L, nestBarrier);

                    assertThat(joinOps).as("join terminal physical writes").containsExactly(OperationType.INSERT);
                    assertThat(nestOps).as("nest terminal physical writes")
                            .containsExactly(OperationType.INSERT, OperationType.UPDATE);
                    System.out.printf("benchmark-terminal-shape join=%s nest=%s%n", joinOps, nestOps);
                    Await.until("terminal ACKs for every stateful source", Duration.ofSeconds(45),
                            () -> workload.sourceChains().stream()
                                    .allMatch(chain -> control.targetAckForIfPresent(chain).isPresent()),
                            () -> workload.sourceChains().stream().map(chain -> chain.id() + "="
                                    + control.targetAckForIfPresent(chain).isPresent()).toList().toString());
                }
            }
        }
    }

    private static boolean hasItem(Document root, long itemId) {
        if (root == null || !(root.get("items") instanceof List<?> items)) {
            return false;
        }
        return items.stream().anyMatch(item -> item instanceof Document document
                && document.get("id") instanceof Number number && number.longValue() == itemId);
    }

    private static String barrier(MongoClient client, String database) {
        String id = UUID.randomUUID().toString();
        client.getDatabase(database).getCollection(BARRIER_COLLECTION).insertOne(new Document("_id", id));
        return id;
    }

    private static List<OperationType> untilBarrier(
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor,
            String database, String collection, String keyField, long key, String barrierId) {
        long deadline = System.nanoTime() + BOUND.toNanos();
        List<OperationType> writes = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            ChangeStreamDocument<Document> change = cursor.tryNext();
            if (change == null) {
                continue;
            }
            MongoNamespace namespace = change.getNamespace();
            if (namespace == null || !database.equals(namespace.getDatabaseName())) {
                continue;
            }
            Document body = change.getFullDocument();
            if (BARRIER_COLLECTION.equals(namespace.getCollectionName())
                    && change.getOperationType() == OperationType.INSERT
                    && body != null && barrierId.equals(body.getString("_id"))) {
                return List.copyOf(writes);
            }
            if (collection.equals(namespace.getCollectionName()) && body != null
                    && body.get(keyField) instanceof Number number && number.longValue() == key) {
                writes.add(change.getOperationType());
            }
        }
        throw new AssertionError("terminal target change-stream barrier did not arrive for " + collection);
    }
}
