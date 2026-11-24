package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A NEST view keeps applying changes with the same documents while its state store validates once per
 * claim lease: each state write is fenced by its compare-and-set on the pipeline's claim, and no longer
 * also advances the pipeline state's runtime write fence in the same transaction.
 */
class ANestViewFencesEveryStateWriteOnItsClaimAloneIT {
    private static final String PARENTS = "orders";
    private static final String CHILDREN = "order_items";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "lease_fenced_orders";
    private static final int CHANGES = 40;
    private static final String BARRIER = "nest_write_witness_barrier";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void changesConvergeWhileStateWritesLeaveTheRuntimeWriteFenceAlone() throws Exception {
        var source = SharedMySql.settings("nest_lease_fenced_source");
        seed(source);
        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            start(server, storage, source);
            NestStateOutlivesARunAndIsDiscardedBetweenThemIT.awaitCustomer(
                    () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW), "customer-0", "the initial assembly");
            try (var client = MongoClients.create(storage.cacheUri())) {
                var cache = client.getDatabase(storage.cacheDatabase());
                var watched = Aggregates.match(Filters.in("ns.coll", List.of(BARRIER,
                        SystemCollections.CACHE_NEST_STATE.collectionName(),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.collectionName(),
                        SystemCollections.CACHE_PIPELINE_STATE.collectionName())));
                try (var changes = cache.watch(List.of(watched)).maxAwaitTime(250, TimeUnit.MILLISECONDS).cursor()) {
                    for (int change = 1; change <= CHANGES; change++) {
                        String customer = "customer-" + change;
                        update(source, customer);
                        // Each change must land before the next one, so the updates cannot be coalesced.
                        NestStateOutlivesARunAndIsDiscardedBetweenThemIT.awaitCustomer(
                                () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW), customer,
                                "change " + change + " to reach the document");
                    }
                    cache.getCollection(BARRIER).insertOne(new Document("_id", PIPELINE));
                    assertClaimOnlyTransactions(changes);
                }
            }
        }
    }

    private static void assertClaimOnlyTransactions(MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes) {
        Set<Transaction> state = new HashSet<>();
        Set<Transaction> claims = new HashSet<>();
        Set<Transaction> fences = new HashSet<>();
        Await.until("the CACHE stream to reach the marker after the observed writes", () -> {
            ChangeStreamDocument<Document> change;
            while ((change = changes.tryNext()) != null) {
                String collection = change.getNamespace().getCollectionName();
                if (collection.equals(BARRIER)) return true;
                if (collection.equals(SystemCollections.CACHE_NEST_STATE.collectionName())) {
                    state.add(transaction(change));
                } else if (collection.equals(SystemCollections.CACHE_WORKLOAD_CLAIMS.collectionName())
                        && updated(change, "fencedPipelineWrites")) {
                    claims.add(transaction(change));
                } else if (collection.equals(SystemCollections.CACHE_PIPELINE_STATE.collectionName())
                        && updated(change, "runtimeWriteFence")) {
                    fences.add(transaction(change));
                }
            }
            return false;
        }, () -> "state transactions=" + state.size() + ", claim transactions=" + claims.size()
                + ", fence transactions=" + fences.size());
        assertThat(state.size()).as("committed Nest writes for the %d changes", CHANGES).isGreaterThanOrEqualTo(CHANGES);
        assertThat(claims).as("each Nest write transaction proves the pipeline claim").containsAll(state);
        assertThat(state).as("no Nest write transaction also advances the runtime write fence")
                .doesNotContainAnyElementsOf(fences);
    }

    private static boolean updated(ChangeStreamDocument<Document> change, String field) {
        return change.getUpdateDescription() != null && change.getUpdateDescription().getUpdatedFields().containsKey(field);
    }

    private static Transaction transaction(ChangeStreamDocument<Document> change) {
        assertThat(change.getLsid()).as("session for %s", change.getNamespace()).isNotNull();
        assertThat(change.getTxnNumber()).as("transaction number for %s", change.getNamespace()).isNotNull();
        return new Transaction(change.getLsid(), change.getTxnNumber());
    }

    private record Transaction(BsonDocument session, BsonInt64 number) {}

    private static void start(ServerHandle server, PreparedNativeStore storage, Map<String, Object> source) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        var resources = new LinkedHashMap<String, String>();
        resources.put("parents.tap.yml", sourceYaml("src_parents", source, PARENTS));
        resources.put("children.tap.yml", sourceYaml("src_children", source, CHILDREN));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_parents, src_children ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                view:
                  id: %s
                  from: order_doc
                  primary_key: id
                """.formatted(PIPELINE, VIEW));
        control.apply(resources);
        control.discoverSchema("src_parents", "mysql", source);
        control.discoverSchema("src_children", "mysql", source);
        control.lifecycle(PIPELINE, LifecycleVerb.START);
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILDREN);
            statement.execute("DROP TABLE IF EXISTS " + PARENTS);
            statement.execute("CREATE TABLE " + PARENTS + " (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("CREATE TABLE " + CHILDREN + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            statement.execute("INSERT INTO " + PARENTS + " (id, customer) VALUES (1, 'customer-0')");
            statement.execute("INSERT INTO " + CHILDREN + " (id, order_id, sku) VALUES (1, 1, 'sku-1'), (2, 1, 'sku-2')");
        }
    }

    private static void update(Map<String, Object> settings, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("UPDATE " + PARENTS + " SET customer = '" + customer + "' WHERE id = 1");
        }
    }

    private static String sourceYaml(String id, Map<String, Object> config, String table) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(id, config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"), table);
    }
}
