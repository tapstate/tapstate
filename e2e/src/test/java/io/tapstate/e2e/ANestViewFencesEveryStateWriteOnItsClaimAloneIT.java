package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A NEST view keeps applying changes with the same documents while its state store validates once per
 * claim lease: each state write is fenced by its compare-and-set on the pipeline's claim, and no longer
 * also advances the pipeline state's runtime write fence. Under per-write validation every state write
 * advanced both, so the fence would move at least as often as the claim's write counter.
 */
class ANestViewFencesEveryStateWriteOnItsClaimAloneIT {
    private static final String PARENTS = "orders";
    private static final String CHILDREN = "order_items";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "lease_fenced_orders";
    private static final int CHANGES = 40;

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
            long fenceBefore = runtimeWriteFence(storage);
            long writesBefore = fencedPipelineWrites(storage);

            for (int change = 1; change <= CHANGES; change++) update(source, "customer-" + change);
            NestStateOutlivesARunAndIsDiscardedBetweenThemIT.awaitCustomer(
                    () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW), "customer-" + CHANGES,
                    "every change to reach the document");

            long fenceMoves = runtimeWriteFence(storage) - fenceBefore;
            long fencedWrites = fencedPipelineWrites(storage) - writesBefore;
            assertThat(fencedWrites).as("the state writes for %d changes were fenced on the claim", CHANGES).isPositive();
            assertThat(fenceMoves * 2).as("state writes (%d) no longer advance the runtime write fence (moved %d)",
                    fencedWrites, fenceMoves).isLessThan(fencedWrites);
        }
    }

    private static long runtimeWriteFence(PreparedNativeStore storage) {
        return cache(storage, SystemCollections.CACHE_PIPELINE_STATE, new Document("_id", PIPELINE)).getLong("runtimeWriteFence");
    }

    private static long fencedPipelineWrites(PreparedNativeStore storage) {
        Object counter = cache(storage, SystemCollections.CACHE_WORKLOAD_CLAIMS,
                new Document("_id.resourceType", "PIPELINE_ACTUATION").append("_id.resourceId", PIPELINE)).get("fencedPipelineWrites");
        return counter == null ? 0L : ((Number) counter).longValue();
    }

    private static Document cache(PreparedNativeStore storage, SystemCollections table, Document filter) {
        try (var client = MongoClients.create(storage.cacheUri())) {
            Document found = table.on(client.getDatabase(storage.cacheDatabase())).find(filter).first();
            assertThat(found).as("%s row for %s", table.collectionName(), PIPELINE).isNotNull();
            return found;
        }
    }

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
