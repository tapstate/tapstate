package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A NEST view on the state session applies its changes with the same documents, committing each vertex
 * instance's state in groups fenced on that instance's own commit fence row. The commits read the pipeline's
 * claim and never write it, so the fence rows count every group while the claim's write counter moves only for
 * lease proofs and writes outside operator state. With groups fenced on the claim, every group moved it.
 */
class ANestViewOnTheStateSessionFencesEachGroupOnItsOwnRowIT {
    private static final String PARENTS = "orders";
    private static final String CHILDREN = "order_items";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "session_fenced_orders";
    private static final int CHANGES = 40;
    private static final String CARRIER = "tapstate.nest.store";
    /** The reserved step of the fence rows, as the store encodes step ids: UTF-16 code units, base64url. */
    private static final String FENCE_STEP = encode("\u0000commit-fence");

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void changesConvergeWhileGroupsFenceOnTheirOwnRowsAndLeaveTheClaimAlone() throws Exception {
        var source = SharedMySql.settings("nest_session_fenced_source");
        seed(source);
        String previous = System.getProperty(CARRIER);
        System.setProperty(CARRIER, "direct");
        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            start(server, storage, source);
            NestStateOutlivesARunAndIsDiscardedBetweenThemIT.awaitCustomer(
                    () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW), "customer-0", "the initial assembly");
            assertThat(fenceRows(storage)).as("each vertex instance that committed has its own fence row").isNotEmpty();
            long claimWritesBefore = fencedPipelineWrites(storage);
            long groupsBefore = groupsCommitted(storage);

            for (int change = 1; change <= CHANGES; change++) update(source, "customer-" + change);
            NestStateOutlivesARunAndIsDiscardedBetweenThemIT.awaitCustomer(
                    () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW), "customer-" + CHANGES,
                    "every change to reach the document");

            long groups = groupsCommitted(storage) - groupsBefore;
            long claimWrites = fencedPipelineWrites(storage) - claimWritesBefore;
            assertThat(groups).as("the state of %d changes was committed in fenced groups", CHANGES).isPositive();
            // The claim still moves for the lease proofs that open and renew groups, and for writes outside
            // operator state; a group fenced on the claim would add one move per group on top of those.
            assertThat(claimWrites).as("%d groups committed while the claim moved %d times", groups, claimWrites)
                    .isLessThan(groups);
        } finally {
            if (previous == null) System.clearProperty(CARRIER);
            else System.setProperty(CARRIER, previous);
        }
    }

    /** The commit fence rows of the pipeline's operator state. */
    private static List<Document> fenceRows(PreparedNativeStore storage) {
        try (var client = MongoClients.create(storage.cacheUri())) {
            return SystemCollections.CACHE_NEST_STATE.on(client.getDatabase(storage.cacheDatabase()))
                    .find(new Document("_id.placement.stepId", FENCE_STEP)).into(new ArrayList<>());
        }
    }

    /** Groups committed so far: each commit and each proof under a new tenure counts once on its row. */
    private static long groupsCommitted(PreparedNativeStore storage) {
        long writes = 0;
        for (Document row : fenceRows(storage)) writes += ((Number) row.get("writes")).longValue();
        return writes;
    }

    private static long fencedPipelineWrites(PreparedNativeStore storage) {
        Object counter = cache(storage, SystemCollections.CACHE_WORKLOAD_CLAIMS,
                new Document("_id.resourceType", "PIPELINE_ACTUATION").append("_id.resourceId", PIPELINE)).get("fencedPipelineWrites");
        return counter == null ? 0L : ((Number) counter).longValue();
    }

    private static String encode(String value) {
        var bytes = ByteBuffer.allocate(value.length() * 2);
        for (int index = 0; index < value.length(); index++) bytes.putChar(value.charAt(index));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array());
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
