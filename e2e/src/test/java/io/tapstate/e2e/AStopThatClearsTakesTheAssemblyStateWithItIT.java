package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stop that was asked to clear takes the assembly state with it, and leaves the target alone.
 *
 * <p>The neighbouring case says what a clearing stop does to the chain's own record and to the counters,
 * and says the rows already written are not its to take. It cannot say anything about the third kind of
 * state a pipeline holds, because the pipeline it drives has no step that holds any: assembly state is
 * what a nest keeps between changes, and unlike everything else it lives under a name fixed by the
 * deployment rather than one the run chose. So this case drives a pipeline that assembles, and reads
 * that name.
 *
 * <p>Both directions are asserted, and the first is not ceremony. Assembly state is created lazily -- a
 * pipeline that never assembled anything holds none -- so "it is gone afterwards" is satisfied by a run
 * that never had any, on every implementation including one that clears nothing at all. The case waits
 * until the assembled document has reached the target and the state is visibly there before it stops.
 *
 * <p>And the target is read at the end for the same reason it is next door: a clear that reached into
 * the user's own database would satisfy every other assertion here.
 *
 * <p>The assembled documents are read from the collection named after the root table, not after the
 * step. What a nest is called is a name inside the pipeline; where its documents land is decided by the
 * model the sink resolved, which is the root's. Written down because the other reading costs an hour:
 * polling the step's name finds nothing, forever, and looks exactly like a pipeline that never
 * assembled.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AStopThatClearsTakesTheAssemblyStateWithItIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class AStopThatClearsTakesTheAssemblyStateWithItIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);

    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String VIEW = "order_doc";
    private static final String ROOT_SOURCE = "src_orders";
    private static final String CHILD_SOURCE = "src_items";
    private static final String TARGET_ID = "tgt_mongo";

    /** The one collection the shared state store keeps every pipeline\u0027s assembly state in. */
    private static final String ASSEMBLY_STATE = "operator_state";

    private static final int ROOTS = 2;
    private static final int CHILDREN_PER_ROOT = 2;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aClearingStopEmptiesTheAssemblyStateAndLeavesTheTarget(Tiers tier) throws Exception {
        String suffix = "nest_purge_" + tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> source = SharedMySql.settings(suffix + "_src");
        seed(source);

        String targetUri = SharedMongo.replicaSetUrl(suffix + "_tgt");
        String storeUri = SharedMongo.replicaSetUrl(suffix + "_state");
        String assemblyStateUri = SharedMongo.assemblyStateUrl();
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri)) {

            ControlPlane control = start(server, suffix, source, targetUri);

            // Assembled and delivered: only then does this pipeline hold any assembly state at all.
            Await.until("the assembled documents to reach the target", TIMEOUT,
                    () -> mongo.count(target, PARENT_TABLE) == ROOTS,
                    () -> "%d of %d documents in %s".formatted(mongo.count(target, PARENT_TABLE), ROOTS, PARENT_TABLE));

            // The state is there before it is taken away. Without this, the assertion after the stop is
            // satisfied by a pipeline that never held any -- which is every implementation.
            Await.until("the assembly state of %s to be there before it is cleared".formatted(suffix),
                    TIMEOUT,
                    () -> !heldBy(mongo, assemblyStateUri, suffix).isEmpty(),
                    () -> "the shared state store held " + mongo.collections(assemblyStateUri)
                            + " with " + heldBy(mongo, assemblyStateUri, suffix).size() + " of this run\u0027s");
            long documentsBeforeTheStop = mongo.count(target, PARENT_TABLE);

            control.stop(suffix, true);
            awaitState(control, suffix, PipelineState.STOPPED);

            assertThat(heldBy(mongo, assemblyStateUri, suffix))
                    .as("the assembly state this pipeline held, after a stop that was asked to clear -- "
                            + "shown above to have been there, so its absence is the clearing and not a "
                            + "pipeline that never assembled anything")
                    .isEmpty();
            assertThat(mongo.count(target, PARENT_TABLE))
                    .as("the documents already written to the user's own database, which a stop does not "
                            + "reach however much of the product's own state it is asked to take")
                    .isEqualTo(documentsBeforeTheStop);
        }
    }

    /**
     * What the shared state store holds for this pipeline.
     *
     * <p>The store keeps every pipeline's assembly state in one collection, keyed within it rather than
     * split by collection -- measured, after looking for a collection per pipeline and finding a single
     * {@code operator_state}. So a clearing stop has to take this pipeline's entries out of a collection
     * other pipelines are still using, which is a sharper claim than dropping something of its own.
     *
     * <p>Matched on the pipeline's id appearing anywhere in the entry rather than on a key this case
     * composes: how a namespace is built from the pipeline and the step is the product's affair, and what
     * this needs to know is which entries are this run's. Every case here takes an id of its own, so the
     * match cannot collect a neighbour's.
     */
    private static List<String> heldBy(MongoEndpoints mongo, String stateUri, String pipelineId) {
        return mongo.documents(EndpointAddress.uri(stateUri), ASSEMBLY_STATE).stream()
                .map(String::valueOf)
                .filter(entry -> entry.contains(pipelineId))
                .toList();
    }

    /** Everything up to and including the start. */
    private static ControlPlane start(
            ServerHandle server, String pipelineId, Map<String, Object> source, String targetUri) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(ROOT_SOURCE + ".tap.yml", sourceYaml(ROOT_SOURCE, PARENT_TABLE, source));
        resources.put(CHILD_SOURCE + ".tap.yml", sourceYaml(CHILD_SOURCE, CHILD_TABLE, source));
        resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
        control.apply(resources);
        // Both models are discovered: the parent's resolves the collection the sink writes, and the
        // child's is where an embedded element's own key comes from.
        control.discoverSchema(ROOT_SOURCE, "mysql", source);
        control.discoverSchema(CHILD_SOURCE, "mysql", source);
        control.lifecycle(pipelineId, LifecycleVerb.START);
        return control;
    }

    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ %s, %s ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: %s
                    type: nest
                    from: { o: %s, i: %s }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: %s
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, ROOT_SOURCE, CHILD_SOURCE, VIEW, PARENT_TABLE, CHILD_TABLE,
                        VIEW, TARGET_ID);
    }

    private static String sourceYaml(String id, String table, Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(
                        id,
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"),
                        table);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(TARGET_ID, targetUri);
    }

    /** Roots with their children, written by a client of the database rather than through the product. */
    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILD_TABLE);
            statement.execute("DROP TABLE IF EXISTS " + PARENT_TABLE);
            statement.execute("CREATE TABLE " + PARENT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("CREATE TABLE " + CHILD_TABLE
                    + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            for (int root = 1; root <= ROOTS; root++) {
                statement.execute("INSERT INTO " + PARENT_TABLE + " (id, name) VALUES ("
                        + root + ", 'order-" + root + "')");
                for (int child = 1; child <= CHILDREN_PER_ROOT; child++) {
                    int childId = (root - 1) * CHILDREN_PER_ROOT + child;
                    statement.execute("INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES ("
                            + childId + ", " + root + ", 'sku-" + childId + "')");
                }
            }
        }
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until("%s to reach %s".formatted(pipelineId, expected), TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }
}
