package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An initial load several times larger than the server's heap reaches the target in full.
 *
 * <p>The size of a load is the size of the source, which the person running the server does not choose and
 * often cannot know ahead of time. So the heap a pipeline needs to load has to be the same whatever the
 * source holds: rows read are handed on as the pipeline takes them, and the source is read no faster than
 * that. A server that instead holds a table - or every table - whole before the pipeline has taken any of
 * it needs a heap the size of the load, and on a source larger than its heap it runs out part way through
 * the load, having written a fraction of it.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The heap is fixed and small, and the load is several times larger than it.</b> A few hundred
 *       thousand rows of three short columns, which the server holds at several hundred bytes a row, against
 *       a heap of a quarter of a gigabyte. A server holding the load whole cannot get through it at any
 *       speed; one holding a bounded part of it at a time gets through it at any size.</li>
 *   <li><b>Every row, not most.</b> The target has to reach the table's own size exactly. A load cut short
 *       and one whose tail was dropped both leave a target most of the way there, and a threshold below the
 *       whole would read either as success.</li>
 *   <li><b>A server that dies is reported as dying, with its own words.</b> It is told to exit on running out
 *       of heap, so the case stops at that moment and the failure carries what the server said, rather than
 *       waiting out the bound over a process that has stopped working and says nothing about why.</li>
 *   <li><b>The real-process tier alone.</b> A heap belongs to a process. Embedded in this JVM, the server
 *       would share the test's own heap, which nothing here sizes, and the case would pass whatever the
 *       server holds.</li>
 * </ul>
 *
 * <p>Runs only where real connector jars are available:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AnInitialLoadLargerThanTheHeapReachesTheTargetIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AnInitialLoadLargerThanTheHeapReachesTheTargetIT {

    /** Several times what the heap below can hold of it at once. */
    private static final int ROWS = 400_000;

    private static final List<String> SMALL_HEAP = List.of("-Xmx256m", "-XX:+ExitOnOutOfMemoryError");

    /** A server start, a snapshot of a few hundred thousand rows through two real connectors: minutes. */
    private static final Duration BOUND = Duration.ofMinutes(6);

    private static final String TABLE = "orders";
    private static final String DATABASE = "large_load_src";
    private static final String PIPELINE_ID = "large_load";
    private static final String SOURCE_ID = "src_pg";
    private static final String TARGET_ID = "tgt_mongo";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void everyRowOfALoadTheHeapCannotHoldWholeReachesTheTarget() throws Exception {
        Map<String, Object> source = SharedPostgres.settings(DATABASE);
        seed(source);

        String storeUri = SharedMongo.replicaSetUrl("large_load_store");
        String targetUri = SharedMongo.replicaSetUrl("large_load_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        // Everything but the load is done by a server with the ordinary heap, and kept in the store for the
        // one under test. Uploading a connector holds its jar several times over on the way in, which a heap
        // this small cannot afford either: a limit of its own, found in the first run of this case, and not
        // the one this case is about.
        try (ServerHandle preparing = RealProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(preparing.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
            resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
            resources.put(PIPELINE_ID + ".tap.yml", Workspaces.pipelineYaml(
                    PIPELINE_ID, SOURCE_ID, TARGET_ID, TABLE));
            control.apply(resources);
            control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));
        }

        try (RealProcessServer server = RealProcessServer.startInJvm(storeUri, SMALL_HEAP);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.login("e2e", "e2e-password");
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            long[] reading = {0};
            Await.until("all " + ROWS + " rows of the load to reach the target", BOUND,
                    () -> {
                        if (!server.isAlive()) {
                            throw new AssertionError("the server exited part way through the load, with "
                                    + mongo.count(target, TABLE) + " of " + ROWS + " rows at the target; "
                                    + "its output ended:\n" + server.tail());
                        }
                        return (reading[0] = mongo.count(target, TABLE)) == ROWS;
                    },
                    () -> reading[0] + " rows");
        }
    }

    /**
     * The table, filled by the database itself: a statement per row from here would spend the bound on the
     * seeding, and a client-side batch this size would be a test of the driver's packet limits.
     */
    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE
                    + " (id INT PRIMARY KEY, customer VARCHAR(64), note VARCHAR(64))");
            statement.execute("INSERT INTO " + TABLE + " (id, customer, note)"
                    + " SELECT g, 'customer-' || g, md5(g::text) FROM generate_series(1, " + ROWS + ") g");
        }
    }

    private static Map<String, Object> discoveryConfig(Map<String, Object> settings) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", settings.get("host"));
        config.put("port", settings.get("port"));
        config.put("database", settings.get("database"));
        config.put("schema", "public");
        config.put("user", settings.get("username"));
        config.put("password", settings.get("password"));
        return config;
    }

    private static String sourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(SOURCE_ID, settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), TABLE);
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
}
