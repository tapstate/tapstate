package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A source column that has the name Tapstate reserves for its own row data reaches the target under
 * another name, said out loud - on a real database.
 *
 * <p>The field {@code __tap} is removed from every row where it is written to a target, because it is
 * where the product keeps what it adds. A customer column of that name would be removed with it: the
 * pipeline would run, the target would fill, and one column would be silently missing from every
 * document. So the column is renamed where it enters, and the rename is reported when the pipeline is
 * applied.
 *
 * <p>Both halves need a real connector. Whether the name survives discovery, and whether the rows a
 * snapshot and a change stream hand over are keyed by it, are the connector's answers. The unit cases
 * prove the rename on the model and the rows they are handed; this proves a real MySQL hands them
 * over in the shape the rename looks for.
 */
class ASourceColumnWithTheReservedNameIsRenamedIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(250);

    private static final String SOURCE_ID = "src_reserved";
    private static final String TARGET_ID = "tgt_reserved";
    private static final String PIPELINE_ID = "reserved_pipeline";
    private static final String TABLE = "events";

    private static final String RESERVED = "__tap";
    private static final String RENAMED = "__tap_src";
    private static final String RENAMED_CODE = "schema.reserved-field-renamed";

    private static Map<String, Object> mysql;

    @BeforeAll
    static void startTheDatabase() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        mysql = SharedMySql.settings("reserved_column");
        seed(mysql);
    }

    @Test
    @DisplayName("a real column named __tap is reported at apply and delivered as __tap_src, through snapshot and change stream")
    void theColumnIsReportedAndDeliveredUnderTheNewName() throws Exception {
        String targetUri = SharedMongo.replicaSetUrl("reserved_column_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = NumericSource.connected(server, storage);

            // Discovered first: the warning is computed from the columns the source was found to have.
            control.discoverSchema(SOURCE_ID, "mysql", mysql);
            List<ControlPlane.Warning> warnings = control.apply(workspace(mysql, targetUri));

            assertThat(warnings)
                    .as("applying a pipeline over a table with a column named `%s` said nothing about "
                            + "the rename; the findings were %s", RESERVED, warnings)
                    .anySatisfy(warning -> {
                        assertThat(warning.code()).isEqualTo(RENAMED_CODE);
                        assertThat(warning.params())
                                .containsEntry("source", SOURCE_ID)
                                .containsEntry("table", TABLE)
                                .containsEntry("column", RESERVED)
                                .containsEntry("renamed", RENAMED);
                    });
            assertThat(control.sourceSchemaFields(SOURCE_ID, TABLE))
                    .as("the stored model knows the column by the name every step and target will see")
                    .contains(RENAMED)
                    .doesNotContain(RESERVED);

            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            // The snapshot half. Without the rename the writer would drop the customer's column.
            awaitColumn(mongo, target, Map.of(1, "a", 2, "b", 3, "c"), "the snapshot of the seeded rows");
            assertThat(mongo.documents(target, TABLE))
                    .as("no document carries the reserved field: nothing in this pipeline delivers one")
                    .allSatisfy(document -> assertThat(document).doesNotContainKey(RESERVED));

            // The change-stream half: rows from the log are keyed by the source's name too.
            execute(mysql, "UPDATE " + TABLE + " SET `" + RESERVED + "` = 'changed' WHERE id = 2");
            awaitColumn(mongo, target, Map.of(1, "a", 2, "changed", 3, "c"), "a change to that column");
            assertThat(mongo.documents(target, TABLE))
                    .allSatisfy(document -> assertThat(document).doesNotContainKey(RESERVED));
        }
    }

    private static void awaitColumn(
            MongoEndpoints mongo, EndpointAddress target, Map<Integer, String> expected, String what) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<Integer, String> last = Map.of();
        while (System.nanoTime() - deadline < 0) {
            last = renamedById(mongo, target);
            if (last.equals(expected)) {
                return;
            }
            sleep();
        }
        assertThat(last).as("`%s` by id in the target after %s", RENAMED, what).isEqualTo(expected);
    }

    private static Map<Integer, String> renamedById(MongoEndpoints mongo, EndpointAddress target) {
        Map<Integer, String> values = new LinkedHashMap<>();
        for (Document document : new ArrayList<>(mongo.documents(target, TABLE))) {
            if (document.get("id") instanceof Number id) {
                values.put(id.intValue(), document.getString(RENAMED));
            }
        }
        return values;
    }

    /** An ordinary column on each side of the one under test, so only the name differs. */
    private static void seed(Map<String, Object> settings) throws Exception {
        execute(settings, "DROP TABLE IF EXISTS " + TABLE);
        execute(settings, "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, `" + RESERVED
                + "` VARCHAR(32), v INT)");
        execute(settings, "INSERT INTO " + TABLE + " (id, `" + RESERVED
                + "`, v) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    }

    private static void execute(Map<String, Object> settings, String sql) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static Map<String, String> workspace(Map<String, Object> config, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_reserved.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, config.get("host"), config.get("port"), config.get("database"),
                config.get("username"), config.get("password"), TABLE));
        resources.put("tgt_reserved.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(TARGET_ID, targetUri));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(PIPELINE_ID, SOURCE_ID, TABLE, TARGET_ID));
        return resources;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting on the target", e);
        }
    }
}
