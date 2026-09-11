package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A column whose own name holds a dot is applied, and applying it says so - on a real database.
 *
 * <p>The unit cases prove the rule fires on the model it is handed. What they cannot prove is the step
 * before it: whether a real column named {@code price.usd} actually reaches the rule under that name.
 * That answer belongs to the connector, which decides what it reports when it discovers a schema, and
 * a connector that normalized the name away would leave the rule looking at something else entirely
 * while every unit case went on passing. So it is settled here, against a real MySQL.
 *
 * <p>Applied, not refused: the name is legal on both sides and the rows are written correctly, so the
 * batch lands and the finding travels beside its outcomes. Both halves are asserted, because a product
 * that refused the batch would also make the finding appear.
 */
class ADottedColumnIsReportedAtApplyIT {

    private static final String SOURCE_ID = "src_dotted";
    private static final String TARGET_ID = "tgt_dotted";
    private static final String PIPELINE_ID = "dotted_pipeline";
    private static final String TABLE = "orders";

    /** Legal in MySQL, and a key a document store reads as a step into a {@code price} that is not there. */
    private static final String DOTTED_COLUMN = "price.usd";

    private static final String READS_AS_A_PATH = "schema.column-name-reads-as-a-path";

    /** A database of this run's own on the server every specification here shares. */
    private static Map<String, Object> mysql;

    @BeforeAll
    static void startTheDatabase() throws Exception {
        RealConnectorGate.require("mysql", "mongodb");
        mysql = SharedMySql.settings("dotted_column");
        seed(mysql);
    }

    @Test
    @DisplayName("a real column named price.usd is applied to a document store, and reported on")
    void aColumnWhoseNameHoldsADotIsAppliedAndReportedOn() throws Exception {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("dotted_column"))) {
            ControlPlane control = NumericSource.connected(server);
            Map<String, Object> config = mysql;
            String target = SharedMongo.replicaSetUrl("dotted_column_target");

            // Discovered first, or the batch would be judged against a source nobody has looked at and
            // there would be no column names to report on at all.
            control.discoverSchema(SOURCE_ID, "mysql", config);
            assertThat(control.sourceSchemaFields(SOURCE_ID, TABLE))
                    .as("the name a real MySQL column reaches the product under, which is what the "
                            + "finding is computed from")
                    .contains(DOTTED_COLUMN);

            List<ControlPlane.Warning> warnings = control.apply(workspace(config, target));

            assertThat(control.artifactIds())
                    .as("the batch is applied, not refused: the rows cross correctly and refusing here "
                            + "would refuse every pipeline whose data already sits in a collection this way")
                    .contains(SOURCE_ID, TARGET_ID, PIPELINE_ID);
            assertThat(warnings)
                    .as("applying a batch that syncs a column named `%s` into a document store said "
                            + "nothing about it; the findings were %s", DOTTED_COLUMN, warnings)
                    .anySatisfy(warning -> {
                        assertThat(warning.code()).isEqualTo(READS_AS_A_PATH);
                        assertThat(warning.params())
                                .as("a finding that does not name the column is one nobody can act on")
                                .containsEntry("column", DOTTED_COLUMN)
                                .containsEntry("table", TABLE)
                                .containsEntry("target", TARGET_ID);
                    });
        }
    }

    /** One table carrying an ordinary column beside the one under test, so only the dot differs. */
    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, price_usd DECIMAL(18,4), "
                    + "`" + DOTTED_COLUMN + "` DECIMAL(18,4))");
            statement.execute("INSERT INTO " + TABLE + " (id, price_usd, `" + DOTTED_COLUMN
                    + "`) VALUES (1, 1.5000, 1.5000)");
        }
    }

    /** A MySQL source over that table, a mongo target, and a pipeline syncing one to the other. */
    private static Map<String, String> workspace(Map<String, Object> config, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_dotted.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, config.get("host"), config.get("port"), config.get("database"),
                config.get("username"), config.get("password"), TABLE));
        resources.put("tgt_dotted.tap.yml", """
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
                serve:
                  from: %s
                  sync:
                    - id: out
                      source: %s
                """.formatted(PIPELINE_ID, SOURCE_ID, TABLE, TARGET_ID));
        return resources;
    }
}
