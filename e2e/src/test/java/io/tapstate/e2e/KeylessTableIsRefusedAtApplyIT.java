package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * An upsert into a table its source declares no key for is refused, on a real database.
 *
 * <p>The unit cases prove the rule fires on the model it is handed. What they cannot prove is the step
 * before it: whether a real table without a primary key actually reaches the rule as a table without a
 * key. That answer belongs to the connector, which decides what counts as a key when it reports a
 * schema, and it is not the same answer on every engine — so it is settled here, against a real MySQL,
 * rather than assumed.
 *
 * <p>That step is what makes the second case worth its container, and it is the case that corrected
 * an assumption rather than confirming one. A table with no primary key but a unique index over a
 * non-null column can be upserted in principle - the unique column identifies the row. The connector
 * does not report it as keyed: it derives the key from a primary-key constraint alone, so such a
 * table arrives keyless and is refused. That refusal is known, deliberate for now, and deliberately
 * pinned here: the case asserts what happens today, so that widening the rule later shows up as this
 * test failing rather than as nobody noticing.
 */
class KeylessTableIsRefusedAtApplyIT {

    private static final String SOURCE_ID = "src_keyless";
    private static final String TARGET_ID = "tgt_keyless";
    private static final String UPSERT_NEEDS_KEY = "dsl.upsert-needs-key";

    /** No primary key, no unique index: nothing a write could be matched to a row by. */
    private static final String KEYLESS = "events";

    /** No primary key either, but a unique index over a non-null column - upsertable in principle. */
    private static final String UNIQUELY_INDEXED = "accounts";

    /**
     * One database for all three cases, started once. Each case reads the same two tables and never
     * writes them, so they do not need one apiece - and a container apiece is not free: this module
     * runs long, container-heavy witnesses side by side, and the ones that measure behaviour under
     * memory pressure are sensitive to how much else is running. Three databases where one will do
     * spends that budget for nothing.
     */
    private static MySQLContainer<?> mysql;

    @BeforeAll
    static void startTheDatabase() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
        mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));
        mysql.start();
        seed(mysql);
    }

    @AfterAll
    static void stopTheDatabase() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @Test
    @DisplayName("a keyless table is refused for upsert, accepted for append, on a real MySQL")
    void aKeylessTableIsRefusedForUpsertAndAcceptedForAppend() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = NumericSource.connected(server, storage);
            Map<String, Object> config = NumericSource.config(mysql);
            String target = SharedMongo.replicaSetUrl("keyless_refused_target");

            // Discovered first, or the refusal would be the one about a source nobody discovered -
            // a different rule, and one that would pass this test for the wrong reason.
            control.discoverSchema(SOURCE_ID, "mysql", config);

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(
                    workspace(config, target, KEYLESS, ""));
            assertThat(refusal.code())
                    .as("the code refusing an upsert into a table nothing can key a write by")
                    .isEqualTo(UPSERT_NEEDS_KEY);
            assertThat(refusal.params()).containsEntry("table", KEYLESS);
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch")
                    .doesNotContain(SOURCE_ID, TARGET_ID, "keyless_pipeline");

            // The same table, written a way that never matches rows to keys. Only the write mode
            // differs, so an implementation that refused this would be refusing the table rather
            // than the combination.
            assertThatCode(() -> control.apply(
                    workspace(config, target, KEYLESS, "\n      write_mode: append")))
                    .doesNotThrowAnyException();
        }
    }

    /**
     * Pinned as it behaves, not as it ought to: a unique index over a non-null column could carry an
     * upsert, and is refused anyway, because the connector counts only a primary-key constraint as a
     * key. Accepted for now rather than fixed - widening the rule to any unique index would admit one
     * over a nullable column, which cannot match rows at all (null never equals null) and would write
     * silently wrong data, and telling the two apart needs a nullability the discovered model does not
     * carry. The refusal costs a reader a clear error naming both ways out; the alternative costs
     * correctness.
     */
    @Test
    @DisplayName("a table keyed only by a unique index is refused today, and that is a known limit")
    void aTableKeyedOnlyByAUniqueIndexIsRefusedForNow() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = NumericSource.connected(server, storage);
            Map<String, Object> config = NumericSource.config(mysql);
            control.discoverSchema(SOURCE_ID, "mysql", config);

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(workspace(
                    config, SharedMongo.replicaSetUrl("keyless_unique_target"), UNIQUELY_INDEXED, ""));

            assertThat(refusal.code()).isEqualTo(UPSERT_NEEDS_KEY);
            assertThat(refusal.params())
                    .as("the refusal names the table, which is what makes the limit actionable")
                    .containsEntry("table", UNIQUELY_INDEXED);
        }
    }

    /**
     * Testing a connection against a real MySQL comes back with a report rather than a crash.
     *
     * <p>The probe is the verb a stranger reaches first, and it takes a path no other verb does: it
     * inits the connector, discovers, then reads a small sample as proof of life. That last step used
     * to hand the connector a table descriptor carrying no columns, which a connector reading the
     * columns off it answers by throwing - inside the connector, where the failure reads as a broken
     * connection rather than as a descriptor we failed to pass. Nothing about that is visible without
     * a real connector, so it is asserted here rather than inferred from the synthetic case.
     */
    @Test
    @DisplayName("testing a connection against a real MySQL returns a report, not a crash")
    void testingAConnectionAgainstARealMySqlReturnsAReport() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = NumericSource.connected(server, storage);

            String report = control.testConnectionBody(SOURCE_ID, "mysql", NumericSource.config(mysql));

            assertThat(report)
                    .as("the report names the connection it tested and carries the connector's checks")
                    .contains(SOURCE_ID)
                    .contains("checks");
            assertThat(report)
                    .as("a report is what comes back - not a stack trace laundered into a message")
                    .doesNotContain("NullPointerException");
        }
    }

    /** Both shapes in one database, so a single discovery reports them together. */
    private static void seed(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + KEYLESS + " (payload VARCHAR(64), seq BIGINT)");
            statement.execute("INSERT INTO " + KEYLESS + " (payload, seq) VALUES ('a', 1)");
            statement.execute("CREATE TABLE " + UNIQUELY_INDEXED
                    + " (email VARCHAR(64) NOT NULL, name VARCHAR(64), UNIQUE KEY uq_email (email))");
            statement.execute("INSERT INTO " + UNIQUELY_INDEXED + " (email, name) VALUES ('a@b.c', 'a')");
        }
    }

    /** A source selecting one table, a mongo target, and a pipeline syncing between them. */
    private static Map<String, String> workspace(
            Map<String, Object> config, String targetUri, String table, String writeMode) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_keyless.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, config.get("host"), config.get("port"), config.get("database"),
                config.get("username"), config.get("password"), table));
        resources.put("tgt_keyless.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(TARGET_ID, targetUri));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: keyless_pipeline
                source: %s
                serve:
                  from: %s
                  sync:
                    - id: out
                      source: %s%s
                """.formatted(SOURCE_ID, table, TARGET_ID, writeMode));
        return resources;
    }
}
