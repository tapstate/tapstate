package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Db2 for i is supported as a source only: a running server installs a pipeline that reads a db2i
 * connection, and refuses, with a code naming the connector, a pipeline that would write into one - and
 * an edit that would move the connection an installed pipeline writes to onto db2i.
 *
 * <p>The connector behind {@code db2i} implements writes, and an on-prem deployment otherwise accepts a
 * sync onto any connector the catalog marks sink-capable - or onto any connector the catalog does not
 * carry at all, which on-prem is taken to be a private one. So the refusal holds only because this
 * release ships a db2i catalog row and that row says it is no target; a release without the row would
 * install the writing pipeline without a word.
 *
 * <p>The read half is asserted first, against the same server, so the refusal cannot be a server that
 * turns away every db2i document. Neither half needs an IBM i system or the connector jar - apply judges
 * a batch against the catalog, before anything connects.
 */
class ADb2ForIConnectionIsReadFromButNeverWrittenToIT {

    private static final String UNSUPPORTED_TARGET_CONNECTOR = "dsl.unsupported-target-connector";

    private static final String DB2I_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_db2i
            connector: db2i
            config: { host: 10.10.0.7, database: ORDERSLIB, journalLibraryName: ORDERSLIB, journalName: QSQJRN }
            mode: cdc
            tables: [ ORDERS ]
            """;

    private static final String MONGO_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_mongo
            connector: mongodb
            config: { uri: "mongodb://10.30.0.11:27017/ods" }
            """;

    private static final String READ_FROM_DB2I = """
            version: tapstate/v1
            kind: pipeline
            id: orders_from_db2i
            source: src_db2i
            serve:
              from: ORDERS
              sync: [ { id: out, source: tgt_mongo, write_mode: upsert } ]
            """;

    private static final String MYSQL_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String DB2I_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_db2i
            connector: db2i
            config: { host: 10.30.0.9, database: ODSLIB }
            """;

    /** The connection the installed pipeline writes to, re-applied on its own with Db2 for i underneath. */
    private static final String MONGO_TARGET_MOVED_TO_DB2I = """
            version: tapstate/v1
            kind: source
            id: tgt_mongo
            connector: db2i
            config: { host: 10.30.0.9, database: ODSLIB }
            """;

    private static final String WRITE_INTO_DB2I = """
            version: tapstate/v1
            kind: pipeline
            id: orders_into_db2i
            source: src_orders
            serve:
              from: orders
              sync: [ { id: out, source: tgt_db2i, write_mode: upsert } ]
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aPipelineReadingDb2ForIIsInstalledAndOneWritingIntoItIsRefused() {
        String database = "e2e_db2i_source_only_" + UUID.randomUUID().toString().replace("-", "");
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl(database))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            control.apply(Map.of(
                    "src_db2i.tap.yml", DB2I_SOURCE,
                    "tgt_mongo.tap.yml", MONGO_TARGET,
                    "orders_from_db2i.tap.yml", READ_FROM_DB2I));

            assertThat(control.artifactIds())
                    .as("a pipeline reading a db2i connection is installed")
                    .contains("orders_from_db2i", "src_db2i", "tgt_mongo");

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                    "src_orders.tap.yml", MYSQL_SOURCE,
                    "tgt_db2i.tap.yml", DB2I_TARGET,
                    "orders_into_db2i.tap.yml", WRITE_INTO_DB2I));

            assertThat(refusal.code())
                    .as("the code refusing a sync onto a source-only connector")
                    .isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(refusal.params())
                    .as("the connector and the connection naming it are what send the author to the fix")
                    .containsEntry("connector", "db2i")
                    .containsEntry("source", "tgt_db2i");
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch")
                    .doesNotContain("orders_into_db2i", "tgt_db2i", "src_orders");

            String installedTarget = control.contentHash("tgt_mongo");
            ControlPlane.Refusal moved = control.applyExpectingRefusal(Map.of(
                    "tgt_mongo.tap.yml", MONGO_TARGET_MOVED_TO_DB2I));

            assertThat(moved.code())
                    .as("the code refusing an edit that would leave an installed sync writing into db2i")
                    .isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(moved.params())
                    .as("the pipeline that would write into it is named, not only the connection")
                    .containsEntry("connector", "db2i")
                    .containsEntry("source", "tgt_mongo")
                    .containsEntry("resource", "orders_from_db2i");
            assertThat(control.contentHash("tgt_mongo"))
                    .as("the connection the installed pipeline writes to, after the refused edit")
                    .isEqualTo(installedTarget);
        }
    }
}
