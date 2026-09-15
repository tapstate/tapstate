package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This release installs a sync onto one connector. A pipeline naming another supported connector as
 * its write target comes back from the running product as a coded refusal naming that connector, and
 * none of the batch is filed.
 *
 * <p>It is settled here rather than in a unit case because of where the rule lives. The document is
 * valid offline — the grammar has always let a sync name any connection, and the offline corpus still
 * does — and what may be written to is a property of the deployment being applied to. So the refusal
 * only exists on the far side of an apply, which is exactly the side an author is on.
 *
 * <p>Both directions are asserted against the same server. A rule that refused every batch would
 * satisfy the first half; the second half applies a batch differing only in the target's connector,
 * so what the first case refused is the connector and not the shape of the pipeline.
 */
class AnUnsupportedTargetConnectorIsRefusedAtApplyIT {

    private static final String UNSUPPORTED_TARGET_CONNECTOR = "dsl.unsupported-target-connector";

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String PG_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_out
            connector: postgres
            config: { host: 10.30.0.6, database: dw, username: w, password: p }
            """;

    private static final String MONGO_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_out
            connector: mongodb
            config: { uri: "mongodb://10.30.0.11:27017/ods" }
            """;

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: orders_out
            source: src_orders
            serve:
              from: orders
              sync: [ { id: out, source: tgt_out, write_mode: upsert } ]
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aSyncOntoAnotherSupportedConnectorIsRefusedAndNoneOfTheBatchIsFiled() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_target_connector"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                    "src_orders.tap.yml", SOURCE,
                    "tgt_out.tap.yml", PG_TARGET,
                    "orders_out.tap.yml", PIPELINE));

            assertThat(refusal.code())
                    .as("the code refusing a sync onto a connector this release does not write to")
                    .isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(refusal.params())
                    .as("the connector and the connection naming it are what send the author to the fix")
                    .containsEntry("connector", "postgres")
                    .containsEntry("source", "tgt_out");
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch")
                    .doesNotContain("orders_out", "tgt_out", "src_orders");

            control.apply(Map.of(
                    "src_orders.tap.yml", SOURCE,
                    "tgt_out.tap.yml", MONGO_TARGET,
                    "orders_out.tap.yml", PIPELINE));

            assertThat(control.artifactIds())
                    .as("the same batch, with only the target's connector changed, is installed")
                    .contains("orders_out", "tgt_out", "src_orders");
        }
    }
}
