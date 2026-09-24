package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;

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
 *
 * <p>The second case is the shape a deployment actually has rather than the shape a test batch has:
 * the connection was filed by an earlier apply, and the write is declared in a reusable serve
 * definition rather than inline. Both of those are ordinary, and each one on its own is enough to
 * make a narrower rule pass the pipeline through.
 */
@RequiresDocker
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

    private static final String ATLAS_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_out
            connector: mongodb-atlas
            config: { isUri: true, uri: "mongodb://10.30.0.11:27017/ods" }
            """;

    private static final String AWS_RDS_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt_out
            connector: aws-rds-mysql
            config: { host: 10.30.0.6, database: dw, username: w, password: p }
            """;

    private static final String SERVE_DEFINITION = """
            version: tapstate/v1
            kind: serve
            id: out
            sync: [ { id: s, source: tgt_out, write_mode: upsert } ]
            """;

    private static final String PIPELINE_USING_THE_DEFINITION = """
            version: tapstate/v1
            kind: pipeline
            id: orders_out
            source: src_orders
            transforms:
              - { id: keep, from: [orders], type: filter, expr: "op != 'd'" }
            serve: out
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

    @Test
    void aSyncDeclaredInAServeDefinitionIsRefusedEvenWhenItsTargetWasFiledEarlier() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_target_connector_serve"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            // A connection is filed on its own and referred to afterwards, which is how a deployment
            // is built up. Nothing about the document says which role it will be asked for, so filing
            // it is accepted.
            control.apply(Map.of("tgt_out.tap.yml", PG_TARGET));

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                    "src_orders.tap.yml", SOURCE,
                    "out.tap.yml", SERVE_DEFINITION,
                    "orders_out.tap.yml", PIPELINE_USING_THE_DEFINITION));

            assertThat(refusal.code()).isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(refusal.params())
                    .as("the document to edit is the definition the element is written in, and the "
                            + "field path is the one that resolves in it")
                    .containsEntry("connector", "postgres")
                    .containsEntry("resource", "out")
                    .containsEntry("path", "sync[0].source");
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch")
                    .doesNotContain("orders_out", "out", "src_orders");

            control.apply(Map.of(
                    "src_orders.tap.yml", SOURCE,
                    "tgt_out.tap.yml", ATLAS_TARGET,
                    "out.tap.yml", SERVE_DEFINITION,
                    "orders_out.tap.yml", PIPELINE_USING_THE_DEFINITION));

            assertThat(control.artifactIds())
                    .as("the same batch installs once the target is a connector of the write kind — "
                            + "a managed variant of it, which is what a deployment on one registers")
                    .contains("orders_out", "out", "tgt_out", "src_orders");
        }
    }

    @Test
    void awsRdsMysqlIsAcceptedAsAReadSourceButRefusedAsAWriteTarget() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_aws_rds_target_gate"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            String awsSource = SOURCE.replace("connector: mysql", "connector: aws-rds-mysql");

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                    "src_orders.tap.yml", awsSource,
                    "tgt_out.tap.yml", AWS_RDS_TARGET,
                    "orders_out.tap.yml", PIPELINE));
            assertThat(refusal.code()).isEqualTo(UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(refusal.params()).containsEntry("connector", "aws-rds-mysql")
                    .containsEntry("source", "tgt_out");
            assertThat(control.artifactIds()).doesNotContain("orders_out", "tgt_out", "src_orders");

            control.apply(Map.of(
                    "src_orders.tap.yml", awsSource,
                    "tgt_out.tap.yml", ATLAS_TARGET,
                    "orders_out.tap.yml", PIPELINE));
            assertThat(control.artifactIds()).contains("orders_out", "tgt_out", "src_orders");
        }
    }
}
