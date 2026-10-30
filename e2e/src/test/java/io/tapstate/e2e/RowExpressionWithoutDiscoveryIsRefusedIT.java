package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that reading a row field obliges the author to have discovered the source first, and
 * that a pipeline reading no row field carries no such obligation.
 *
 * <p>Both halves are here because either alone permits an outcome this test exists to exclude. The
 * refusal alone is satisfied by a product that demands discovery of everything, which would make the
 * obligation a tax on pipelines it cannot possibly protect; the acceptance alone is satisfied by a
 * product that demands discovery of nothing, which is the hole the obligation was written to close.
 * Together they pin the boundary at the one place it belongs: whether the expression reads the row.
 *
 * <p>The two pipelines differ in their expression and nothing else - same source, same target, same
 * shape, applied against the same undiscovered source. So a refusal that arrived for any other reason
 * would take the second half down with it rather than leaving a green half to explain it away.
 *
 * <p>The refusal is asserted by its code and by the server's own artifact listing, not by the code
 * alone: "refused, having already filed half the batch" and a clean refusal answer the same code, and
 * only the listing tells them apart. The listing is read over every artifact the batch submitted rather
 * than the pipeline alone: the source and target are the ones written first, so a batch that filed half
 * of itself is half-filed in exactly the artifacts a pipeline-only assertion never looks at.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else. That is
 * deliberate. The obligation's subject is whether an expression reads a row, which no real database is
 * required to demonstrate - and a witness that runs in an ordinary build guards it on every change,
 * where one gated on real connector jars would only be consulted at night.
 */
class RowExpressionWithoutDiscoveryIsRefusedIT {

    private static final String SOURCE_ID = "src_file";
    private static final String TARGET_ID = "tgt_file";
    private static final String PIPELINE_ID = "row_expression_pipeline";
    private static final String NEEDS_DISCOVERY = "dsl.row-expression-needs-discovery";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void readingARowFieldWithoutDiscoveryIsRefusedAndNothingIsFiled(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
            ControlPlane control = connected(server, storage, directory);

            ControlPlane.Refusal refusal =
                    control.applyExpectingRefusal(workspace(directory, "int(after.id) % 2 == 0"));

            assertThat(refusal.code())
                    .as("the code refusing a row expression over a source that was never discovered")
                    .isEqualTo(NEEDS_DISCOVERY);
            assertThat(control.artifactIds())
                    .as("what the server holds after refusing the batch - a refusal that filed any of "
                            + "it is not a refusal")
                    .doesNotContain(SOURCE_ID, TARGET_ID, PIPELINE_ID);
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aPipelineThatReadsNoRowFieldAppliesWithoutAnyDiscovery(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
            ControlPlane control = connected(server, storage, directory);

            // Envelope scalars only: the op a change carries, never a column of the row it carries.
            control.apply(workspace(directory, "op == 'i'"));

            assertThat(control.artifactIds())
                    .as("the pipeline is applied, because its expression reads nothing whose type "
                            + "discovery would have supplied")
                    .contains(PIPELINE_ID);
        }
    }

    private static ControlPlane connected(ServerHandle server, PreparedNativeStore storage, Path directory) throws Exception {
        ControlPlane control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        control.registerConnector(
                E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
        return control;
    }


    /** Source, target and a pipeline whose filter is the one thing that varies. */
    private static Map<String, String> workspace(Path directory, String filterExpression) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_file.tap.yml", endpointYaml("src_file", directory.resolve("src")));
        resources.put("tgt_file.tap.yml", endpointYaml("tgt_file", directory.resolve("tgt")));
        resources.put("pipeline.tap.yml", pipelineYaml(filterExpression));
        return resources;
    }

    private static String endpointYaml(String id, Path uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, uri);
    }

    private static String pipelineYaml(String filterExpression) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_file
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: picked, from: [orders], type: filter, expr: "%s" }
                serve:
                  from: picked
                  sync:
                    - source: tgt_file
                """
                .formatted(PIPELINE_ID, filterExpression);
    }
}
