package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A table with no rows when its pipeline starts still has a load, one that is over the moment it begins, and a
 * sink that spreads the table over several writers - as a sink of the default width does - holds the table's
 * changes until that load has landed at every one of them. The load has to be seen to land for the changes to
 * go: held for a load that never lands, they would never reach the target, while the pipeline went on reporting
 * RUNNING with nothing failed.
 */
class AnEmptyTablesChangesReachASinkOfSeveralWritersIT {

    private static final String SOURCE = "empty_table_source";
    private static final String PIPELINE = "empty_table_pipeline";
    private static final String TABLE = "orders";
    private static final int ROWS = 5;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void theChangesOfATableThatWasEmptyWhenItsPipelineStartedReachTheTarget(Tiers tier, @TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(sourceAddress, TABLE, List.of());

        String store = SharedMongo.replicaSetUrl("empty_table_" + tier.name().toLowerCase(Locale.ROOT));
        try (ServerHandle server = tier.launch(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(workspace(source, target));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the empty table's load to be read through", Duration.ofMinutes(2),
                    () -> control.snapshotTables(PIPELINE).containsKey(TABLE),
                    () -> "state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
            assertThat(control.executionPlan(PIPELINE).orElseThrow().nodes())
                    .as("the sink runs several writers, so the table's changes wait for its load to land")
                    .anySatisfy(node -> assertThat(node.effective()).isGreaterThan(1));

            files.cdc(sourceAddress, TABLE, CdcOp.INSERT, ROWS);

            Await.until("the empty table's changes to reach the target", Duration.ofMinutes(2),
                    () -> files.count(targetAddress, TABLE) == ROWS,
                    () -> "rows=" + files.count(targetAddress, TABLE) + ", snapshot="
                            + control.snapshotTables(PIPELINE) + ", state=" + control.state(PIPELINE)
                            + ", logs=" + control.logs(PIPELINE));
            Await.until("the empty table's load to read as landed", Duration.ofMinutes(2),
                    () -> control.snapshotTable(PIPELINE, TABLE).map(table -> table.landed()).orElse(false),
                    () -> "snapshot=" + control.snapshotTables(PIPELINE) + ", logs=" + control.logs(PIPELINE));
            assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
        }
    }

    private static Map<String, String> workspace(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, E2eConnectorJar.CONNECTOR_ID, source, TABLE));
        resources.put("target.tap.yml", Workspaces.targetYaml("target", target));
        resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, "target", TABLE));
        return resources;
    }
}
