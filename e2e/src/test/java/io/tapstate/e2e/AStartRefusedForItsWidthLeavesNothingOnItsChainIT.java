package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A start refused for its width opens nothing for the run it would have been: no capture, and so no consumer on
 * the mining chain it would have read. A consumer left there reads nothing, and it holds every other pipeline
 * reading the same table on that chain at the first changes the table's ring can hold - each of them still
 * reporting RUNNING, with nothing failed.
 */
class AStartRefusedForItsWidthLeavesNothingOnItsChainIT {

    private static final String SOURCE = "refused_width_source";
    private static final String RUNNING = "refused_width_running";
    private static final String REFUSED = "refused_width_refused";
    private static final String TABLE = "orders";
    private static final int BATCHES = 11;
    private static final int BATCH_SIZE = 100;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRefusedStartLeavesNoConsumerToHoldTheTableItWouldHaveRead(Tiers tier, @TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path runningTarget = Files.createDirectory(directory.resolve("running-target"));
        Path refusedTarget = Files.createDirectory(directory.resolve("refused-target"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress runningAddress = EndpointAddress.uri(runningTarget.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(sourceAddress, TABLE, SeedRows.generated(1));

        String store = SharedMongo.replicaSetUrl("refused_width_" + tier.name().toLowerCase(Locale.ROOT));
        try (ServerHandle server = tier.launch(store); StoreDocuments documents = StoreDocuments.at(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(workspace(source, runningTarget, refusedTarget));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(RUNNING, LifecycleVerb.START);
            Await.until("the running pipeline to land the table's load", Duration.ofMinutes(2),
                    () -> files.count(runningAddress, TABLE) == 1,
                    () -> "running=" + control.state(RUNNING) + ", logs=" + control.logs(RUNNING));

            control.lifecycle(REFUSED, LifecycleVerb.START);
            Await.until("the start asking for more processors than any member runs to be refused",
                    Duration.ofMinutes(2), () -> control.failureCode(REFUSED).isPresent(),
                    () -> "refused=" + control.state(REFUSED) + ", logs=" + control.logs(REFUSED));
            assertThat(control.failureCode(REFUSED)).contains("actuation.no-safe-parallelism");

            for (int batch = 1; batch <= BATCHES; batch++) {
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, BATCH_SIZE);
                long expected = 1L + (long) batch * BATCH_SIZE;
                Await.until("changes past what the table's ring holds to reach the running pipeline's target",
                        Duration.ofMinutes(2), () -> files.count(runningAddress, TABLE) == expected,
                        () -> "rows=" + files.count(runningAddress, TABLE) + ", expected=" + expected
                                + ", consumers=" + consumers(documents) + ", running=" + control.state(RUNNING)
                                + ", logs=" + control.logs(RUNNING));
            }
            assertThat(control.state(RUNNING)).contains(PipelineState.RUNNING);
            assertThat(documents.miningChainIds()).hasSize(1);
            assertThat(documents.consumersOf(documents.miningChainIds().iterator().next()))
                    .as("the refused start joined no chain").containsExactly(SrsConsumerId.of(RUNNING, SOURCE).value());
        }
    }

    private static String consumers(StoreDocuments documents) {
        Map<String, Object> byChain = new LinkedHashMap<>();
        documents.miningChainIds().forEach(chain -> byChain.put(chain, documents.consumersOf(chain)));
        return byChain.toString();
    }

    private static Map<String, String> workspace(Path source, Path runningTarget, Path refusedTarget) {
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
        resources.put("running_target.tap.yml", Workspaces.targetYaml("running_target", runningTarget));
        resources.put("refused_target.tap.yml", Workspaces.targetYaml("refused_target", refusedTarget));
        resources.put(RUNNING + ".tap.yml", Workspaces.pipelineYaml(RUNNING, SOURCE, "running_target", TABLE));
        // A width this run cannot honour: far more processors of one step than a member runs.
        resources.put(REFUSED + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: wide, from: [ %s ], type: filter, expr: "op != 'x'", execution: { parallelism: 1000 } }
                serve:
                  from: wide
                  sync:
                    - source: refused_target
                """.formatted(REFUSED, SOURCE, TABLE));
        return resources;
    }
}
