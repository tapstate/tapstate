package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** A disjoint consumer cannot fill another pipeline's table ring and stop its CDC delivery. */
class DisjointTableConsumersKeepSharedRingMovingIT {

    private static final String SOURCE = "disjoint_source";
    private static final String JOIN = "disjoint_join";
    private static final String NEST = "disjoint_nest";
    private static final String JOIN_TABLE = "join_orders";
    private static final String NEST_TABLE = "nest_items";
    private static final int BATCHES = 11;
    private static final int BATCH_SIZE = 100;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void anUnrelatedConsumerDoesNotPinTheRingAfterItFills(Tiers tier, @TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path joinTarget = Files.createDirectory(directory.resolve("join-target"));
        Path nestTarget = Files.createDirectory(directory.resolve("nest-target"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress joinAddress = EndpointAddress.uri(joinTarget.toString());
        EndpointAddress nestAddress = EndpointAddress.uri(nestTarget.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(sourceAddress, JOIN_TABLE, SeedRows.generated(1));
        files.seed(sourceAddress, NEST_TABLE, SeedRows.generated(1));

        String store = SharedMongo.replicaSetUrl("disjoint_ring_" + tier.name().toLowerCase(Locale.ROOT));
        try (ServerHandle server = tier.launch(store); StoreDocuments documents = StoreDocuments.at(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(workspace(source, joinTarget, nestTarget));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(JOIN, LifecycleVerb.START);
            control.lifecycle(NEST, LifecycleVerb.START);

            Await.until("both disjoint pipelines to consume one mining chain", Duration.ofMinutes(2),
                    () -> documents.miningChainIds().size() == 1
                            && documents.consumersOf(documents.miningChainIds().iterator().next())
                                    .equals(Set.of(JOIN, NEST))
                            && files.count(joinAddress, JOIN_TABLE) == 1
                            && files.count(nestAddress, NEST_TABLE) == 1,
                    () -> "chains=" + documents.miningChainIds() + ", join=" + control.state(JOIN)
                            + ", nest=" + control.state(NEST) + ", logs=" + control.logs(NEST));
            String chain = documents.miningChainIds().iterator().next();
            Document joinCursor = documents.consumerOffset(chain, JOIN).get("perTableSeq", Document.class);
            Document nestCursor = documents.consumerOffset(chain, NEST).get("perTableSeq", Document.class);
            assertThat(joinCursor.keySet()).containsExactly(JOIN_TABLE);
            assertThat(nestCursor.keySet()).containsExactly(NEST_TABLE);

            for (int batch = 1; batch <= BATCHES; batch++) {
                files.cdc(sourceAddress, NEST_TABLE, CdcOp.INSERT, BATCH_SIZE);
                long expected = 1L + (long) batch * BATCH_SIZE;
                Await.until("Nest changes beyond the ring capacity to reach the target", Duration.ofMinutes(2),
                        () -> files.count(nestAddress, NEST_TABLE) == expected,
                        () -> "nest rows=" + files.count(nestAddress, NEST_TABLE)
                                + ", expected=" + expected
                                + ", cursor=" + documents.consumerReadSeq(chain, NEST, NEST_TABLE)
                                + ", join=" + control.state(JOIN) + ", nest=" + control.state(NEST)
                                + ", logs=" + control.logs(NEST));
            }
            assertThat(files.count(joinAddress, JOIN_TABLE)).isEqualTo(1);
            assertThat(control.state(JOIN)).contains(PipelineState.RUNNING);
            assertThat(control.state(NEST)).contains(PipelineState.RUNNING);
        }
    }

    private static Map<String, String> workspace(Path source, Path joinTarget, Path nestTarget) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s, %s ]
                """.formatted(SOURCE, E2eConnectorJar.CONNECTOR_ID, source, JOIN_TABLE, NEST_TABLE));
        resources.put("join_target.tap.yml", Workspaces.targetYaml("join_target", joinTarget));
        resources.put("nest_target.tap.yml", Workspaces.targetYaml("nest_target", nestTarget));
        resources.put(JOIN + ".tap.yml", Workspaces.pipelineYaml(JOIN, SOURCE, "join_target", JOIN_TABLE));
        resources.put(NEST + ".tap.yml", Workspaces.pipelineYaml(NEST, SOURCE, "nest_target", NEST_TABLE));
        return resources;
    }
}
