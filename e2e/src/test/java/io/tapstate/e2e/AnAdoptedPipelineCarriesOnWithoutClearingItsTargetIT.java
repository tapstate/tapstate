package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipeline a restarted server carries on is neither asked about nor cleared, even when its target is set to
 * be cleared before a new full load.
 *
 * <p>The question a start asks belongs to the verb a person sends. A pipeline the product brings back by
 * itself - a server restarted onto the store it left - is carrying on from the position it recorded, not
 * loading afresh, so there is nobody to ask and nothing to clear: {@code on_full_load: clear} is for the next
 * new full load, and this is not one. Two things hold that: the run's start judged as a resume, and the
 * receipt the target's first preparation left behind. Each is checked on its own below this level; what this
 * case reads is what they hold together, and an implementation that lost both clears the target on the way
 * up.
 *
 * <p>The witness is a row the source never had, put at the target while no server is running. Clearing would
 * take it, and reloading would not bring it back, so it is the one reading that tells "carried on" from
 * "cleared and loaded again" - a row count cannot, both end with the source's rows at the target. It is read
 * after a change made at the source crosses the restarted run, which is after the run prepared its target.
 *
 * <p>Java rather than a declarative example, and the reason is a missing word: the specification has no
 * step that ends the server process and brings another up on the store it left.
 */
class AnAdoptedPipelineCarriesOnWithoutClearingItsTargetIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "adopted_keeps_target";
    private static final long SEEDED = 4;
    private static final long FOREIGN_ID = 900;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRestartedServerCarriesOnAPipelineSetToClearWithoutClearingItsTarget(Tiers tier) throws Exception {
        Path directory = Files.createTempDirectory("adopted-keeps-target");
        String storeUri = SharedMongo.replicaSetUrl("adopted_keeps_target_" + tier.name().toLowerCase(Locale.ROOT));
        EndpointAddress source = EndpointAddress.uri(Files.createDirectories(directory.resolve("src")).toString());
        EndpointAddress target = EndpointAddress.uri(Files.createDirectories(directory.resolve("tgt")).toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(source, TABLE, SeedRows.generated(SEEDED));

        try (ServerHandle first = tier.launch(storeUri)) {
            ControlPlane control = new ControlPlane(first.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.discoverSchema("src_file", E2eConnectorJar.CONNECTOR_ID, source.settings());
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_file.tap.yml", Workspaces.cdcSourceYaml("src_file", Path.of(source.text("uri"))));
            resources.put("tgt_file.tap.yml", Workspaces.targetYaml("tgt_file", Path.of(target.text("uri"))));
            resources.put(PIPELINE + ".tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_file
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: %s
                      sync:
                        - source: tgt_file
                          on_full_load: clear
                    """.formatted(PIPELINE, TABLE));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the first full load", () -> files.count(target, TABLE) == SEEDED,
                    () -> "rows=" + files.count(target, TABLE));
        }

        // No server is running: nothing but this case touches the target now.
        files.insert(target, TABLE, List.of(Map.of("id", FOREIGN_ID, "seq", FOREIGN_ID)));

        try (ServerHandle second = tier.launch(storeUri)) {
            ControlPlane control = new ControlPlane(second.baseUrl());
            control.login("e2e", "e2e-password");
            assertThat(control.state(PIPELINE))
                    .as("the pipeline the restarted server carried on, with nobody asked")
                    .contains(PipelineState.RUNNING);

            files.cdc(source, TABLE, CdcOp.INSERT, 1);
            Await.until("a change made after the restart to cross the restarted run",
                    () -> files.fetch(target, TABLE, Map.of("id", SEEDED + 1)).isPresent(),
                    () -> "rows=" + files.count(target, TABLE));

            assertThat(files.fetch(target, TABLE, Map.of("id", FOREIGN_ID)))
                    .as("the row put at the target while no server ran - a target cleared on the way up "
                            + "would have lost it, and no reload brings it back")
                    .isPresent();
            assertThat(files.count(target, TABLE)).as("rows at the target").isEqualTo(SEEDED + 2);
        }
    }
}
