package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The claim the two bindings exist to make: one specification, unchanged, runs on every tier.
 *
 * <p>This is checked by running the same YAML through the same executor twice, once per tier, rather
 * than by asserting the bindings look alike. A specification that passed on one tier and needed so
 * much as a different word on the other would fail here, which is the only place that promise can be
 * kept honest.
 *
 * <p>What it deliberately does not do is move data through a connector: no connector jar is
 * available to the build, so a specification that started a pipeline here would be testing the
 * absence of one. This checks what the binding itself owns - provisioning, endpoints, readings.
 */
class TierBindingsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL = Duration.ofMillis(200);

    /** Short on purpose: the run below waits for something that is never coming, and says so. */
    private static final Duration UNOBSERVED_BOUND = Duration.ofMillis(500);

    @TempDir
    private Path workspace;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void theSameSpecificationRunsOnEveryTier(Tiers tier) {
        String database = "e2e_both_" + tier.name().toLowerCase();
        String endpointUri = SharedMongo.replicaSetUrl(database);
        writeWorkspace();

        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(database + "_store"));
                MongoEndpoints endpoints = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding =
                    new HttpTierBinding(control, workspace, driversFor(endpoints), envFor(endpointUri));

            new E2eExecutor(binding, new FilePipelineLoader(workspace), TIMEOUT, POLL)
                    .execute(EnvelopeParser.parse(specification()));

            // The counts above move - three seeded, two more inserted - so a binding that answered
            // with a constant, or from its own record rather than the database, cannot satisfy both
            // assertions. This last read confirms the rows are really there, through a driver the
            // binding does not own.
            assertThat(endpoints.count(EndpointAddress.uri(endpointUri), "orders")).isEqualTo(5L);
            assertThat(control.artifactIds()).contains("tgt_mongo", "e2e_pipeline");
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void everyTierRefusesToReadAnEndpointTheSpecificationNeverApplied(Tiers tier) {
        String database = "e2e_unapplied_" + tier.name().toLowerCase();
        writeWorkspace();

        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(database + "_store"));
                MongoEndpoints endpoints = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, driversFor(endpoints), envFor(SharedMongo.replicaSetUrl(database)));

            assertThatThrownBy(() -> binding.count(new TableAlias("never_applied", "orders")))
                    .isInstanceOf(EnvelopeException.class)
                    .hasMessageContaining("no source declared for never_applied");
        }
    }

    /**
     * A pipeline that was applied and never started publishes no observation at all: the convergence pass
     * reconciles the pipelines that carry a recorded intent, and applying one records none. So the product
     * answers this read with its coded "no observation" refusal for as long as the specification cares to
     * ask - which makes it the one place the wait model's treatment of that answer can be pinned against a
     * real server rather than against a stand-in that never produces it.
     *
     * <p>Every real run passes through a shorter version of this window between {@code start} and the first
     * convergence pass, where an unpublished reading means "not yet" and nothing more.
     */
    @ParameterizedTest
    @EnumSource(Tiers.class)
    void awaitWaitsOutAPipelineThatHasPublishedNoObservation(Tiers tier) {
        String database = "e2e_unobserved_" + tier.name().toLowerCase();
        writeWorkspace();

        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(database + "_store"));
                MongoEndpoints endpoints = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, driversFor(endpoints), envFor(SharedMongo.replicaSetUrl(database)));

            long start = System.nanoTime();

            assertThatThrownBy(
                            () ->
                                    new E2eExecutor(
                                                    binding, new FilePipelineLoader(workspace), UNOBSERVED_BOUND, POLL)
                                            .execute(EnvelopeParser.parse(unstartedSpecification())))
                    // Reporting the unpublished read as the reading is what tells the author the pipeline
                    // never converged, rather than blaming a transport that answered exactly as designed.
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("e2e_pipeline expected RUNNING, found no published observation");

            // The message alone cannot tell a wait apart from a give-up: an executor that abandoned the run
            // on the first refusal would word its timeout identically and get here in microseconds. The clock
            // is the only witness that the refusal was polled through rather than thrown on.
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(UNOBSERVED_BOUND);
        }
    }

    /** The specification under test: identical text for both tiers, by construction. */
    private String specification() {
        return """
                name: both-tiers-see-the-same-endpoint
                setup:
                  apply: [tgt_mongo.tap.yml, pipeline.tap.yml]
                pipeline: pipeline.tap.yml
                seed:
                  tgt_mongo.orders: { rows: 3 }
                steps:
                  - assert: { count: { tgt_mongo.orders: 3 } }
                  - cdc: { tgt_mongo.orders: insert 2 }
                  - assert: { count: { tgt_mongo.orders: 5 } }
                """;
    }

    /** Applies a pipeline and never starts it, so nothing is ever published about it. */
    private String unstartedSpecification() {
        return """
                name: an-unstarted-pipeline-publishes-no-observation
                setup:
                  apply: [tgt_mongo.tap.yml, pipeline.tap.yml]
                pipeline: pipeline.tap.yml
                steps:
                  - await: { state: RUNNING }
                """;
    }

    private void writeWorkspace() {
        // The endpoint's address is only known once the container is up, and the resource names it the
        // way an author would: a ${...} reference the loading side resolves. What is checked in stays
        // legal product DSL, and the address stays out of it.
        write("tgt_mongo.tap.yml", """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                mode: cdc
                config: { uri: "${MONGO_URI}" }
                """);
        write("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: e2e_pipeline
                source: tgt_mongo
                serve:
                  from: /.*/
                  sync:
                    - source: tgt_mongo
                """);
    }

    /** The one connector these specifications reach, and the driver that reads it independently. */
    private static Map<String, Endpoints> driversFor(MongoEndpoints endpoints) {
        return Map.of("mongodb", endpoints);
    }

    /**
     * The environment the specification's references resolve against — supplied by the harness, which is
     * the client and therefore the interpolating side. A real author's shell plays this part.
     */
    private static UnaryOperator<String> envFor(String endpointUri) {
        return Map.of("MONGO_URI", endpointUri)::get;
    }

    private void write(String name, String content) {
        try {
            Files.writeString(workspace.resolve(name), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
