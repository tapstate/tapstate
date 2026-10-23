package io.tapstate.e2e;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.spi.store.WorkloadClaimFence;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shipped CLI does not call ordinary millisecond-scale frontier pauses a stopped chain.
 *
 * <p>The observation is the exact shape reported by the first-run path: a live pipeline that has moved
 * eleven records, with two chains sampled as pinned for 9 ms and 196 ms. It is stored through the
 * production observation adapter, then read through a running server and a separate CLI process. The
 * publisher is deliberately outside this case: measuring the pinned duration is already its own contract;
 * this case is about the classification applied after that measurement reaches the observation face.
 *
 * <p>The earlier checklist rules are made false explicitly. The observation is current, carries no
 * failure, has no reconcile-failure streak, and has moved records. That leaves the frontier rule as the
 * only way this input can become a diagnosis. Restoring the old greater-than-zero test changes the CLI's
 * kind from {@code NO_MATCH} to {@code FRONTIER_STALLED}, so the assertion below fails on the regression
 * rather than merely proving that the command ran.
 */
class StatusDoesNotCallAnOrdinaryFrontierPauseStalledIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void millisecondScalePausesRemainAnUndiagnosedRunningPipeline(@TempDir Path directory) throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create(USER, PASSWORD);
                InProcessServer server = InProcessServer.startPrepared(storage)) {
            var running = RunningPipeline.started(server, directory, storage);
            String pipeline = running.pipelineId();
            assertThat(server.stopPublishingObservations()).isEqualTo(1);
            publishObservedPause(storage, pipeline);

            CliOnce.Run status = CliOnce.runWithPassword(PASSWORD,
                    "-c", server.baseUrl().toString(), "-u", USER, "status", pipeline);

            assertThat(status.exitCode())
                    .as("status must complete; stdout was:%n%s%nstderr was:%n%s", status.stdout(), status.stderr())
                    .isZero();
            assertThat(status.stdout())
                    .contains(pipeline + "  running")
                    .contains("kind       NO_MATCH")
                    .contains("read       metrics.frontierStalledMillis = {orders=196, shipments=9}")
                    .doesNotContain("FRONTIER_STALLED", "CHECK_TARGET");
        }
    }

    /** Stores the observed input through the same codec the running publisher uses. */
    private static void publishObservedPause(PreparedNativeStore storage, String pipeline) {
        try (var configuration = MongoConfigurationDomain.openExisting(
                    new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)));
                var cache = MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                        Duration.ofSeconds(5), "observation-fixture")) {
            var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
            var scope = configuration.intents().load(pipeline).orElseThrow().scope();
            var claim = cache.workloadClaims(business).readPipeline(scope).orElseThrow().claim();
            cache.pipelineObservations(scope, WorkloadClaimFence.from(claim), () -> {}).save(new Observation(
                    pipeline,
                    PipelineState.RUNNING,
                    Map.of("recordCount", 11L, "frontierStalledMillis.shipments", 9L, "frontierStalledMillis.orders", 196L),
                    Map.of(), Map.of(), null, Instant.now()));
        }
    }
}
