package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

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
    private static final String PIPELINE = "ordinary_frontier_pause";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void millisecondScalePausesRemainAnUndiagnosedRunningPipeline() {
        String storeUri = SharedMongo.replicaSetUrl("ordinary_frontier_pause");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);
            publishObservedPause(storeUri);

            CliOnce.Run status = CliOnce.runWithPassword(PASSWORD,
                    "-c", server.baseUrl().toString(), "-u", USER, "status", PIPELINE);

            assertThat(status.exitCode())
                    .as("status must complete; stdout was:%n%s%nstderr was:%n%s", status.stdout(), status.stderr())
                    .isZero();
            assertThat(status.stdout())
                    .contains(PIPELINE + "  running")
                    .contains("kind       NO_MATCH")
                    .contains("read       metrics.frontierStalledMillis = {}")
                    .doesNotContain("FRONTIER_STALLED", "CHECK_TARGET");
        }
    }

    /** Stores the observed input through the same codec the running publisher uses. */
    private static void publishObservedPause(String storeUri) {
        ConnectionString connection = new ConnectionString(storeUri);
        String database = connection.getDatabase();
        if (database == null) {
            throw new AssertionError("the store URL names no database: " + storeUri);
        }
        try (MongoClient client = MongoClients.create(connection)) {
            MongoObservationStore observations = new MongoObservationStore(client.getDatabase(database)
                    .getCollection(MongoStorePort.PIPELINE_OBSERVATION));
            observations.save(new Observation(
                    PIPELINE,
                    PipelineState.RUNNING,
                    Map.of(
                            "recordCount", 11L,
                            "frontierStalledMillis.shipments", 9L,
                            "frontierStalledMillis.orders", 196L),
                    Map.of(),
                    Map.of(),
                    null,
                    Instant.now()));
        }
    }
}
