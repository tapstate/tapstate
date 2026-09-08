package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stop that clears takes every kind of state the pipeline accumulated, and does not touch the target.
 *
 * <p>Both halves, in one case, after one stop. That pairing is the whole of it. An implementation that
 * only let go of the resume position satisfies any case written about the target; one that swept the
 * user's target database along with everything else satisfies any case written about the state. The two
 * failures are opposite in direction and a case that asserts one is blind to the other, so the reason
 * this exists as a single scenario rather than two is that the two readings have to be taken of the same
 * stop.
 *
 * <p>What is checked as gone: the chain's durable record - the position a next run would resume from -
 * and the pipeline's published observation, which is where its counters are read from. What is checked
 * as untouched: the rows already written to the target, counted before the stop and again after it.
 *
 * <p><b>The nest layer is deliberately not asserted here, and the reason is worth stating.</b> A stop
 * drops the namespaces its pipeline <em>declares</em>, and this pipeline declares none: it carries no
 * nest. Seeding a namespace by hand and expecting the stop to find it would be asking the stop to reach
 * past its own contract, which is the very thing the neighbouring removal witness exists to say it must
 * not do - so that assertion would have passed only against an implementation behaving badly. The nest
 * layer's own stop witness is a unit case over an actuator with a nest-bearing pipeline in front of it,
 * where the declaration is real; what has no witness anywhere, and is what this case is for, is the two
 * readings taken either side of one stop.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class StopClearsEverythingButTheTargetIT {

    private static final String SOURCE_ID = "clearing_src";
    private static final String TARGET_ID = "clearing_tgt";
    private static final String PIPELINE_BASE = "clearing_pipeline";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 4;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aClearingStopTakesEveryKindOfStateAndLeavesTheTargetAlone(Tiers tier, @TempDir Path directory)
            throws Exception {
        String storeUri = SharedMongo.replicaSetUrl(
                "stop_clears_everything_" + tier.name().toLowerCase(Locale.ROOT));
        // The tier rides on the pipeline id, not on the namespace: the state database has a fixed name, so
        // on one Mongo the two tiers would otherwise seed over each other, and a namespace is built from
        // the id, so a cleanup keyed off that id has to be able to match what is seeded here.
        String pipelineId = PIPELINE_BASE + "_" + tier.name().toLowerCase(Locale.ROOT);
        Path targetDirectory = Files.createDirectories(directory.resolve(TARGET_ID));

        try (ServerHandle server = tier.launch(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));

            Path sourceDirectory = Files.createDirectories(directory.resolve(SOURCE_ID));
            FileEndpoints files = new FileEndpoints();
            files.seed(EndpointAddress.uri(sourceDirectory.toString()), TABLE, SeedRows.generated(SEEDED_ROWS));

            control.discoverSchema(
                    SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));
            control.apply(workspace(sourceDirectory, targetDirectory, pipelineId));
            control.lifecycle(pipelineId, LifecycleVerb.START);
            Await.until(
                    pipelineId + " to reach " + PipelineState.RUNNING,
                    () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));

            EndpointAddress target = EndpointAddress.uri(targetDirectory.toString());
            Await.until(
                    "the seeded rows to reach the target",
                    () -> files.count(target, TABLE) == SEEDED_ROWS,
                    () -> "rows at target = " + files.count(target, TABLE));

            // A chain with a record on it, which is the position a next run would resume from. Waited for
            // rather than assumed: a stop asserted to have taken it away has to be shown taking something.
            Await.until(
                    "the pipeline to hold a cursor on its mining chain",
                    () -> documents.miningChainIds().size() == 1
                            && documents.consumersOf(onlyChain(documents)).contains(pipelineId),
                    () -> "chains=" + documents.miningChainIds() + " consumers="
                            + documents.miningChainIds().stream().map(documents::consumersOf).toList());
            String chainId = onlyChain(documents);

            assertThat(control.recordCount(pipelineId))
                    .as("the pipeline published counters before the stop, so what is asserted after it "
                            + "is a difference the stop made rather than a reading that was never there")
                    .isPresent();

            long rowsBeforeTheStop = files.count(target, TABLE);

            control.stop(pipelineId, true);
            Await.until(
                    pipelineId + " to reach " + PipelineState.STOPPED,
                    () -> control.state(pipelineId).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));

            // Half one: what the run recorded is gone.
            assertThat(documents.miningChainIds())
                    .as("the chain's own record, which is where the position a next run would resume "
                            + "from is kept; this pipeline was the only one reading it, so the whole "
                            + "record is this stop's to take")
                    .doesNotContain(chainId);
            assertThat(control.recordCount(pipelineId))
                    .as("and the observation its counters are read from")
                    .isEmpty();

            // Half two, and it is the half a clearing that reached too far would fail. The rows are the
            // user's, written to a database the product was merely pointed at; nothing about stopping a
            // pipeline entitles it to any of them.
            assertThat(files.count(target, TABLE))
                    .as("the rows already written to the target, which a stop does not touch")
                    .isEqualTo(rowsBeforeTheStop);
        }
    }

    /** The single chain this pipeline reads through; asserted to be single before this is ever called. */
    private static String onlyChain(StoreDocuments documents) {
        return documents.miningChainIds().iterator().next();
    }

    private static Map<String, String> workspace(Path sourceDirectory, Path targetDirectory, String pipelineId)
            throws Exception {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, sourceDirectory));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, targetDirectory));
        resources.put(pipelineId + ".tap.yml",
                Workspaces.pipelineYaml(pipelineId, SOURCE_ID, TARGET_ID, TABLE));
        return resources;
    }
}
