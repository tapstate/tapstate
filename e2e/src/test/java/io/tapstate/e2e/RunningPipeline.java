package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One pipeline brought all the way up: connector registered, rows laid down, source discovered, resources
 * applied, start driven, and the runtime observed to have reached {@code RUNNING}.
 *
 * <p>The removal rules this plan adds are read off documents a live runtime writes - a checkpoint, a
 * published observation, a cursor on a mining chain. A case that planted those documents directly would be
 * watching the gate read its own fixture, so the cases that assert on them pay for a real run and get one
 * here rather than each assembling it again.
 *
 * <p>The source is a cdc read over a directory, which does not end. That is deliberate and load-bearing:
 * a pipeline over a finite source completes on its own and would be at rest by the time a case got to it,
 * turning "refused because it is running" into a race the case loses at random.
 */
final class RunningPipeline {

    /** Rows seeded into the source table, enough that a run has something to carry. */
    private static final long SEEDED_ROWS = 4;

    private static final String TABLE = "orders";

    private final ControlPlane control;
    private final String pipelineId;
    private final String sourceId;
    private final String targetId;
    private final Path sourceDirectory;
    private final Path targetDirectory;

    private RunningPipeline(ControlPlane control, String pipelineId, String sourceId, String targetId,
                            Path sourceDirectory, Path targetDirectory) {
        this.control = control;
        this.pipelineId = pipelineId;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.sourceDirectory = sourceDirectory;
        this.targetDirectory = targetDirectory;
    }

    ControlPlane control() {
        return control;
    }

    String pipelineId() {
        return pipelineId;
    }

    String sourceId() {
        return sourceId;
    }

    String targetId() {
        return targetId;
    }

    Path targetDirectory() {
        return targetDirectory;
    }

    /** Lays down further changes at the source, for a case that needs to see the run still moving. */
    void insertAtSource(long rows) {
        new FileEndpoints().cdc(EndpointAddress.uri(sourceDirectory.toString()), TABLE, CdcOp.INSERT, rows);
    }

    /** The rows that have arrived at the target so far. */
    long rowsAtTarget() {
        return new FileEndpoints().count(EndpointAddress.uri(targetDirectory.toString()), TABLE);
    }

    /** Drives a stop and waits for the runtime to report it has reached rest. */
    void stopAndSettle() {
        control.stop(pipelineId, true);
        Await.until(
                pipelineId + " to reach " + PipelineState.STOPPED,
                () -> control.state(pipelineId).filter(PipelineState.STOPPED::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    /** Brings up a single pipeline with default ids, and returns once the runtime reports it running. */
    static RunningPipeline started(ServerHandle server, Path directory) throws Exception {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector(
                E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));

        String sourceId = "run_src";
        String targetId = "run_tgt";
        String pipelineId = "run_pipeline";
        Path sourceDirectory = Files.createDirectories(directory.resolve(sourceId));
        Path targetDirectory = Files.createDirectories(directory.resolve(targetId));
        // Seeded before the discovery: a model is read out of what the source holds, and this is what puts
        // the table there. Discovering an absent table answers an empty model, which leaves the sink with
        // no key to upsert on and does so quietly.
        new FileEndpoints().seed(EndpointAddress.uri(sourceDirectory.toString()), TABLE, SeedRows.generated(SEEDED_ROWS));

        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(sourceId + ".tap.yml", Workspaces.cdcSourceYaml(sourceId, sourceDirectory));
        resources.put(targetId + ".tap.yml", Workspaces.targetYaml(targetId, targetDirectory));
        resources.put(pipelineId + ".tap.yml", Workspaces.pipelineYaml(pipelineId, sourceId, targetId, TABLE));

        control.discoverSchema(sourceId, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));
        control.apply(resources);
        control.lifecycle(pipelineId, LifecycleVerb.START);

        RunningPipeline running = new RunningPipeline(
                control, pipelineId, sourceId, targetId, sourceDirectory, targetDirectory);
        Await.until(
                pipelineId + " to reach " + PipelineState.RUNNING,
                () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
        return running;
    }
}
