package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The declarative vocabulary cannot observe the cutoff or the checkpoint used to restart a quiet source. */
class DirectTimestampHeartbeatIT {
    private static final String PIPELINE = "quiet_timestamp_tail";
    private static final String SOURCE = "quiet_source";
    private static final Instant CUTOFF = Instant.parse("2026-01-01T00:00:01Z");
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    void anUntimedChangeOnAConnectorWorkerFailsThePipeline(@TempDir Path directory) throws Exception {
        DockerGate.require();
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,priority\n");
        Path delegate = E2eConnectorJar.buildInto(Files.createDirectory(directory.resolve("connector")));
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, CdcRecoveryFixture.connectorJar(delegate));
            Map<String, String> resources = workspace(source, target);
            control.apply(Map.of("source.tap.yml", resources.get("source.tap.yml")));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the timestamp stream to start", TIMEOUT,
                    () -> streamHasLine(source, "START 0 orders"), () -> control.logs(PIPELINE));

            CdcRecoveryFixture.deliverUntimedChangeOnWorker(source);
            Await.until("the worker to throw and the stream read to return", TIMEOUT,
                    () -> streamHasLine(source, "CALLBACK_FAILED") && streamHasLine(source, "END 0"),
                    () -> control.logs(PIPELINE));
            Await.until("the pipeline to expose the projection failure", TIMEOUT,
                    () -> control.state(PIPELINE).orElse(null) == PipelineState.FAILED
                            && control.failureCode(PIPELINE).isPresent(),
                    () -> "state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
            assertThat(control.failureCode(PIPELINE)).contains(ConnectorError.PROJECTION_FAILED.code());
            assertThat(rows(target)).isEmpty();
        }
    }

    @Test
    void quietCheckpointsAdvanceAndTheRestartKeepsFiltering(@TempDir Path directory) throws Exception {
        DockerGate.require();
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,priority\n");
        Path delegate = E2eConnectorJar.buildInto(Files.createDirectory(directory.resolve("connector")));
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, CdcRecoveryFixture.connectorJar(delegate));
            Map<String, String> resources = workspace(source, target);
            control.apply(Map.of("source.tap.yml", resources.get("source.tap.yml")));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the initial source anchor", TIMEOUT,
                    () -> streamHasLine(source, "START 0 orders") && checkpoint(documents) != null,
                    () -> String.valueOf(documents.directChannel(PIPELINE, SOURCE)));
            String anchor = checkpoint(documents);

            CdcRecoveryFixture.heartbeat(source, 1);
            awaitSettled(documents, 1, 1);
            String firstHeartbeat = checkpoint(documents);
            assertThat(firstHeartbeat).isNotEqualTo(anchor);
            CdcRecoveryFixture.heartbeat(source, 2);
            awaitSettled(documents, 1, 2);
            assertThat(checkpoint(documents)).isNotEqualTo(firstHeartbeat);
            assertThat(rows(target)).isEmpty();
            assertThat(documents.directChannel(PIPELINE, SOURCE).getString("pendingStartAt"))
                    .isEqualTo(CUTOFF.toString());

            control.stop(PIPELINE, false);
            Await.until("the original quiet stream to stop", TIMEOUT,
                    () -> control.state(PIPELINE).orElse(null) == PipelineState.STOPPED && streamHasLine(source, "END 2"),
                    () -> control.logs(PIPELINE).toString());
            CdcRecoveryFixture.changeAt(source, 3, CUTOFF.minusMillis(1), "orders", "early", "", "skip");
            CdcRecoveryFixture.changeAt(source, 4, CUTOFF, "orders", "first", "", "keep");
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the replacement to read from the last heartbeat", TIMEOUT,
                    () -> streamHasLine(source, "START 2 orders"), () -> control.logs(PIPELINE).toString());
            awaitSettled(documents, 2, 1);
            assertThat(rows(target)).containsExactly("first,keep");
            assertThat(documents.directChannel(PIPELINE, SOURCE).getString("pendingStartAt")).isNull();
        }
    }

    private static void awaitSettled(NativeSrsDocuments documents, long epoch, long ordinal) {
        Await.until("the private source receipt " + epoch + "/" + ordinal + " to settle", TIMEOUT, () -> {
            Document channel = documents.directChannel(PIPELINE, SOURCE);
            Document window = channel == null ? null : channel.get("current", Document.class);
            return window != null && Long.valueOf(epoch).equals(window.getLong("epoch"))
                    && window.getLong("settledThrough") >= ordinal;
        }, () -> String.valueOf(documents.directChannel(PIPELINE, SOURCE)));
    }

    private static String checkpoint(NativeSrsDocuments documents) {
        Document channel = documents.directChannel(PIPELINE, SOURCE);
        if (channel == null) return null;
        Document confirmed = channel.get("cursor", Document.class).get("sinkAcked", Document.class);
        return confirmed == null ? null : confirmed.getString("token");
    }

    private static boolean streamHasLine(Path source, String line) {
        return CdcRecoveryFixture.streams(source).stream()
                .anyMatch(stream -> CdcRecoveryFixture.lines(stream).contains(line));
    }

    private static List<String> rows(Path target) {
        return CdcRecoveryFixture.lines(target.resolve("orders.csv")).stream().skip(1).toList();
    }

    private static Map<String, String> workspace(Path source, Path target) {
        return Map.of(
                "source.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: %s
                        connector: e2e_file
                        config: { uri: "%s" }
                        mode: cdc
                        tables: [orders]
                        srs: {enabled: false}
                        """.formatted(SOURCE, source),
                "target.tap.yml", Workspaces.targetYaml("quiet_target", target),
                "pipeline.tap.yml", """
                        version: tapstate/v1
                        kind: pipeline
                        id: %s
                        source: %s
                        settings: {read_mode: cdc_only, start_from: "%s"}
                        serve:
                          from: orders
                          sync:
                            - source: quiet_target
                        """.formatted(PIPELINE, SOURCE, CUTOFF));
    }
}
