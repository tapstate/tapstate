package io.tapstate.app;

import io.tapstate.adapters.otel.ExportSettings;
import io.tapstate.adapters.otel.OtelMetricsExport;
import io.tapstate.control.core.TelemetryCleanupReporter;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.metrics.MetricsExport;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryCleanupHealthTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-27T06:00:00Z"), ZoneOffset.UTC);

    @Test
    void aScrapeAfterDeletionCannotSeeOldCurrentBeforeTheNewExecutionExports() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        try (OtelMetricsExport export = OtelMetricsExport.start(
                ExportSettings.prometheusOn("127.0.0.1", port))) {
            export.bindCurrentScopes(id -> scopes.current(id).map(scope -> new MetricsExport.ScopeToken(
                    scope.pipelineIncarnationId(), scope.executionGeneration())));
            TelemetryCleanupHealth health = new TelemetryCleanupHealth(CLOCK, export, event -> { },
                    (id, incarnation) -> {
                        scopes.forgetIncarnation(id, incarnation);
                        export.forgetIncarnation(id, incarnation);
                    });
            scopes.begin("orders", "inc-old", 41);
            export.offerFoldedScoped("orders", new MetricsExport.ScopeToken("inc-old", 41),
                    PipelineState.RUNNING, CLOCK.instant(), List.of(driven(3, CLOCK.instant())));
            assertThat(scrape(port)).contains("tapstate_pipeline_id=\"orders\"");

            health.removed("orders", "inc-old");
            assertThat(scrape(port)).doesNotContain("tapstate_pipeline_id=\"orders\"");

            scopes.begin("orders", "inc-new", 42);
            export.offerFoldedScoped("orders", new MetricsExport.ScopeToken("inc-new", 42),
                    PipelineState.RUNNING, CLOCK.instant().plusSeconds(1),
                    List.of(driven(7, CLOCK.instant().plusSeconds(1))));
            health.removed("orders", "inc-old");
            assertThat(scrape(port)).containsPattern("tapstate_pipeline_records_driven_total\\{[^}]*\\} 7")
                    .doesNotContain("pipelineIncarnationId", "executionGeneration");
        }
    }

    private static MetricFact driven(long value, Instant at) {
        return MetricFact.single("tapstate.pipeline.records.driven", MetricType.COUNTER, "{record}",
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "orders"),
                        at, at, value));
    }

    private static String scrape(int port) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }

    @Test
    void failureProducesLowCardinalityHealthAndAnOldOwnerEventOffer() {
        List<PipelineEvent> offered = new ArrayList<>();
        AtomicReference<Supplier<List<MetricFact>>> process = new AtomicReference<>();
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void forgetPipelinesOutside(Collection<String> ids) { }
            @Override public void observeProcess(String source, Supplier<List<MetricFact>> facts) {
                assertThat(source).isEqualTo("telemetry-cleanup");
                process.set(facts);
            }
        };
        TelemetryCleanupHealth health = new TelemetryCleanupHealth(CLOCK, export, offered::add);
        assertThat(process.get()).isNotNull();
        assertThat(process.get().get()).isEmpty();

        health.failed(new TelemetryCleanupReporter.Failure("flow", "inc-old", OptionalLong.of(4),
                "event-history", true));

        assertThat(health.health()).isEqualTo(new TelemetryCleanupHealth.Health(1, 1, true));
        assertThat(offered).singleElement().satisfies(event -> {
            assertThat(event.pipelineId()).isEqualTo("flow");
            assertThat(event.pipelineIncarnationId()).isEqualTo("inc-old");
            assertThat(event.executionGeneration()).isEqualTo(4);
            assertThat(event.kind()).isEqualTo(PipelineEvent.Kind.CLEANUP_INCOMPLETE);
            assertThat(event.reason()).isEqualTo("event-history");
        });
        assertThat(process.get()).isNotNull();
        assertThat(process.get().get()).extracting(MetricFact::name).containsExactly(
                "tapstate.process.telemetry.cleanup.failure",
                "tapstate.process.telemetry.cleanup.rejected",
                "tapstate.process.telemetry.cleanup.degraded");
        assertThat(process.get().get()).allSatisfy(fact ->
                assertThat(fact.points()).allSatisfy(point -> assertThat(point.attributes()).isEmpty()));
    }

    @Test
    void noExistingExecutionStillReportsFailureWithoutInventingAnEventOwner() {
        List<PipelineEvent> offered = new ArrayList<>();
        TelemetryCleanupHealth health = new TelemetryCleanupHealth(CLOCK, MetricsExport.none(), offered::add);

        health.failed(new TelemetryCleanupReporter.Failure("flow", "inc-old", OptionalLong.empty(),
                "observation", false));

        assertThat(offered).isEmpty();
        assertThat(health.health()).isEqualTo(new TelemetryCleanupHealth.Health(1, 0, true));
    }
}
