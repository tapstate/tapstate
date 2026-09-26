package io.tapstate.app;

import io.tapstate.adapters.otel.ExportSettings;
import io.tapstate.adapters.otel.OtelMetricsExport;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LifecycleProcessFactsScrapeTest {

    @Test
    void aRealScrapeShowsLifecyclePressureWithoutPipelineSeriesAndStaysQuietBeforeWork() throws Exception {
        int port;
        try (ServerSocket available = new ServerSocket(0)) {
            port = available.getLocalPort();
        }
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (OtelMetricsExport export = OtelMetricsExport.start(ExportSettings.prometheusOn("127.0.0.1", port));
                LifecycleWorkDispatcher dispatcher = new RuntimeConvergenceConfiguration()
                        .lifecycleWorkDispatcher(1, 1, export, new LifecyclePendingRegistry(), Clock.systemUTC())) {
            assertThat(scrape(port)).doesNotContain("tapstate_process_lifecycle_slots_active");
            dispatcher.offer("flow-a", new DesiredState("flow-a", PipelineState.RUNNING, "r1"), () -> {
                entered.countDown();
                try {
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("lifecycle test work interrupted", interrupted);
                }
                return new ConvergeResult(ConvergeStatus.NOTHING_TO_DO, Optional.empty(), Optional.empty());
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.recordVerb(LifecycleWorkDispatcher.Verb.START, TimeUnit.MILLISECONDS.toNanos(3));

            assertThat(scrape(port))
                    .containsPattern("tapstate_process_lifecycle_slots_active\\{[^}]*\\} 1")
                    .containsPattern("tapstate_process_lifecycle_pipelines_pending\\{[^}]*\\} 1")
                    .contains("tapstate_process_lifecycle_capacity_wait_duration_seconds_bucket")
                    .contains("tapstate_process_lifecycle_work_duration_seconds_bucket")
                    .contains("verb=\"start\"")
                    .doesNotContain("tapstate_pipeline_id");
            release.countDown();
        } finally {
            release.countDown();
        }
    }

    private static String scrape(int port) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }
}
