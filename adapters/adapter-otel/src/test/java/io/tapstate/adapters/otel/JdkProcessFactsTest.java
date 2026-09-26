package io.tapstate.adapters.otel;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class JdkProcessFactsTest {

    private static final Instant START = Instant.parse("2026-09-27T00:00:00Z");
    private static final Instant NOW = START.plusSeconds(10);

    @Test
    void measuredResourcesCarryTheirActualUnitsAndProcessLifetime() {
        var probe = new JdkProcessFacts(() -> new JdkProcessFacts.Sample(
                1_750_000_000L, 0.735, 2048, 4096, 3, 11), START,
                Clock.fixed(NOW, ZoneOffset.UTC));

        Map<String, MetricFact> facts = byName(probe.snapshot());

        assertThat(facts).hasSize(6);
        assertReading(facts, "tapstate.process.cpu.time", MetricType.COUNTER, "ns", 1_750_000_000L);
        assertReading(facts, "tapstate.process.cpu.load", MetricType.GAUGE, "%", 74);
        assertReading(facts, "tapstate.process.jvm.heap.used", MetricType.GAUGE, "By", 2048);
        assertReading(facts, "tapstate.process.jvm.heap.committed", MetricType.GAUGE, "By", 4096);
        assertReading(facts, "tapstate.process.jvm.gc.collections", MetricType.COUNTER, "{collection}", 3);
        assertReading(facts, "tapstate.process.jvm.gc.collection.time", MetricType.COUNTER, "ms", 11);
        assertThat(facts.values()).allSatisfy(fact -> {
            assertThat(fact.points()).hasSize(1);
            assertThat(fact.points().getFirst().attributes()).isEmpty();
            assertThat(fact.points().getFirst().observedAt()).isEqualTo(NOW);
        });
    }

    @Test
    void unavailableReadingsRemainAbsentRatherThanBecomingZero() {
        var probe = new JdkProcessFacts(() -> new JdkProcessFacts.Sample(
                -1, -1, 2048, -1, -1, -1), START, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThat(probe.snapshot()).extracting(MetricFact::name)
                .containsExactly("tapstate.process.jvm.heap.used");
        assertThat(new JdkProcessFacts(() -> new JdkProcessFacts.Sample(
                -1, Double.NaN, -1, -1, -1, -1), START, Clock.fixed(NOW, ZoneOffset.UTC))
                .snapshot()).isEmpty();
    }

    @Test
    void aRealScrapeReadsHeapWithoutAnyPipelineOrDynamicResourceLabels() throws Exception {
        int port = freePort();
        try (OtelMetricsExport export = OtelMetricsExport.start(ExportSettings.prometheusOn("127.0.0.1", port))) {
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics"))
                            .GET().build(), HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body())
                    .contains("tapstate_process_jvm_heap_used_bytes{")
                    .contains("tapstate_process_jvm_heap_committed_bytes{")
                    .doesNotContain("tapstate_pipeline_state")
                    .doesNotContain("pipeline_id=");
            assertThat(response.body().lines().filter(line -> line.startsWith("tapstate_process_jvm_heap_used_bytes{")))
                    .hasSize(1);
        }
    }

    private static void assertReading(Map<String, MetricFact> facts, String name, MetricType type,
            String unit, long value) {
        MetricFact fact = facts.get(name);
        assertThat(fact).isNotNull();
        assertThat(fact.type()).isEqualTo(type);
        assertThat(fact.unit()).isEqualTo(unit);
        assertThat(fact.points().getFirst().value()).isEqualTo(value);
        assertThat(fact.points().getFirst().startTime()).isEqualTo(type == MetricType.COUNTER ? START : null);
    }

    private static Map<String, MetricFact> byName(List<MetricFact> facts) {
        return facts.stream().collect(Collectors.toMap(MetricFact::name, Function.identity()));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
