package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeQueueProbeTest {
    private static Map<String, Object> vertex(Object index) {
        return Map.of("name", "sink", "executionId", "exec", "processors",
                List.of(Map.of("memberUuid", "member", "index", index)));
    }

    @Test void completeTopologyUsesExactProcessorAndExecutionIdentities() {
        var roster = BenchmarkNativeQueueProbe.topologyRoster(Map.of("vertices", List.of(vertex(BigDecimal.ZERO))), "member");
        assertThat(roster.identities()).containsExactly("member/sink/0");
        assertThat(roster.executions()).containsExactly("exec");
    }

    @Test void malformedVertexCannotDisappearFromAQualifiedSubset() {
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(0), Map.of("name", "missing"))), "member"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("incomplete execution identity");
    }

    @Test void fractionalAndDuplicateProcessorsCannotBecomeACompleteRoster() {
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(new BigDecimal("0.5")))), "member"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exact integer");
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(0), vertex(0))), "member"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicate");
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(0))), "other"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("different processor");
    }

    @Test void deliveryKeepsEverySettledCellAndOnlyTheDeclaredOutputSinkRoster() {
        var metrics = JobMetrics.of(Map.of(
                "recordsOut.r.public.orders", List.of(row("recordsOut.r.public.orders", "serve.out", "0", 7, 5)),
                "recordsOut.u.public.orders", List.of(row("recordsOut.u.public.orders", "serve.out", "0", 7, 3)),
                "recordsOut.i.view.table", List.of(row("recordsOut.i.view.table", "view.saved", "1", 7, 2)),
                MetricNames.RECEIVED_COUNT, List.of(row(MetricNames.RECEIVED_COUNT, "source.in", "0", 99, 100))));
        var snapshot = delivery(metrics, topology());
        assertThat(snapshot.pipeline()).isEqualTo("pipeline");
        assertThat(snapshot.jobId()).isEqualTo("job"); assertThat(snapshot.memberUuid()).isEqualTo("member");
        assertThat(snapshot.executionId()).isEqualTo("exec"); assertThat(snapshot.publicationStamp()).isEqualTo(7);
        assertThat(snapshot.readStartedAtNanos()).isEqualTo(-10); assertThat(snapshot.readCompletedAtNanos()).isEqualTo(10);
        assertThat(snapshot.expectedSinkIdentities()).containsExactlyInAnyOrder("member/serve.out/0", "member/view.saved/1");
        assertThat(snapshot.counters()).hasSize(3)
                .containsEntry("recordsOut.r.public.orders|member/serve.out/0", 5L)
                .containsEntry("recordsOut.u.public.orders|member/serve.out/0", 3L)
                .containsEntry("recordsOut.i.view.table|member/view.saved/1", 2L);
        assertThatThrownBy(snapshot.counters()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(snapshot.expectedSinkIdentities()::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void everyDeliverySinkMustHaveAnActualCounterAndNoForeignSinkMayAppear() {
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders",
                List.of(row("recordsOut.r.orders", "serve.out", "0", 7, 1)))), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("roster is incomplete");
        assertThatThrownBy(() -> delivery(JobMetrics.empty(), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("roster is incomplete");
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders",
                List.of(row("recordsOut.r.orders", "serve.foreign", "0", 7, 1)))), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("another identity");
    }

    @Test void mixedPublicationOrDuplicateDeliveryCellsCannotFormOneSnapshot() {
        var first = row("recordsOut.r.orders", "serve.out", "0", 7, 5);
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders", List.of(first, first))), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicate");
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders", List.of(first,
                row("recordsOut.r.orders", "view.saved", "1", 8, 2)))), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("mix publication");
    }

    @Test void deliveryMetricIdentityMustMatchEveryActualSdkTag() {
        for (String changed : List.of(MetricTags.MEMBER, MetricTags.JOB, MetricTags.EXECUTION, MetricTags.PROCESSOR)) {
            var tags = tags("serve.out", "0"); tags.put(changed, "foreign");
            var bad = Measurement.of("recordsOut.r.orders", 1, 7, tags);
            assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders", List.of(bad))), topology()))
                    .isInstanceOf(AssertionError.class);
        }
        var tags = tags("serve.out", "0"); tags.remove(MetricTags.JOB);
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders",
                List.of(Measurement.of("recordsOut.r.orders", 1, 7, tags)))), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("identity field is missing");
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders",
                List.of(row("recordsOut.u.orders", "serve.out", "0", 7, 1)))), topology()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("another identity");
    }

    @Test void incompleteTopologyAndMixedExecutionsStayUnknownForDelivery() {
        var metrics = JobMetrics.of(Map.of("recordsOut.r.orders", List.of(row("recordsOut.r.orders", "serve.out", "0", 7, 1))));
        assertThatThrownBy(() -> delivery(metrics, Map.of("vertices", List.of(
                sinkVertex("serve.out", "exec", 0), Map.of("name", "source.in")))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("incomplete execution identity");
        assertThatThrownBy(() -> delivery(metrics, Map.of("vertices", List.of(
                sinkVertex("serve.out", "exec", 0), sinkVertex("view.saved", "other", 1)))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unique execution");
        assertThatThrownBy(() -> delivery(metrics, Map.of("vertices", List.of(sinkVertex("source.in", "exec", 0)))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("sink roster is missing");
        assertThatThrownBy(() -> delivery(metrics, Map.of("vertices", List.of(
                sinkVertex("serve.out", "exec", new BigDecimal("0.5"))))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exact integer");
    }

    @Test void countersRequestOrderAndDelimitedFieldsHaveNoLossyFallback() {
        var topology = Map.of("vertices", List.of(sinkVertex("serve.out", "exec", 0)));
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders",
                List.of(row("recordsOut.r.orders", "serve.out", "0", 7, -1)))), topology))
                .isInstanceOf(AssertionError.class).hasMessageContaining("invalid counter");
        var valid = JobMetrics.of(Map.of("recordsOut.r.orders", List.of(row("recordsOut.r.orders", "serve.out", "0", 7, 0))));
        assertThat(delivery(valid, topology).counters()).containsValue(0L);
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.deliverySnapshot("pipeline", "job", "member", valid,
                topology, 11, 10)).isInstanceOf(AssertionError.class).hasMessageContaining("clock moved backward");
        assertThatThrownBy(() -> delivery(JobMetrics.of(Map.of("recordsOut.r.orders|other",
                List.of(row("recordsOut.r.orders|other", "serve.out", "0", 7, 1)))), topology))
                .isInstanceOf(AssertionError.class).hasMessageContaining("ambiguous");
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.deliverySnapshot("x".repeat(513), "job", "member", valid,
                topology, 0, 1)).isInstanceOf(AssertionError.class).hasMessageContaining("exceeds its bound");
    }

    @Test void deliveryCounterAndSinkRostersKeepTheirDeclaredBounds() {
        Map<String, List<Measurement>> cells = new LinkedHashMap<>();
        for (int index = 0; index <= 4096; index++) {
            String name = "recordsOut.r.orders_" + index;
            cells.put(name, List.of(row(name, "serve.out", "0", 7, 1)));
        }
        assertThatThrownBy(() -> delivery(JobMetrics.of(cells), Map.of("vertices", List.of(sinkVertex("serve.out", "exec", 0)))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter is duplicate or exceeds its bound");
        List<Map<String, Object>> processors = new ArrayList<>();
        for (int index = 0; index < 129; index++) processors.add(Map.of("memberUuid", "member", "index", index));
        assertThatThrownBy(() -> delivery(JobMetrics.empty(), Map.of("vertices", List.of(Map.of(
                "name", "serve.out", "executionId", "exec", "processors", processors)))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("sink roster is missing or exceeds its bound");
    }

    private static BenchmarkNativeCounterBaseline.Snapshot delivery(JobMetrics metrics, Map<?, ?> topology) {
        return BenchmarkNativeQueueProbe.deliverySnapshot("pipeline", "job", "member", metrics, topology, -10, 10);
    }

    private static Map<String, Object> topology() {
        return Map.of("vertices", List.of(sinkVertex("source.in", "exec", 0),
                sinkVertex("serve.out", "exec", 0), sinkVertex("view.saved", "exec", 1)));
    }

    private static Map<String, Object> sinkVertex(String name, String execution, Object index) {
        return Map.of("name", name, "executionId", execution, "processors", List.of(Map.of("memberUuid", "member", "index", index)));
    }

    private static Measurement row(String metric, String vertex, String processor, long publication, long value) {
        return Measurement.of(metric, value, publication, tags(vertex, processor));
    }

    private static Map<String, String> tags(String vertex, String processor) {
        Map<String, String> result = new LinkedHashMap<>();
        result.put(MetricTags.JOB, "job"); result.put(MetricTags.MEMBER, "member");
        result.put(MetricTags.EXECUTION, "exec"); result.put(MetricTags.VERTEX, vertex); result.put(MetricTags.PROCESSOR, processor);
        return result;
    }
}
