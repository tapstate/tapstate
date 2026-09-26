package io.tapstate.adapters.otel;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.tapstate.core.lifecycle.MetricFact;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ObservedMetricExporterTest {

    @Test
    void asynchronousResultsAreMeasuredAndExporterContractsAreDelegated() {
        FakeExporter delegate = new FakeExporter();
        ObservedMetricExporter observed = new ObservedMetricExporter(delegate);

        assertThat(observed.getAggregationTemporality(InstrumentType.COUNTER))
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(observed.getDefaultAggregation(InstrumentType.COUNTER))
                .isSameAs(delegate.aggregation);
        assertThat(observed.getMemoryMode()).isEqualTo(MemoryMode.REUSABLE_DATA);
        assertThat(observed.flush()).isSameAs(delegate.flush);
        assertThat(observed.shutdown()).isSameAs(delegate.shutdown);

        CompletableResultCode rejected = delegate.next = new CompletableResultCode();
        assertThat(observed.export(List.of())).isSameAs(rejected);
        assertThat(value(observed, "tapstate.process.otlp.export.failure")).isZero();
        rejected.fail();
        assertThat(value(observed, "tapstate.process.otlp.export.failure")).isEqualTo(1);
        assertThat(value(observed, "tapstate.process.otlp.export.degraded")).isEqualTo(1);
        assertThat(observed.healthFacts()).noneMatch(fact ->
                fact.name().equals("tapstate.process.otlp.export.last_success.age"));

        CompletableResultCode accepted = delegate.next = new CompletableResultCode();
        assertThat(observed.export(List.of())).isSameAs(accepted);
        accepted.succeed();
        assertThat(value(observed, "tapstate.process.otlp.export.success")).isEqualTo(1);
        assertThat(value(observed, "tapstate.process.otlp.export.failure")).isEqualTo(1);
        assertThat(value(observed, "tapstate.process.otlp.export.degraded")).isZero();
        assertThat(value(observed, "tapstate.process.otlp.export.last_success.age")).isGreaterThanOrEqualTo(0);
    }

    private static long value(ObservedMetricExporter exporter, String name) {
        return exporter.healthFacts().stream().filter(fact -> fact.name().equals(name)).findFirst()
                .map(MetricFact::points).orElseThrow().getFirst().value();
    }

    private static final class FakeExporter implements MetricExporter {
        private final Aggregation aggregation = Aggregation.lastValue();
        private final CompletableResultCode flush = CompletableResultCode.ofSuccess();
        private final CompletableResultCode shutdown = CompletableResultCode.ofSuccess();
        private CompletableResultCode next;

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.DELTA;
        }

        @Override
        public Aggregation getDefaultAggregation(InstrumentType instrumentType) {
            return aggregation;
        }

        @Override
        public MemoryMode getMemoryMode() {
            return MemoryMode.REUSABLE_DATA;
        }

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            return next;
        }

        @Override
        public CompletableResultCode flush() {
            return flush;
        }

        @Override
        public CompletableResultCode shutdown() {
            return shutdown;
        }
    }
}
