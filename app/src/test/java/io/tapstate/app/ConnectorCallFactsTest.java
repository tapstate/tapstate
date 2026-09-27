package io.tapstate.app;

import io.tapstate.adapters.pdk.PdkExternalCallStats;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.spi.metrics.MetricsExport;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorCallFactsTest {

    private static final Instant AT = Instant.parse("2026-09-27T00:00:00Z");

    @Test
    void quietConnectorCallsDoNotCreateZeroSeries() {
        List<MetricFact> facts = ConnectorCallFacts.snapshot(new PdkExternalCallStats(true), AT, AT);
        assertThat(facts).isEmpty();
    }

    @Test
    void measuredWriteProducesOneFixedOutcomeSeriesWithoutIdentityLabels() {
        PdkExternalCallStats measured = mock(PdkExternalCallStats.class);
        HistogramBounds bounds = HistogramBounds.CONNECTOR_EXTERNAL_CALL_DURATION;
        List<Long> buckets = new java.util.ArrayList<>(java.util.Collections.nCopies(bounds.buckets(), 0L));
        buckets.set(3, 1L);
        when(measured.snapshot()).thenReturn(Map.of(PdkExternalCallStats.Call.SINK_WRITE,
                Map.of(PdkExternalCallStats.Outcome.SUCCESS,
                        new PdkExternalCallStats.Reading(1, bounds.value(1, 0.025, buckets)))));

        List<MetricFact> facts = ConnectorCallFacts.snapshot(measured, AT, AT);

        assertThat(facts).extracting(MetricFact::name).containsExactly(
                "tapstate.process.connector.external.call.count",
                "tapstate.process.connector.external.call.duration");
        assertThat(facts.get(0).type()).isEqualTo(MetricType.COUNTER);
        assertThat(facts.get(1).type()).isEqualTo(MetricType.HISTOGRAM);
        assertThat(facts).allSatisfy(fact -> assertThat(fact.points()).singleElement()
                .satisfies(point -> assertThat(point.attributes()).isEqualTo(Map.of(
                        MetricAttributes.CONNECTOR_CALL, "sink_write",
                        MetricAttributes.CONNECTOR_OUTCOME, "success"))));
        assertThat(facts.get(0).points().getFirst().value()).isEqualTo(1L);
        assertThat(facts.get(1).points().getFirst().histogram().count()).isEqualTo(1L);
    }

    @Test
    void assemblyRegistersOnePullSupplierOnlyWhenExportIsConfigured() {
        CapturingExport export = new CapturingExport();
        Clock clock = Clock.fixed(AT, ZoneOffset.UTC);
        PdkExternalCallStats stats = new DataPlaneActuationConfiguration().pdkExternalCallStats(export, clock);
        assertThat(stats.snapshot()).hasSize(2);
        assertThat(export.source).isEqualTo("connector-calls");
        assertThat(export.facts.get()).isEmpty();

        PdkExternalCallStats disabled = new DataPlaneActuationConfiguration()
                .pdkExternalCallStats(MetricsExport.none(), clock);
        assertThat(disabled.snapshot()).isEmpty();
    }

    private static final class CapturingExport implements MetricsExport {
        private String source;
        private Supplier<List<MetricFact>> facts;

        @Override
        public void observeProcess(String source, Supplier<List<MetricFact>> facts) {
            this.source = source;
            this.facts = facts;
        }

        @Override
        public void offer(String pipelineId, io.tapstate.core.lifecycle.PipelineState state,
                Instant observedAt, List<MetricFact> facts) {
        }

        @Override
        public void forgetPipelinesOutside(Collection<String> pipelineIds) {
        }
    }
}
