package io.tapstate.app;

import io.tapstate.adapters.pdk.PdkExternalCallStats;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.metrics.MetricsExport;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorCallFactsTest {

    private static final Instant AT = Instant.parse("2026-09-27T00:00:00Z");

    @Test
    void processProjectionHasFourFixedSeriesPerInstrumentWithoutConnectorOrPipelineLabels() {
        List<MetricFact> facts = ConnectorCallFacts.snapshot(new PdkExternalCallStats(true), AT, AT);
        assertThat(facts).extracting(MetricFact::name).containsExactly(
                "tapstate.process.connector.external.call.count",
                "tapstate.process.connector.external.call.duration");
        assertThat(facts.get(0).type()).isEqualTo(MetricType.COUNTER);
        assertThat(facts.get(1).type()).isEqualTo(MetricType.HISTOGRAM);
        for (MetricFact fact : facts) {
            assertThat(fact.points()).hasSize(4);
            assertThat(fact.points()).allSatisfy(point -> {
                assertThat(point.attributes()).containsOnlyKeys(
                        MetricAttributes.CONNECTOR_CALL, MetricAttributes.CONNECTOR_OUTCOME);
                assertThat(point.startTime()).isEqualTo(AT);
            });
        }
        assertThat(facts.get(0).points()).allSatisfy(point -> assertThat(point.value()).isZero());
        assertThat(facts.get(1).points()).allSatisfy(point ->
                assertThat(point.histogram().count()).isZero());
    }

    @Test
    void assemblyRegistersOnePullSupplierOnlyWhenExportIsConfigured() {
        CapturingExport export = new CapturingExport();
        Clock clock = Clock.fixed(AT, ZoneOffset.UTC);
        PdkExternalCallStats stats = new DataPlaneActuationConfiguration().pdkExternalCallStats(export, clock);
        assertThat(stats.snapshot()).hasSize(2);
        assertThat(export.source).isEqualTo("connector-calls");
        assertThat(export.facts.get()).hasSize(2);

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
