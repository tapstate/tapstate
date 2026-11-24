package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineMetrics;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NestStateMetricsResponseTest {
    @Test void theExistingMetricsResponseCarriesNamespaceAndBucketCounts() {
        var instrument = HistogramBounds.STATE_COMMIT_KEYS;
        var attributes = Map.of("tapstate.pipeline.id", "orders", "tapstate.nest.namespace", "root");
        var buckets = new java.util.ArrayList<Long>(Collections.nCopies(instrument.buckets(), 0L));
        buckets.set(0, 1L);
        var histogram = instrument.value(1, 1.0, buckets);
        var at = Instant.parse("2026-11-10T00:00:01Z");
        var fact = new MetricFact(instrument.instrument(), MetricType.HISTOGRAM, instrument.unit(),
                List.of(MetricPoint.distribution(attributes, at.minusSeconds(1), at, histogram)));
        var response = PipelineMetricsResponse.of(new PipelineMetrics("orders", Map.of(), Map.of(), List.of(fact)));

        assertThat(response.facts()).singleElement().satisfies(metric -> {
            assertThat(metric.name()).isEqualTo("tapstate.pipeline.state.commit.keys");
            assertThat(metric.unit()).isEqualTo("{key}");
            assertThat(metric.points()).singleElement().satisfies(point -> {
                assertThat(point.attributes()).containsEntry("tapstate.nest.namespace", "root");
                assertThat(point.bucketCounts()).isEqualTo(buckets);
                assertThat(point.count()).isEqualTo(1L);
            });
        });
    }
}
