package io.tapstate.archtests;

import io.tapstate.control.core.PipelineMetrics;
import io.tapstate.control.restapi.PipelineMetricsResponse;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.core.util.DefaultIndenter;
import tools.jackson.core.util.DefaultPrettyPrinter;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The metrics face is what a dashboard, a script or a command line reads, and none of them is rebuilt
 * when this product is. Its shape on the wire is therefore a contract: a field that moves, changes its
 * name or nests differently is a change every one of those readers has to be told about, and the build
 * is the only place that can tell anybody.
 *
 * <p>What this guards against is the change nothing else sees. A field moved from inside one object to
 * beside it compiles, keeps every test that reads the new position green, and breaks every reader that
 * knew the old one — that exact change happened once to this face, and no test noticed because no test
 * held the bytes. This one holds the bytes: a fixed set of facts, rendered as the face renders them, is
 * compared line for line with the checked-in golden.
 *
 * <p><strong>The rendering is the record's, not a codec's.</strong> The wire record carries only strings,
 * numbers, lists and objects, formats its own instants and its own type words, and sorts its own maps, so
 * what a JSON codec adds is punctuation. That is what lets a plain mapper here stand for the server's:
 * nothing in the bytes depends on how the server's mapper is configured, and a golden written against
 * them is a golden of the wire.
 *
 * <p>So the rule is not "this file must never change". It is that changing it is a decision somebody made
 * on purpose: a reader-facing shape that moved without a line of this golden moving is the one failure in
 * this area that nothing else in the build notices.
 */
class MetricsFaceWireGateTest {

    private static final Path GOLDEN = Path.of("src", "test", "resources", "metrics-face-wire.golden");

    private static final Instant COUNTING_SINCE = Instant.parse("2026-09-17T09:00:00Z");
    private static final Instant TAKEN = Instant.parse("2026-09-17T10:00:00.250Z");
    private static final String PIPELINE = "orders_sync";

    @Test
    @DisplayName("the metrics face renders the fixed facts exactly as the golden records them")
    void theMetricsFaceMatchesTheGolden() throws IOException {
        List<String> rendered = Arrays.asList(render(fixture()).split("\n"));

        assertThat(rendered)
                .as("the metrics face's wire shape changed; if that was the intent, update %s and say so in "
                        + "the release note, since every reader of this face is outside this build", GOLDEN)
                .containsExactlyElementsOf(golden());
    }

    @Test
    @DisplayName("positive control: the fixture exercises every kind of point the face can carry")
    void theFixtureCarriesEveryKindOfPoint() {
        String rendered = render(fixture());

        // A golden over a fixture that lacks a kind of point would pass over a change to how that kind
        // renders. Each of these is a field only one kind carries.
        assertThat(rendered).contains("\"startTime\"").contains("\"bucketCounts\"").contains("\"value\"");
        assertThat(rendered).contains("\"type\" : \"counter\"").contains("\"type\" : \"gauge\"")
                .contains("\"type\" : \"histogram\"");
        assertThat(rendered).contains("\"otel.metric.overflow\"");
    }

    /**
     * One pipeline's metrics with one of everything the face carries: the flat map with a squeezed family
     * and a bare key, an acked position, a counter broken down by table, direction and operation, a gauge
     * with no attributes, a gauge per table, a counter per code, a distribution over the registered bounds,
     * and the overflow series a cardinality budget folds the rest into.
     */
    private static PipelineMetrics fixture() {
        Map<String, String> orders = Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                MetricAttributes.TABLE_ID, "orders");
        MetricFact records = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", List.of(
                MetricPoint.accumulated(with(orders, MetricAttributes.DIRECTION, "in", MetricAttributes.OP, "insert"),
                        COUNTING_SINCE, TAKEN, 100L),
                MetricPoint.accumulated(with(orders, MetricAttributes.DIRECTION, "in", MetricAttributes.OP, "read"),
                        COUNTING_SINCE, TAKEN, 90_000L),
                MetricPoint.accumulated(with(orders, MetricAttributes.DIRECTION, "out", MetricAttributes.OP, "insert"),
                        COUNTING_SINCE, TAKEN, 98L),
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.DIRECTION,
                        "in", MetricAttributes.OP, "update", MetricAttributes.OVERFLOW, "true"),
                        COUNTING_SINCE, TAKEN, 12L)));
        MetricFact count = MetricFact.single("recordCount", MetricType.GAUGE, "{record}",
                MetricPoint.reading(Map.of(), TAKEN, 98L));
        MetricFact lag = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(orders, TAKEN, 4L));
        MetricFact errors = MetricFact.single("tapstate.pipeline.errors", MetricType.COUNTER, "{error}",
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.CODE,
                        "sink.write-rejected"), COUNTING_SINCE, TAKEN, 1L));
        HistogramBounds bounds = HistogramBounds.RECORD_DELIVERY_DURATION;
        List<Long> buckets = new ArrayList<>();
        for (int i = 0; i < bounds.buckets(); i++) {
            buckets.add(i == 3 ? 7L : 0L);
        }
        MetricFact delivery = MetricFact.single(bounds.instrument(), MetricType.HISTOGRAM, HistogramBounds.UNIT,
                MetricPoint.distribution(orders, COUNTING_SINCE, TAKEN, bounds.value(7, 0.63, buckets)));
        return new PipelineMetrics(PIPELINE,
                Map.of("recordCount", 98L, "records.in", 90_112L, "records.out", 98L, "lag.orders", 4L,
                        "errors.sink.write-rejected", 1L),
                Map.of("orders", "w7"),
                List.of(records, count, lag, errors, delivery));
    }

    private static Map<String, String> with(Map<String, String> base, String... pairs) {
        Map<String, String> out = new java.util.HashMap<>(base);
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(pairs[i], pairs[i + 1]);
        }
        return out;
    }

    /**
     * Pretty-printed with a fixed indent and a fixed line feed, so the golden reads as lines on every
     * platform; the server sends the same bytes without the whitespace.
     */
    private static String render(PipelineMetrics metrics) {
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter()
                .withObjectIndenter(new DefaultIndenter("  ", "\n"))
                .withArrayIndenter(new DefaultIndenter("  ", "\n"));
        return JsonMapper.builder().build().writer().with(printer)
                .writeValueAsString(PipelineMetricsResponse.of(metrics));
    }

    /** The recorded shape: comment and blank lines are prose, everything else is a line of the wire. */
    private static List<String> golden() throws IOException {
        return Files.readAllLines(GOLDEN).stream()
                .filter(line -> !line.isBlank() && !line.startsWith("#"))
                .toList();
    }
}
