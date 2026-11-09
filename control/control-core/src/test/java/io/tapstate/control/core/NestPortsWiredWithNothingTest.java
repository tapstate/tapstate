package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the nest explain ports do when an assembly wires nothing behind them: a sampler with no source reads
 * every table as not sampled and says why, and a tree reader with no compiler is a wiring defect that fails
 * loudly rather than answering with an empty tree a reader would take for a nest with no embeds.
 */
class NestPortsWiredWithNothingTest {

    @Test
    void aSamplerWithNoSourceReadsEveryTableAsNotSampled() {
        NestSampler.Sample sample = NestSampler.NONE.sample("shop", "orders", 200);

        assertThat(sample.rows()).isEmpty();
        assertThat(sample.tableRows()).isNull();
        assertThat(sample.unavailable()).isEqualTo("no source can be read from here");
    }

    @Test
    void aTreeReaderWithNoCompilerFailsAsAWiringDefect() {
        assertThatThrownBy(() -> NestTreeReader.NONE.tree("order_doc", "doc"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aWarningKeepsItsArgumentsAsGivenAndASampleCopiesItsRows() {
        NestExplanation.Warning warning = new NestExplanation.Warning(MonitorError.NEST_NEAR_ELEMENT_LIMIT, "items",
                Map.of("embed", "items", "count", 10L));
        NestSampler.Sample sample = new NestSampler.Sample(1L, null, null);

        assertThat(warning.args()).containsEntry("count", 10L);
        assertThat(sample.rows()).isEqualTo(List.of());
    }
}
