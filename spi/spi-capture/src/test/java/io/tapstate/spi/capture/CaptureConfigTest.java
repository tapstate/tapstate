package io.tapstate.spi.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.model.PipelineNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CaptureConfigTest {

    @Test
    void connectorIdIsRequired() {
        assertThatNullPointerException()
                .isThrownBy(() -> new CaptureConfig(null, Map.of(), List.of()));
    }

    @Test
    void settingsAndStreamsDefaultToEmptyWhenNull() {
        CaptureConfig config = new CaptureConfig("mysql", null, null);

        assertThat(config.settings()).isEmpty();
        assertThat(config.streams()).isEmpty();
    }

    @Test
    void settingsAreADefensiveCopy() {
        Map<String, Object> source = new HashMap<>();
        source.put("host", "localhost");
        CaptureConfig config = new CaptureConfig("mysql", source, List.of());

        source.put("host", "elsewhere");

        assertThat(config.settings()).containsEntry("host", "localhost");
    }

    @Test
    void settingsAreUnmodifiable() {
        CaptureConfig config = new CaptureConfig("mysql", Map.of("host", "localhost"), List.of());

        assertThatThrownBy(() -> config.settings().put("port", 3306))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * The three-argument form is what the read-only drives build, and it has to say "no node" rather
     * than invent one. A node they made up would name a namespace nothing ever reads back, and it
     * would read from outside exactly like a real one.
     */
    @Test
    void aConfigBuiltWithoutANodeNamesNone() {
        assertThat(new CaptureConfig("mysql", Map.of(), List.of()).node()).isNull();
    }

    @Test
    void aConfigCarriesTheNodeItIsReadFor() {
        PipelineNode node = new PipelineNode("p1", "src_a");

        CaptureConfig config = new CaptureConfig("mysql", Map.of(), List.of("orders"), node);

        assertThat(config.node()).isEqualTo(node);
    }

    /**
     * Attaching a node changes nothing else. The identity is added at the one point that knows it -
     * where a pipeline starts a source - onto a config resolved well before that, so a copy that also
     * altered the connector, the settings or the stream selection would silently read something other
     * than what was resolved.
     */
    @Test
    void attachingANodeLeavesTheRestOfTheConfigAlone() {
        CaptureConfig resolved = new CaptureConfig("mysql", Map.of("host", "localhost"), List.of("orders"));

        CaptureConfig at = resolved.at(new PipelineNode("p1", "src_a"));

        assertThat(at.connectorId()).isEqualTo("mysql");
        assertThat(at.settings()).containsExactlyEntriesOf(Map.of("host", "localhost"));
        assertThat(at.streams()).containsExactly("orders");
        assertThat(at.node()).isEqualTo(new PipelineNode("p1", "src_a"));
        assertThat(resolved.node()).as("the config attached to is left as it was").isNull();
    }

    @Test
    void streamsAreADefensiveCopyAndUnmodifiable() {
        List<String> source = new ArrayList<>(List.of("orders"));
        CaptureConfig config = new CaptureConfig("mysql", Map.of(), source);

        source.add("customers");

        assertThat(config.streams()).containsExactly("orders");
        assertThatThrownBy(() -> config.streams().add("customers"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
