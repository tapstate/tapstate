package io.tapstate.runtime.srs;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A run spec is a pipeline's read of one of its sources, and the connector doing that read files notes
 * it has to find again on a later drive. Which node those notes belong to is the pair this spec already
 * names — the pipeline and the source — so the spec scopes its own config rather than being handed a
 * config somebody else scoped.
 *
 * <p>What that removes is a second derivation of one pair. Carried in on the config, the identity was
 * worked out once by whoever built the spec and once again as the two ids beside it, and nothing held
 * the two together: a caller that stopped scoping the config, or scoped it to a different node, would
 * leave the connector filing under a name no later drive looks at. Every other assertion stays green
 * through that — the rows all arrive, the chain is still mined once — and the loss shows up as a
 * connector that reads its own notes as an empty notepad, which is indistinguishable from a first run.
 */
class CaptureRunSpecTest {

    private static final CaptureConfig UNSCOPED =
            new CaptureConfig("mysql", Map.of("host", "db1"), List.of("orders"));

    private static CaptureRunSpec spec(CaptureConfig config) {
        return spec(config, "p1", "src_a");
    }

    private static CaptureRunSpec spec(CaptureConfig config, String pipelineId, String sourceId) {
        return new CaptureRunSpec(
                config, ReadMode.CDC_ONLY, null, true, sourceId, pipelineId, StartFrom.earliest(),
                new SourcePosition("cdc-start-0"), null, 0L, () -> new SourcePosition("w1"));
    }

    @Test
    void aRunScopesItsConfigToTheNodeItIsRunFor() {
        assertThat(spec(UNSCOPED).config().node()).isEqualTo(new PipelineNode("p1", "src_a"));
    }

    /**
     * The other half of the pair. Without it the case above passes over a spec that merely accepted
     * whatever node it was handed, which is the arrangement that let the two derivations drift.
     */
    @Test
    void aConfigScopedToSomebodyElsesNodeIsScopedBackToThisRuns() {
        CaptureConfig elsewhere = UNSCOPED.at(new PipelineNode("p2", "src_b"));

        assertThat(spec(elsewhere).config().node()).isEqualTo(new PipelineNode("p1", "src_a"));
    }

    /**
     * The scope is read off each run rather than being one name every run gets. Two pipelines reading
     * one source through one set of settings are the arrangement this separates: they are meant to mine
     * the log once between them and to keep what their connectors record apart, so a scope that did not
     * vary with the run would hand the second pipeline the first one's notes - and a replication slot
     * admits one live consumer, so both would be holding the same one.
     */
    @Test
    void twoPipelinesReadingOneSourceScopeApart() {
        CaptureRunSpec one = spec(UNSCOPED, "p1", "src_a");
        CaptureRunSpec other = spec(UNSCOPED, "p2", "src_b");

        assertThat(one.config().node()).isEqualTo(new PipelineNode("p1", "src_a"));
        assertThat(other.config().node()).isEqualTo(new PipelineNode("p2", "src_b"));
    }

    /**
     * Scoping changes the node and nothing else. A copy that also altered the connector, the settings
     * or the stream selection would read something other than what was resolved for this run.
     */
    @Test
    void scopingLeavesTheRestOfTheConfigAlone() {
        CaptureConfig scoped = spec(UNSCOPED).config();

        assertThat(scoped.connectorId()).isEqualTo("mysql");
        assertThat(scoped.settings()).containsExactlyEntriesOf(Map.of("host", "db1"));
        assertThat(scoped.streams()).containsExactly("orders");
    }

    /**
     * The chain a shared mining run is keyed by stays what it was. That identity is the physical source
     * coordinate, and two pipelines reading one database through one set of settings are meant to land
     * on one chain — so a scope folded into it would mine the same log once per reader, with every ring
     * still well-formed and nothing above saying a word.
     */
    @Test
    void scopingDoesNotMoveTheChainTheRunMinesFrom() {
        assertThat(MiningChainId.of(spec(UNSCOPED).config())).isEqualTo(MiningChainId.of(UNSCOPED));
    }
}
