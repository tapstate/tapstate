package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The one place a pipeline-referenced source resolves into its capture/read ring identity. The reader that
 * builds the source vertex and the capture side that fills the ring must derive the identical ring from the
 * same source, so this pins the derivation: connector + settings + single table into a mining-chain id and
 * the per-table ring name, deterministically, with the explicit srs key overriding config-hash derivation.
 */
class SourceCaptureResolutionTest {

    @Test
    void derivesConnectorConfigAndSingleTableFromTheSource() {
        SourceResource source = cdcSource("orders_src", "orders", null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(source);

        assertThat(resolution.sourceId()).isEqualTo("orders_src");
        assertThat(resolution.table()).isEqualTo("orders");
        assertThat(resolution.config().connectorId()).isEqualTo("mysql");
        assertThat(resolution.config().settings()).containsEntry("host", "h");
        assertThat(resolution.config().streams()).containsExactly("orders");
        assertThat(resolution.srsKey()).isNull();
    }

    @Test
    void derivesTheRingIdentityFromTheConfigHashWhenNoSrsKeyIsSet() {
        SourceResource source = cdcSource("orders_src", "orders", null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(source);

        CaptureConfig config = new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders"));
        MiningChainId expected = MiningChainId.resolve(config, null);
        assertThat(resolution.chainId()).isEqualTo(expected);
        assertThat(resolution.ringName()).isEqualTo(SrsRingbuffer.ringName(expected.value(), "orders"));
    }

    @Test
    void anExplicitSrsKeyOverridesTheConfigHashDerivation() {
        SourceResource source = cdcSource("orders_src", "orders", "shared-key");

        SourceCaptureResolution resolution = SourceCaptureResolution.of(source);

        assertThat(resolution.srsKey()).isEqualTo("shared-key");
        assertThat(resolution.chainId()).isEqualTo(MiningChainId.ofKey("shared-key"));
    }

    @Test
    void twoResolutionsOfTheSameSourceDeriveTheIdenticalRingName() {
        SourceResource source = cdcSource("orders_src", "orders", null);

        // The load-bearing contract: the reader and the capture side each resolve the source independently and
        // must land on the same ring -- deriving one identity in one place is what guarantees it.
        assertThat(SourceCaptureResolution.of(source).ringName())
                .isEqualTo(SourceCaptureResolution.of(source).ringName());
    }

    @Test
    void expandsAnOmittedTableListToTheDiscoveryOrder() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(source, discovered("players", "cards"));

        assertThat(resolution.tables()).containsExactly("players", "cards");
        assertThat(resolution.config().streams()).containsExactly("players", "cards");
    }

    @Test
    void expandsRegexWithFullMatchAndKeepsSelectorOrder() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.regex("Player.*"), TableRef.literal("Orders")), null, null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                source, discovered("Player", "PlayerCard", "XPlayer", "Orders"));

        assertThat(resolution.tables()).containsExactly("Player", "PlayerCard", "Orders");
    }

    @Test
    void deDuplicatesOverlappingSelectorsOnFirstOccurrence() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("Player"), TableRef.regex("Player.*")), null, null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                source, discovered("Player", "PlayerCard"));

        assertThat(resolution.tables()).containsExactly("Player", "PlayerCard");
    }

    @Test
    void requiresDiscoveryForAnOmittedOrRegexTableList() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-schema-not-discovered");
    }

    @Test
    void rejectsAnOmittedTableListWhenDiscoveryContainsNoTables() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source, discovered()))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-table-selection-empty");
    }

    @Test
    void rejectsASelectorThatMatchesNoDiscoveredTable() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.regex("Missing.*")), null, null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source, discovered("orders")))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-table-selection-empty");
    }

    @Test
    void supportsMultipleConcreteStreams() {
        SourceResource multiTable = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders"), TableRef.literal("items")), null, null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(multiTable);

        assertThat(resolution.tables()).containsExactly("orders", "items");
        assertThat(resolution.table()).isEqualTo("orders");
    }

    private static SourceModel discovered(String... names) {
        return new SourceModel(List.of(names).stream()
                .map(name -> new SourceTable(name, List.of(), List.of(), List.of()))
                .toList());
    }

    private static SourceResource cdcSource(String id, String table, String srsKey) {
        Srs srs = srsKey == null ? null : new Srs(srsKey, null, null, null, null);
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, srs, null);
    }
}
