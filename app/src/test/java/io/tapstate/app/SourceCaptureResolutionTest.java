package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.StartFrom;
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
                SourceMode.CDC, null, null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(source, discovered("players", "cards"));

        assertThat(resolution.tables()).containsExactly("players", "cards");
        assertThat(resolution.config().streams()).containsExactly("players", "cards");
    }

    @Test
    void expandsRegexWithFullMatchAndKeepsSelectorOrder() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.regex("Player.*"), TableRef.literal("Orders")), null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                source, discovered("Player", "PlayerCard", "XPlayer", "Orders"));

        assertThat(resolution.tables()).containsExactly("Player", "PlayerCard", "Orders");
    }

    @Test
    void deDuplicatesOverlappingSelectorsOnFirstOccurrence() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("Player"), TableRef.regex("Player.*")), null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                source, discovered("Player", "PlayerCard"));

        assertThat(resolution.tables()).containsExactly("Player", "PlayerCard");
    }

    @Test
    void requiresDiscoveryForAnOmittedOrRegexTableList() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-schema-not-discovered");
    }

    @Test
    void rejectsAnOmittedTableListWhenDiscoveryContainsNoTables() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source, discovered()))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-table-selection-empty");
    }

    @Test
    void rejectsASelectorThatMatchesNoDiscoveredTable() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.regex("Missing.*")), null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source, discovered("orders")))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-table-selection-empty");
    }

    @Test
    void supportsMultipleConcreteStreams() {
        SourceResource multiTable = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders"), TableRef.literal("items")), null, null);

        SourceCaptureResolution resolution = SourceCaptureResolution.of(multiTable);

        assertThat(resolution.tables()).containsExactly("orders", "items");
        assertThat(resolution.table()).isEqualTo("orders");
    }

    @Test
    void rejectsUnsupportedTableSpecSettingsBeforeCapture() {
        SourceResource source = new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC,
                List.of(TableRef.spec("orders", "amount > 0", List.of("id"))), null, null);

        assertThatThrownBy(() -> SourceCaptureResolution.of(source))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-table-spec-unsupported");
    }

    @Test
    void aDirectReferenceResolvesTheSameRecoveryRecordAsItsCaptureRun() {
        SourceResource source = cdcSource("orders_src", "orders", "shared-db");
        PipelineResource pipeline = pipeline("pipeline-a", SourceRef.spec(source.id(), false));
        SourceCaptureResolution resolution = SourceCaptureResolution.forPipeline(pipeline, source, null)
                .orElseThrow();
        CaptureRunSpec run = new CaptureRunSpec(resolution.config(), ReadMode.CDC_ONLY,
                resolution.srsKey(), false, source.id(), pipeline.id(), StartFrom.latest(), null, 0L);

        assertThat(resolution.chainId()).isEqualTo(run.miningChainId());
        assertThat(resolution.chainId()).isNotEqualTo(MiningChainId.ofKey("shared-db"));
    }

    @Test
    void twoDirectPipelinesOnOneDatabaseResolveIndependentRecoveryRecords() {
        SourceResource source = cdcSource("orders_src", "orders", null);
        SourceCaptureResolution first = SourceCaptureResolution.forPipeline(
                pipeline("pipeline-a", SourceRef.spec(source.id(), false)), source, null).orElseThrow();
        SourceCaptureResolution second = SourceCaptureResolution.forPipeline(
                pipeline("pipeline-b", SourceRef.spec(source.id(), false)), source, null).orElseThrow();

        assertThat(first.chainId()).isNotEqualTo(second.chainId());
        assertThat(first.chainId()).isNotEqualTo(SourceCaptureResolution.of(source).chainId());
    }

    @Test
    void twoDirectSourceNodesInOnePipelineResolveIndependentRecoveryRecords() {
        SourceResource root = cdcSource("root_src", "orders", null);
        SourceResource mail = cdcSource("mail_src", "emailmessage", null);
        PipelineResource pipeline = pipeline("pipeline",
                SourceRef.spec(root.id(), false), SourceRef.spec(mail.id(), false));

        assertThat(SourceCaptureResolution.forPipeline(pipeline, root, null).orElseThrow().chainId())
                .isNotEqualTo(SourceCaptureResolution.forPipeline(pipeline, mail, null).orElseThrow().chainId());
    }

    @Test
    void sharedSourceNodesOnOneDatabaseResolveTheSamePhysicalCapture() {
        SourceResource root = cdcSource("root_src", "orders", null);
        SourceResource mail = cdcSource("mail_src", "emailmessage", null);
        PipelineResource pipeline = pipeline("pipeline",
                SourceRef.spec(root.id(), true), SourceRef.spec(mail.id(), true));

        assertThat(SourceCaptureResolution.forPipeline(pipeline, root, null).orElseThrow().chainId())
                .isEqualTo(SourceCaptureResolution.forPipeline(pipeline, mail, null).orElseThrow().chainId());
    }

    @Test
    void scopingAResolutionAlwaysStartsFromThePhysicalSourceIdentity() {
        SourceCaptureResolution physical = SourceCaptureResolution.of(cdcSource("orders_src", "orders", null));

        assertThat(physical.scopedTo("pipeline-a", false).scopedTo("pipeline-a", false).chainId())
                .isEqualTo(physical.scopedTo("pipeline-a", false).chainId());
        assertThat(physical.scopedTo("pipeline-a", false).scopedTo("pipeline-b", true).chainId())
                .isEqualTo(physical.chainId());
    }

    private static PipelineResource pipeline(String id, SourceRef... refs) {
        return new PipelineResource(id, null, List.of(refs), null, null, null, null, null);
    }

    private static SourceModel discovered(String... names) {
        return new SourceModel(List.of(names).stream()
                .map(name -> new SourceTable(name, List.of(), List.of(), List.of()))
                .toList());
    }

    private static SourceResource cdcSource(String id, String table, String srsKey) {
        Srs srs = srsKey == null ? null : new Srs(srsKey, null, null, null, null);
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), srs, null);
    }
}
