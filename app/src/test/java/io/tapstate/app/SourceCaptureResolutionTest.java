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

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void pipelineOverridesReachCaptureAndPreparationWithIndependentKeyedIdentities(boolean shared) {
        SourceResource source = cdcSource("orders_src", "orders", "shared-db");
        PipelineResource inherited = pipeline("pipeline-a", SourceRef.spec(source.id(), shared));
        PipelineResource changed = pipeline("pipeline-a", new SourceRef.Spec(source.id(), shared, Map.of("preImage", false)));
        SourceCaptureResolution before = SourceCaptureResolution.forPipeline(inherited, source, null).orElseThrow();
        SourceCaptureResolution after = SourceCaptureResolution.forPipeline(changed, source, null).orElseThrow();
        assertThat(after.config().settings()).containsEntry("preImage", false).containsEntry("host", "h");
        assertThat(source.config()).doesNotContainKey("preImage");
        assertThat(after.chainId()).isNotEqualTo(before.chainId());
        for (ReadMode mode : List.of(ReadMode.SNAPSHOT_ONLY, ReadMode.CDC_ONLY)) {
            var originalRun = new CaptureRunSpec(before.config(), mode, before.srsKey(), shared,
                    source.id(), inherited.id(), StartFrom.latest(), null, 0L);
            var localRun = new CaptureRunSpec(after.config(), mode, after.srsKey(), shared,
                    source.id(), changed.id(), StartFrom.latest(), null, 0L);
            assertThat(io.tapstate.runtime.srs.CaptureId.of(localRun)).isNotEqualTo(io.tapstate.runtime.srs.CaptureId.of(originalRun));
        }
        if (!shared) {
            var prepared = io.tapstate.spi.store.DirectChannelPreparation.of(
                    new io.tapstate.spi.store.PipelineIntentScope("cluster", 1L, "cache", changed.id(), 1L),
                    source, changed, ReadMode.CDC_ONLY, "latest", after.tables());
            assertThat(prepared.context().channelId()).isEqualTo(after.chainId().value());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void sameSourceBindingsKeepIndependentSettingsAndPreparedReaders(boolean shared) {
        var source = new SourceResource("crm", null, "mongodb", Map.of("host", "h", "preImage", true),
                SourceMode.CDC, List.of(TableRef.literal("orders"), TableRef.literal("items")),
                new Srs("same-database", null, null, null, null), null);
        var orders = new SourceRef.Spec("crm", shared, Map.of("preImage", true), "root_input");
        var items = new SourceRef.Spec("crm", shared, Map.of("preImage", false), "items_input");
        var view = new io.tapstate.core.model.ViewBlock.Inline("orders_view",
                io.tapstate.core.model.FromRef.literal("root_input.orders"), null, null);
        var step = io.tapstate.core.model.Step.inline("items",
                io.tapstate.core.model.FromClause.list(io.tapstate.core.model.FromRef.literal("items_input.items")),
                new io.tapstate.core.model.TransformBody.Filter("true"), Map.of());
        var pipeline = new PipelineResource("pipeline", null, List.of(orders, items), List.of(step), view,
                null, null, null);
        var root = SourceCaptureResolution.forPipeline(pipeline, orders, source, null).orElseThrow();
        var child = SourceCaptureResolution.forPipeline(pipeline, items, source, null).orElseThrow();
        assertThat(root.config().streams()).containsExactly("orders");
        assertThat(child.config().streams()).containsExactly("items");
        assertThat(root.config().settings()).containsEntry("preImage", true);
        assertThat(child.config().settings()).containsEntry("preImage", false);
        assertThat(root.chainId()).isNotEqualTo(child.chainId());
        var scope = new io.tapstate.spi.store.PipelineIntentScope("cluster", 1L, "cache", pipeline.id(), 1L);
        var inputs = io.tapstate.spi.store.PipelinePreparationInputs.of(scope, pipeline, Map.of("crm", source),
                Map.of(orders.binding(), java.util.Set.of("orders"), items.binding(), java.util.Set.of("items")));
        assertThat(inputs.sourceHashes()).containsOnlyKeys(orders.binding(), items.binding());
        assertThat(inputs.sourceHashes().values()).containsOnly(io.tapstate.core.model.canonical.CanonicalHash.of(source));
        if (!shared) assertThat(inputs.directChannels()).extracting(channel -> channel.context().channelId())
                .containsExactlyInAnyOrder(root.chainId().value(), child.chainId().value());
        var inherited = new SourceRef.Spec("crm", shared, Map.of(), "items_input");
        assertThat(io.tapstate.core.model.PipelineSourceConfig.settings(inherited, source)).containsEntry("preImage", true);
        assertThat(source.config()).containsEntry("preImage", true);
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
