package io.tapstate.app;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PipelineConnectorUsageTest {
    private static final String OLD = "a".repeat(64), NEXT = "b".repeat(64);

    @Test void versionUsagePagesAndNamesReadOneBindingSnapshotAndExcludeCandidates() {
        var intents = mock(PipelineIntentStore.class);
        var bindings = mock(ConnectorBindingStore.class);
        var drafts = mock(PipelineDraftStore.class);
        when(intents.connectorBindings()).thenReturn(bindings);
        var states = new LinkedHashMap<String, ConnectorRunBindings>();
        var names = new ArrayList<PipelineDraftSummary>();
        for (int n = 0; n < 11; n++) {
            String id = "reader_" + String.format("%02d", n);
            var scope = new PipelineIntentScope("cluster", 1, "cache", id, 1);
            var old = binding(scope, OLD, "1.0");
            var pending = new ConnectorRunBindings.Handoff(2, ConnectorRunBindings.Phase.PREPARED,
                    List.of(binding(scope, NEXT, "2.0")), List.of());
            states.put(id, new ConnectorRunBindings(scope, 1, List.of(old), pending));
            names.add(new PipelineDraftSummary(id, PipelineDraft.Mode.DAG, "Friendly " + n, null, 1,
                    null, null, null, Instant.EPOCH, Instant.EPOCH, "user"));
        }
        when(bindings.loadAll()).thenReturn(states);
        when(drafts.listSummaries()).thenReturn(names);
        var query = new PipelineConnectorUsage(intents, drafts, () -> {});
        var page = query.usages("mongodb", OLD, null, 2, 10);
        assertThat(page.total()).isEqualTo(11);
        assertThat(page.pipelines()).singleElement().satisfies(row -> {
            assertThat(row.pipelineId()).isEqualTo("reader_10");
            assertThat(row.name()).isEqualTo("Friendly 10");
            assertThat(row.connectors()).singleElement().satisfies(version -> {
                assertThat(version.version()).isEqualTo("1.0");
                assertThat(version.sha256()).isEqualTo(OLD);
                assertThat(version.chainId()).isEqualTo("chain-" + OLD);
            });
        });
        verify(bindings).loadAll();
        verify(bindings, never()).load(any());
        verify(drafts).listSummaries();
        assertThat(query.usages("mongodb", NEXT, null, 1, 10).total()).isZero();
        assertThat(query.usages("mongodb", OLD, "FRIENDLY 10", 1, 10).pipelines())
                .extracting(io.tapstate.control.core.ConnectorUsageQuery.Pipeline::pipelineId).containsExactly("reader_10");
    }

    @Test void monitoringReadsTheEffectiveExactHashAndLeavesUnknownLegacyBindingsEmpty() {
        var intents = mock(PipelineIntentStore.class);
        var bindings = mock(ConnectorBindingStore.class);
        when(intents.connectorBindings()).thenReturn(bindings);
        var scope = new PipelineIntentScope("cluster", 1, "cache", "reader", 1);
        var query = new PipelineConnectorUsage(intents, mock(PipelineDraftStore.class), () -> {});
        when(bindings.loadCurrent("reader")).thenReturn(Optional.empty());
        assertThat(query.forPipeline("reader")).isEmpty();
        when(bindings.loadCurrent("reader")).thenReturn(Optional.of(new ConnectorRunBindings(scope, 1,
                List.of(binding(scope, OLD, "1.0")), new ConnectorRunBindings.Handoff(2,
                ConnectorRunBindings.Phase.PREPARED, List.of(binding(scope, NEXT, "2.0")), List.of()))));
        assertThat(query.forPipeline("reader")).singleElement().satisfies(value -> {
            assertThat(value.sha256()).isEqualTo(OLD);
            assertThat(value.connectorId()).isEqualTo("mongodb");
        });
    }

    private static ConnectorRunBindings.Binding binding(PipelineIntentScope scope, String hash, String version) {
        String chain = "chain-" + hash;
        var notes = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.chain." + chain,
                RequiredNoteOwner.Kind.CHAIN, chain, 0), 1);
        return new ConnectorRunBindings.Binding("input", PipelineNode.Purpose.CAPTURE, "mongodb", version,
                hash, notes, "capture-" + hash, chain);
    }
}
