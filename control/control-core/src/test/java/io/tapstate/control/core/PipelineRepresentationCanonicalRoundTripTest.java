package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineRepresentationCanonicalRoundTripTest {

    @Test
    void anonymousAndNamedSyncOutputsKeepCanonicalIdentityOnReadback() {
        PipelineInput input = new PipelineInput(
                "orders_sync", null, List.of("mysql_orders"), null, null,
                Map.of("from", "orders", "sync", List.of(
                        Map.of("source", "mongodb_target"),
                        Map.of("id", "warehouse", "source", "archive_target"),
                        Map.of("source", "audit_target"))),
                null, null);

        PipelineResource model = new PipelineRepresentation().toModel(input, null);
        String storedHash = CanonicalHash.of(model);
        PipelineResource readback = (PipelineResource) new DslParser()
                .fromTree(new CanonicalWriter().tree(model));
        List<String> authoredIds = ((ServeBlock.Inline) model.serve()).sync().stream()
                .map(SyncElement::id).toList();
        List<String> readbackIds = ((ServeBlock.Inline) readback.serve()).sync().stream()
                .map(SyncElement::id).toList();

        assertThat(readbackIds).containsExactly("sync_1", "warehouse", "sync_3");
        assertThat(authoredIds.get(1)).isEqualTo("warehouse");
        assertThat(CanonicalHash.of(readback))
                .as("canonical readback must preserve the stored hash; sync ids before=%s, after=%s",
                        authoredIds, readbackIds)
                .isEqualTo(storedHash);
        assertThat(readback).isEqualTo(model);
    }
}
