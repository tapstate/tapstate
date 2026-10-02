package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineDraftTest {

    private static final Instant NOW = Instant.parse("2026-09-21T00:00:00Z");

    @Test
    void draftEnforcesRevisionModeAndPublicationInvariants() {
        assertThatThrownBy(() -> new PipelineDraft(" ", 1, 1, PipelineDraft.Mode.DAG,
                "name", null, graph(), null, null, null, null, NOW, NOW, "author"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("pipelineId must not be blank");
        assertThatThrownBy(() -> draft(PipelineDraft.Mode.DAG, null, wizard()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dag drafts require graph and no wizard payload");
        assertThatThrownBy(() -> draft(PipelineDraft.Mode.WIZARD, graph(), wizard()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("wizard drafts require wizard and no graph payload");
        assertThatThrownBy(() -> new PipelineDraft("orders", 2, 1, PipelineDraft.Mode.DAG,
                "name", null, graph(), null, null, null, null, NOW, NOW, "author"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("unsupported draft schema version: 2");
        assertThatThrownBy(() -> new PipelineDraft("orders", 1, 0, PipelineDraft.Mode.DAG,
                "name", null, graph(), null, null, null, null, NOW, NOW, "author"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("revision must be positive");
        assertThatThrownBy(() -> new PipelineDraft("orders", 1, 1, PipelineDraft.Mode.DAG,
                "name", null, graph(), null, null, 2L, null, NOW, NOW, "author"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("published revision cannot exceed draft revision");
    }

    @Test
    void graphNodesEdgesAndViewportValidateAndDefensivelyCopyInput() {
        Map<String, Object> config = new HashMap<>();
        config.put("table", "orders");
        PipelineDraft.Node node = new PipelineDraft.Node("source", "source", "mysql", "orders", config, null);
        config.put("table", "mutated");

        assertThat(node.config()).containsEntry("table", "orders");
        assertThat(node.metadata()).isEmpty();
        assertThatThrownBy(() -> node.config().put("new", true)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(new PipelineDraft.Graph(new ArrayList<>(List.of(node)), List.of(), new PipelineDraft.Viewport(1, 2, 1))
                .nodes()).containsExactly(node);
        assertThatThrownBy(() -> new PipelineDraft.Node(" ", "source", null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("node id must not be blank");
        assertThatThrownBy(() -> new PipelineDraft.Edge("edge", " ", "target"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("edge source must not be blank");
        assertThatThrownBy(() -> new PipelineDraft.Viewport(Double.NaN, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("viewport must be finite with a positive zoom");
        assertThatThrownBy(() -> new PipelineDraft.Viewport(0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("viewport must be finite with a positive zoom");
    }

    @Test
    void wizardRelationsAndTransformsEnforceTheirShapeContracts() {
        PipelineDraft.Root root = new PipelineDraft.Root("root", "mysql", "orders", null, null);
        assertThat(root.key()).isEmpty();
        assertThat(root.preTransforms()).isEmpty();
        assertThat(new PipelineDraft.Wizard(root, new ArrayList<>(), List.of(), null).related()).isEmpty();

        assertThatThrownBy(() -> new PipelineDraft.Relation(List.of(), PipelineDraft.Shape.OBJECT,
                "profile", List.of(), List.of("id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("arrayKey is only valid for array relations");
        assertThatThrownBy(() -> new PipelineDraft.Relation(List.of(), PipelineDraft.Shape.FLAT,
                "profile", List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("flat relations do not have a path");
        assertThatThrownBy(() -> new PipelineDraft.Related("child", "root", "mysql", "items", null, null))
                .isInstanceOf(NullPointerException.class).hasMessage("relation");
        assertThatThrownBy(() -> new PipelineDraft.FieldPair(null, "id"))
                .isInstanceOf(NullPointerException.class).hasMessage("child field");
        assertThatThrownBy(() -> new PipelineDraft.Transform("step", " ", Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("transform type must not be blank");
        assertThat(new PipelineDraft.Transform("step", "map", null).fields()).isEmpty();
        assertThat(new PipelineDraft.Output("atlas", null).config()).isEmpty();
    }

    @Test
    void publicationMustTargetItsPipelineAndCarryAnActorAndHash() {
        Resource other = new PipelineResource("other", null, List.of(SourceRef.bare("mysql")),
                List.of(), null, null, null, Map.of());

        assertThatThrownBy(() -> new PipelineDraft.Publication("orders", 1, null, other,
                "hash", NOW, "author"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("publication artifact id must equal pipeline id");
        assertThatThrownBy(() -> new PipelineDraft.Publication("orders", 0, null, other,
                "hash", NOW, "author"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("expected draft revision must be positive");
    }

    private static PipelineDraft draft(PipelineDraft.Mode mode, PipelineDraft.Graph graph,
            PipelineDraft.Wizard wizard) {
        return new PipelineDraft("orders", 1, 1, mode, "Orders", null, graph, wizard,
                null, null, null, NOW, NOW, "author");
    }

    private static PipelineDraft.Graph graph() {
        return new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1));
    }

    private static PipelineDraft.Wizard wizard() {
        return new PipelineDraft.Wizard(new PipelineDraft.Root("root", "mysql", "orders", List.of(), List.of()),
                List.of(), List.of(), null);
    }
}
