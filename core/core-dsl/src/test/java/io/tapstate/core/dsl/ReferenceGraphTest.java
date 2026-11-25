package io.tapstate.core.dsl;

import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cross-resource reference index that powers {@code desc}'s relationship view.
 * Where {@link ReferenceClosure} validates the fine-grained {@code from:} addressing inside a
 * pipeline, the graph records only the coarse edges between top-level ids: which sources / reusable
 * definitions a pipeline (or serve) names, and the transpose — who references a given resource. An
 * edge is recorded only when its target resolves inside the batch; a dangling name is a validate-layer
 * concern, surfaced there rather than as a phantom edge here.
 */
class ReferenceGraphTest {

    private final DslParser parser = new DslParser();

    /** Parses each YAML document into its resource model, in order. */
    private List<Resource> parse(String... docs) {
        return Arrays.stream(docs).map(parser::parse).toList();
    }

    @Test
    void typedDeclarationsKeepMissingReferencesWhileTheDescriptiveGraphStillOmitsThem() {
        var pipeline = new io.tapstate.core.model.PipelineResource("p",null,
                List.of(io.tapstate.core.model.SourceRef.spec("missing_source",true)),
                List.of(io.tapstate.core.model.Step.use("step","missing_transform",io.tapstate.core.model.FromClause.list(io.tapstate.core.model.FromRef.literal("orders")))),
                new io.tapstate.core.model.ViewBlock.Use(null,"missing_view",io.tapstate.core.model.FromRef.literal("step")),
                new io.tapstate.core.model.ServeBlock.Use(null,"missing_serve",io.tapstate.core.model.FromRef.literal("step")),null,null);
        assertThat(ReferenceGraph.declaredReferences(pipeline)).containsExactly(
                new ReferenceGraph.Edge("missing_serve","serve"),new ReferenceGraph.Edge("missing_source","source"),
                new ReferenceGraph.Edge("missing_transform","transform"),new ReferenceGraph.Edge("missing_view","view"));
        assertThat(ReferenceGraph.of(List.of(pipeline)).references("p")).isEmpty();
        var serve = new io.tapstate.core.model.ServeResource("serve",null,
                List.of(new io.tapstate.core.model.SyncElement("sink","missing_sink_source",null,null,null)),null,null,null);
        assertThat(ReferenceGraph.declaredReferences(serve)).containsExactly(new ReferenceGraph.Edge("missing_sink_source","source"));
    }

    // ---- forward edges: what a resource references --------------------------------------

    @Test
    @DisplayName("a pipeline references its source, its used transform, view and serve — each with its kind")
    void pipelineForwardEdgesNameSourceAndUsedDefinitions() {
        // mirrors corpus s11: pure-reference assembly over reusable definition bodies (X19)
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: src_crm
                connector: mysql
                mode: cdc
                tables: [ customers ]
                """,
                """
                version: tapstate/v1
                kind: transform
                id: mask_pii
                type: map
                fields: { ssn: false }
                """,
                """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: customer_id
                """,
                """
                version: tapstate/v1
                kind: serve
                id: std_api
                query: [ { type: rest } ]
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: crm_pack
                source: src_crm
                transforms:
                  - { type: filter, from: [customers], expr: "op != 'd'" }
                  - mask_pii
                view:  v_cust
                serve: std_api
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        // sorted by id, deduped; the inline filter step is intra-pipeline wiring, not a top-level edge
        assertThat(graph.references("crm_pack")).containsExactly(
                new ReferenceGraph.Edge("mask_pii", "transform"),
                new ReferenceGraph.Edge("src_crm", "source"),
                new ReferenceGraph.Edge("std_api", "serve"),
                new ReferenceGraph.Edge("v_cust", "view"));
    }

    @Test
    @DisplayName("a leaf resource (source / view) references nothing")
    void leafResourcesReferenceNothing() {
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mode: cdc
                """,
                """
                version: tapstate/v1
                kind: view
                id: v_a
                primary_key: id
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        assertThat(graph.references("src_a")).isEmpty();
        assertThat(graph.references("v_a")).isEmpty();
    }

    @Test
    @DisplayName("an inline serve's sync/push sources, and a standalone serve's, are forward edges")
    void serveSinkSourcesAreForwardEdges() {
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mode: cdc
                """,
                """
                version: tapstate/v1
                kind: source
                id: tgt_pg
                connector: postgres
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: tgt_pg
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        // p1 references both the read source and the sync's target-connection source
        assertThat(graph.references("p1")).containsExactly(
                new ReferenceGraph.Edge("src_a", "source"),
                new ReferenceGraph.Edge("tgt_pg", "source"));
    }

    @Test
    @DisplayName("a name that resolves to nothing in the batch is not recorded as an edge")
    void danglingReferenceIsNotAnEdge() {
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mode: cdc
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  use: ghost_view
                  from: /.*/
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        // ghost_view does not exist in the batch — no phantom edge; only the resolved src_a remains
        assertThat(graph.references("p1")).containsExactly(new ReferenceGraph.Edge("src_a", "source"));
    }

    // ---- reverse edges: who references a resource ---------------------------------------

    @Test
    @DisplayName("reverse edges name every resource that references the target")
    void reverseEdgesNameTheReferrers() {
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: src_crm
                connector: mysql
                mode: cdc
                tables: [ customers ]
                """,
                """
                version: tapstate/v1
                kind: transform
                id: mask_pii
                type: map
                fields: { ssn: false }
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: crm_pack
                source: src_crm
                transforms:
                  - { type: filter, from: [customers], expr: "op != 'd'" }
                  - mask_pii
                view:
                  id: v1
                  from: mask_pii
                  primary_key: customer_id
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        assertThat(graph.referencedBy("src_crm")).containsExactly(
                new ReferenceGraph.Edge("crm_pack", "pipeline"));
        assertThat(graph.referencedBy("mask_pii")).containsExactly(
                new ReferenceGraph.Edge("crm_pack", "pipeline"));
        // a referenced-by-nobody resource has an empty reverse list
        assertThat(graph.referencedBy("crm_pack")).isEmpty();
    }

    @Test
    @DisplayName("a source referenced twice by one pipeline (read + sync) yields a single deduped reverse edge")
    void duplicateEdgesAreDeduped() {
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mode: cdc
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: src_a
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        // src_a is both the read source and the sync target — one edge each way, not two
        assertThat(graph.references("p1")).containsExactly(new ReferenceGraph.Edge("src_a", "source"));
        assertThat(graph.referencedBy("src_a")).containsExactly(new ReferenceGraph.Edge("p1", "pipeline"));
    }

    @Test
    @DisplayName("a standalone serve referrer contributes a reverse edge carrying its own kind, not 'pipeline'")
    void reverseEdgeCarriesTheReferrersActualKind() {
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: tgt_pg
                connector: postgres
                """,
                """
                version: tapstate/v1
                kind: serve
                id: pub
                sync:
                  - id: sync_1
                    source: tgt_pg
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        // the referrer is a serve, so the reverse edge's kind must be 'serve' — never a hardcoded 'pipeline'
        assertThat(graph.referencedBy("tgt_pg")).containsExactly(new ReferenceGraph.Edge("pub", "serve"));
        assertThat(graph.references("pub")).containsExactly(new ReferenceGraph.Edge("tgt_pg", "source"));
    }

    @Test
    @DisplayName("on a duplicate id the graph describes the first-wins resource, matching id resolution")
    void duplicateIdUsesTheFirstWinsResourceForForwardEdges() {
        // two top-level resources share id 'dup' (a validate-layer error) — the source comes first
        List<Resource> batch = parse(
                """
                version: tapstate/v1
                kind: source
                id: dup
                connector: mysql
                mode: cdc
                """,
                """
                version: tapstate/v1
                kind: source
                id: realsrc
                connector: postgres
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: dup
                source: realsrc
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: realsrc
                """);

        ReferenceGraph graph = ReferenceGraph.of(batch);

        // the first 'dup' (a source) is canonical: it has no forward edges — the shadow pipeline's
        // realsrc edge must not bleed through, or the header (source) and references would disagree
        assertThat(graph.references("dup")).isEmpty();
        // and realsrc is not reported as referenced-by the shadow pipeline 'dup'
        assertThat(graph.referencedBy("realsrc")).isEmpty();
    }

    @Test
    @DisplayName("an unknown id has empty forward and reverse lists")
    void unknownIdHasEmptyEdges() {
        ReferenceGraph graph = ReferenceGraph.of(parse(
                """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                mode: cdc
                """));

        assertThat(graph.references("nope")).isEmpty();
        assertThat(graph.referencedBy("nope")).isEmpty();
    }
}
