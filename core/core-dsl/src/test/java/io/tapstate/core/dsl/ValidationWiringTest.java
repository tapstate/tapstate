package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValidationWiringTest {
    private static final Wiring.InspectionLimits LIMITS = new Wiring.InspectionLimits(128, 1024, 128, 32, 65536);

    @Test
    void selectedBranchesPreserveRowOrderWithoutReadingAnUnrelatedMissingSourceOrReusableBody() {
        PipelineResource pipeline = pipeline("""
                transforms:
                  - { id: base, from: [src_orders.orders], type: filter, expr: "op == 'i'" }
                  - { id: numbers, from: [base], type: map, fields: { amount: 1 } }
                  - { id: unchanged, from: [base], type: filter, expr: "op == 'i'" }
                  - { id: merge, from: [numbers, unchanged], type: union }
                  - { id: unrelated, from: [src_archive.other], use: absent_transform }
                view: { id: orders_view, from: merge, primary_key: id }
                """);
        var result = inspect(pipeline, Map.of("src_orders", source("src_orders", "[orders]")), "orders_view");
        assertThat(result.declarationComplete()).isTrue();
        assertThat(result.routes()).hasSize(2).extracting(Wiring.InspectedRoute::upstream)
                .containsExactly(new Upstream("src_orders", "orders"), new Upstream("src_orders", "orders"));
        assertThat(result.routes().get(0).steps()).extracting(Step::id).containsExactly("base", "numbers", "merge");
        assertThat(result.routes().get(1).steps()).extracting(Step::id).containsExactly("base", "unchanged", "merge");
        assertThat(result.nodes()).containsExactlyInAnyOrder("orders_view", "merge", "numbers", "unchanged", "base");
        assertThat(result.sourceDeclarations()).containsExactly("src_orders");
        assertThat(result.routes().getFirst().selector()).isEqualTo(FromRef.literal("src_orders.orders"));
    }

    @Test
    void sourceIdentifiersSelectTheirConfiguredRangeRatherThanAnInventedTable() {
        var result = inspect(pipeline(""), sources("[orders]", "[archive]"), "src_orders");
        assertThat(result.routes()).extracting(Wiring.InspectedRoute::upstream)
                .containsExactly(new Upstream("src_orders", null));
        assertThat(result.routes().getFirst().selector()).isEqualTo(FromRef.literal("src_orders"));
    }

    @Test
    void bareTableAttributionPinsNegativeEvidenceFromEveryClosedSource() {
        var result = inspect(pipeline(""), sources("[orders]", "[archive]"), "orders");
        assertThat(result.declarationComplete()).isTrue();
        assertThat(result.routes()).extracting(Wiring.InspectedRoute::upstream)
                .containsExactly(new Upstream("src_orders", "orders"));
        assertThat(result.sourceDeclarations()).containsExactlyInAnyOrder("src_orders", "src_archive");
    }

    @Test
    void ambiguousAndMissingClosedTablesDoNotBecomeAllSourceRoutes() {
        assertThatThrownBy(() -> inspect(pipeline(""), sources("[orders]", "[orders]"), "orders"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.ambiguous-reference");
        for (String missing : List.of("missing", "other.orders", "src_orders.missing", "src_orders.")) {
            assertThatThrownBy(() -> inspect(pipeline(""), sources("[orders]", "[archive]"), missing))
                    .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference");
        }
    }

    @Test
    void openSourcesKeepBareAttributionPendingButPermitExplicitQualifiedIntent() {
        var resources = sources("[orders]", null);
        var pending = inspect(pipeline(""), resources, "orders");
        assertThat(pending.declarationComplete()).isFalse();
        assertThat(pending.routes()).isEmpty();
        assertThat(pending.pending()).extracting(Wiring.PendingReference::reference).containsExactly(FromRef.literal("orders"));
        assertThat(pending.sourceDeclarations()).containsExactlyInAnyOrder("src_orders", "src_archive");
        var qualified = inspect(pipeline(""), resources, "src_archive.orders");
        assertThat(qualified.declarationComplete()).isTrue();
        assertThat(qualified.routes()).extracting(Wiring.InspectedRoute::upstream)
                .containsExactly(new Upstream("src_archive", "orders"));
    }

    @Test
    void regexIsPreservedWithoutExecutionOrInventingASourceOnlyClosure() {
        PipelineResource pipeline = pipeline("""
                transforms:
                  - { id: selected, from: [/step_.*/], type: map, fields: { amount: 1 } }
                  - { id: step_one, from: [src_orders.orders], use: pending_body }
                """);
        var result = inspect(pipeline, Map.of(), "selected");
        assertThat(result.declarationComplete()).isFalse();
        assertThat(result.routes()).isEmpty();
        assertThat(result.sourceDeclarations()).isEmpty();
        assertThat(result.pending()).hasSize(1);
        assertThat(result.pending().getFirst().reference()).isEqualTo(FromRef.regex("step_.*"));
        assertThat(result.pending().getFirst().downstreamSteps()).extracting(Step::id).containsExactly("selected");
    }

    @Test
    void selectedReusableStepKeepsItsOriginalDeclarationForLaterBodyExpansion() {
        var result = inspect(pipeline("""
                transforms:
                  - { id: selected, from: [src_orders.orders], use: reusable }
                """), sources("[orders]", "[archive]"), "selected");
        assertThat(result.routes().getFirst().steps()).singleElement().isInstanceOf(Step.Use.class);
        assertThat(((Step.Use) result.routes().getFirst().steps().getFirst()).use()).isEqualTo("reusable");
    }

    @Test
    void selectedSourceMustHaveTheRightDeclarationKind() {
        assertThatThrownBy(() -> inspect(pipeline(""), Map.of(), "src_orders.orders"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference");
        assertThatThrownBy(() -> inspect(pipeline(""), Map.of("src_orders", pipeline("")), "src_orders.orders"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference");
    }

    @Test
    void cyclesFailWhileSharedAcyclicBranchesRemainDistinct() {
        PipelineResource cyclic = pipeline("""
                transforms:
                  - { id: first, from: [second], type: filter, expr: "op == 'i'" }
                  - { id: second, from: [first], type: union }
                """);
        assertThatThrownBy(() -> inspect(cyclic, Map.of(), "first"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.rule-unsupported");
    }

    @Test
    void everyIndependentInspectionDimensionIsBounded() {
        PipelineResource chain = pipeline("""
                transforms:
                  - { id: first, from: [src_orders.orders], type: union }
                  - { id: second, from: [first], type: union }
                """);
        var resources = sources("[orders]", "[archive]");
        assertLimit(chain, resources, FromClause.list(FromRef.literal("second")), new Wiring.InspectionLimits(3, 1024, 128, 32, 65536), "nodes");
        assertLimit(chain, resources, FromClause.list(FromRef.literal("second")), new Wiring.InspectionLimits(128, 1, 128, 32, 65536), "references");
        assertLimit(chain, resources, FromClause.list(FromRef.literal("second")), new Wiring.InspectionLimits(128, 1024, 128, 1, 65536), "depth");
        assertLimit(chain, resources, FromClause.list(FromRef.literal("second")), new Wiring.InspectionLimits(128, 1024, 128, 32, 512), "bytes");
        FromClause two = new FromClause.Flow(List.of(FromRef.literal("src_orders.orders"), FromRef.literal("src_orders.orders")));
        assertLimit(chain, resources, two, new Wiring.InspectionLimits(128, 1024, 1, 32, 65536), "routes");
    }

    @Test
    void twoClosedSuppliersAlreadyProveAmbiguityEvenWhenAnotherSourceIsOpen() {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: mixed
                source: [src_orders, src_archive, src_open]
                """);
        var resources = Map.of("src_orders", source("src_orders", "[orders]"),
                "src_archive", source("src_archive", "[orders]"), "src_open", source("src_open", null));
        assertThatThrownBy(() -> inspect(pipeline, resources, "orders"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.ambiguous-reference");
    }

    @Test
    void internalIndexOfUnselectedSourcesConsumesMetadataBudget() {
        String ids = java.util.stream.IntStream.range(0, 256).mapToObj(i -> "src_" + i)
                .collect(java.util.stream.Collectors.joining(", "));
        PipelineResource pipeline = (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: broad
                """ + "source: [" + ids + "]\n");
        assertLimit(pipeline, Map.of("src_0", source("src_0", "[orders]")),
                FromClause.list(FromRef.literal("src_0.orders")),
                new Wiring.InspectionLimits(4096, 1024, 128, 32, 2048), "bytes");
    }

    @Test
    void returnedCollectionsCannotBeMutatedAndTheirMetadataIsAccounted() {
        var result = inspect(pipeline(""), sources("[orders]", "[archive]"), "src_orders.orders");
        assertThat(result.accountedBytes()).isBetween(512L, LIMITS.bytes());
        assertThatThrownBy(() -> result.routes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.sourceDeclarations().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    private static void assertLimit(PipelineResource pipeline, Map<String, Resource> resources, FromClause selected,
            Wiring.InspectionLimits limits, String dimension) {
        assertThatThrownBy(() -> Wiring.inspect(pipeline, resources, selected, limits))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.value-limit-exceeded")
                .hasMessageContaining("validation.wiring." + dimension);
    }
    private static Wiring.Inspection inspect(PipelineResource pipeline, Map<String, Resource> resources, String selected) {
        return Wiring.inspect(pipeline, resources, FromClause.list(FromRef.literal(selected)), LIMITS);
    }
    private static Map<String, Resource> sources(String orders, String archive) {
        return Map.of("src_orders", source("src_orders", orders), "src_archive", source("src_archive", archive));
    }
    private static Resource source(String id, String tables) {
        return new DslParser().parse("""
                version: tapstate/v1
                kind: source
                connector: mysql
                config: { host: localhost }
                mode: cdc
                """ + "id: " + id + "\n" + (tables == null ? "" : "tables: " + tables + "\n"));
    }
    private static PipelineResource pipeline(String body) {
        return (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: [src_orders, src_archive]
                """ + body);
    }
}
