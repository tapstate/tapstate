package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValidationDeclarationsTest {
    private static final ValidationDeclarations.Limits LIMITS = ValidationDeclarations.Limits.defaults();

    @Test
    void selectedSyncExpandsReusableBodiesAndKeepsLocalWiringExecutionAndRename() {
        PipelineResource pipeline = pipeline("""
                transforms:
                  - id: mapped
                    use: map_definition
                    from: src.orders
                    execution: { parallelism: 3 }
                  - { id: unrelated, use: missing_transform, from: absent.other }
                view: { id: current_view, use: view_definition, from: mapped }
                serve: { use: serve_definition, from: current_view }
                """);
        var result = inspect(pipeline, definitions(), ValidationDeclarations.OutputKind.SYNC, "orders_sink");
        assertThat(result.requiredResourceIds()).containsExactlyInAnyOrder(
                "pipeline", "src", "target", "map_definition", "view_definition", "serve_definition");
        assertThat(result.steps()).containsOnlyKeys("mapped");
        Step.Inline step = result.steps().get("mapped");
        assertThat(step.id()).isEqualTo("mapped");
        assertThat(step.from()).isEqualTo(FromClause.list(FromRef.literal("src.orders")));
        assertThat(step.execution().parallelism()).isEqualTo(3);
        assertThat(step.body()).isInstanceOf(TransformBody.MapProjection.class);
        assertThat(result.view().id()).isEqualTo("current_view");
        assertThat(result.view().from()).isEqualTo(FromRef.literal("mapped"));
        assertThat(result.view().primaryKey()).isEqualTo("id");
        assertThat(result.view().execution().parallelism()).isEqualTo(5);
        assertThat(result.from()).isEqualTo(FromClause.list(FromRef.literal("current_view")));
        assertThat(result.sync().rename().map()).containsEntry("orders", "renamed_orders");
        assertThat(result.sync().source()).isEqualTo("target");
        assertThat(result.routes()).singleElement().satisfies(route -> {
            assertThat(route.source()).isEqualTo("src");
            assertThat(route.table()).isEqualTo("orders");
            assertThat(route.steps()).containsExactly(step);
        });
        assertThat(result.pending()).isEmpty();
    }

    @Test
    void viewSelectionIgnoresUnrelatedServeDefinitionAndTarget() {
        var resources = definitions();
        resources.remove("serve_definition"); resources.remove("target");
        var result = inspect(pipeline("""
                view: { id: current_view, use: view_definition, from: src.orders }
                serve: { use: serve_definition, from: current_view }
                """), resources, ValidationDeclarations.OutputKind.VIEW, "current_view");
        assertThat(result.requiredResourceIds()).containsExactlyInAnyOrder("pipeline", "src", "view_definition");
        assertThat(result.sync()).isNull();
        assertThat(result.view().id()).isEqualTo("current_view");
    }

    @Test
    void selectedSyncIgnoresUnvisitedViewAndOtherSinks() {
        var result = inspect(pipeline("""
                view: { use: missing_view, from: absent.other }
                serve:
                  from: src.orders
                  sync:
                    - { id: selected, source: target }
                    - { id: other, source: absent_target }
                """), definitions(), ValidationDeclarations.OutputKind.SYNC, "selected");
        assertThat(result.view()).isNull();
        assertThat(result.requiredResourceIds()).containsExactlyInAnyOrder("pipeline", "src", "target");
    }

    @Test
    void selectedMissingAndWrongKindReusableDefinitionsFailAtTheirUseLocation() {
        PipelineResource pipeline = pipeline("""
                transforms:
                  - { id: selected, use: unavailable, from: src.orders }
                view: { id: output, from: selected, primary_key: id }
                """);
        var resources = definitions();
        assertThatThrownBy(() -> inspect(pipeline, resources, ValidationDeclarations.OutputKind.VIEW, "output"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference")
                .hasMessageContaining("transforms.selected.use");
        resources.put("unavailable", source("unavailable"));
        assertThatThrownBy(() -> inspect(pipeline, resources, ValidationDeclarations.OutputKind.VIEW, "output"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference");
    }

    @Test
    void dynamicReferenceRetainsDownstreamReusableStepsAndUnresolvedObligation() {
        var result = inspect(pipeline("""
                transforms:
                  - { id: mapped, use: map_definition, from: /step_.*/ }
                  - { id: step_one, use: not_yet_resolved, from: src.orders }
                view: { id: output, from: mapped, primary_key: id }
                """), definitions(), ValidationDeclarations.OutputKind.VIEW, "output");
        assertThat(result.routes()).isEmpty();
        assertThat(result.pending()).singleElement().satisfies(pending -> {
            assertThat(pending.reference()).isEqualTo(FromRef.regex("step_.*"));
            assertThat(pending.downstreamSteps()).extracting(Step.Inline::id).containsExactly("mapped");
        });
        assertThat(result.requiredResourceIds()).containsExactlyInAnyOrder("pipeline", "map_definition");
    }

    @Test
    void mismatchedResourceMapIdentityCannotBecomeARequiredVersionReference() {
        var resources = definitions();
        resources.put("target", source("some_other_connection"));
        assertThatThrownBy(() -> inspect(pipeline("""
                serve: { from: src.orders, sync: [{ id: selected, source: target }] }
                """), resources, ValidationDeclarations.OutputKind.SYNC, "selected"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference");
    }

    @Test
    void aliasInputsCannotBeUsedWithAReusableStreamingBody() {
        PipelineResource pipeline = new PipelineResource("pipeline", null,
                pipeline("").sources().subList(0, 1),
                java.util.List.of(Step.use("mapped", "map_definition",
                        new FromClause.Aliases(Map.of("root", FromRef.literal("src.orders"))))),
                new io.tapstate.core.model.ViewBlock.Inline("output", FromRef.literal("mapped"), "id", null),
                null, null, null);
        assertThatThrownBy(() -> inspect(pipeline, definitions(), ValidationDeclarations.OutputKind.VIEW, "output"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.composition")
                .hasMessageContaining("transforms.mapped.from");
    }

    @Test
    void missingOutputAndDuplicateSelectedSyncAreRejected() {
        PipelineResource pipeline = pipeline("""
                serve:
                  from: src.orders
                  sync:
                    - { id: duplicate, source: target }
                    - { id: duplicate, source: target }
                """);
        assertThatThrownBy(() -> inspect(pipeline, definitions(), ValidationDeclarations.OutputKind.SYNC, "missing"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.missing-reference");
        assertThatThrownBy(() -> inspect(pipeline, definitions(), ValidationDeclarations.OutputKind.SYNC, "duplicate"))
                .isInstanceOf(DslException.class).hasMessageContaining("dsl.duplicate-id");
    }

    @Test
    void reusableExperimentalMetadataIsChargedBeforeTheInlineConstructorCopiesIt() {
        var resources = definitions();
        var definition = (io.tapstate.core.model.TransformResource) resources.get("map_definition");
        var experimental = new LinkedHashMap<String, Object>();
        for (int i = 0; i < 1024; i++) experimental.put("option_" + i, true);
        resources.put("map_definition", new io.tapstate.core.model.TransformResource(
                definition.id(), definition.metadata(), definition.body(), experimental));
        PipelineResource pipeline = pipeline("""
                transforms:
                  - { id: selected, use: map_definition, from: src.orders }
                view: { id: output, from: selected, primary_key: id }
                """);
        assertThatThrownBy(() -> ValidationDeclarations.inspect(pipeline, resources,
                new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.VIEW, "output"),
                new ValidationDeclarations.Limits(128, 1024, 128, 32, 8192)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.value-limit-exceeded");
    }

    @Test
    void metadataIsBoundedAndReturnedCollectionsAreReadOnly() {
        PipelineResource pipeline = pipeline("view: { id: output, from: src.orders, primary_key: id }\n");
        assertThatThrownBy(() -> ValidationDeclarations.inspect(pipeline, definitions(),
                new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.VIEW, "output"),
                new ValidationDeclarations.Limits(128, 1024, 128, 32, 1024)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.value-limit-exceeded");
        var result = inspect(pipeline, definitions(), ValidationDeclarations.OutputKind.VIEW, "output");
        assertThat(result.accountedMetadataBytes()).isBetween(512L, LIMITS.metadataBytes());
        assertThatThrownBy(() -> result.requiredResourceIds().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.steps().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.routes().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    private static ValidationDeclarations.Inspection inspect(PipelineResource pipeline, Map<String, Resource> resources,
            ValidationDeclarations.OutputKind kind, String id) {
        return ValidationDeclarations.inspect(pipeline, resources, new ValidationDeclarations.Output(kind, id), LIMITS);
    }
    private static Map<String, Resource> definitions() {
        var result = new LinkedHashMap<String, Resource>();
        for (Resource resource : java.util.List.of(source("src"), source("target"),
                parse("kind: transform\nid: map_definition\ntype: map\nfields: { amount: 1 }\n"),
                parse("kind: view\nid: view_definition\nprimary_key: id\nexecution: { parallelism: 5 }\n"),
                parse("""
                        kind: serve
                        id: serve_definition
                        sync:
                          - id: orders_sink
                            source: target
                            rename: { map: { orders: renamed_orders } }
                          - { id: other, source: absent_target }
                        """))) result.put(resource.id(), resource);
        return result;
    }
    private static Resource source(String id) {
        return parse("kind: source\nid: " + id + "\nconnector: mysql\nconfig: { host: localhost }\nmode: cdc\ntables: [orders]\n");
    }
    private static PipelineResource pipeline(String body) {
        return (PipelineResource) parse("kind: pipeline\nid: pipeline\nsource: [src, absent]\n" + body);
    }
    private static Resource parse(String body) { return new DslParser().parse("version: tapstate/v1\n" + body); }
}
