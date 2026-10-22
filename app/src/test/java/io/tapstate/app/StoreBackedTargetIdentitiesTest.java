package io.tapstate.app;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class StoreBackedTargetIdentitiesTest {
    private final InMemoryStorePort store = new InMemoryStorePort();
    private final DslParser parser = new DslParser();
    private final StoreBackedTargetIdentities identities = new StoreBackedTargetIdentities(store,
            id -> { throw new AssertionError("scope detection must not open connectors"); });

    @Test
    void plainAndUnreachableExpansionsNeedNoDiscoveredSchemaOrTargetMetadata() {
        var plain = pipeline("", "orders");
        assertThat(identities.relevant(plain)).isFalse();
        assertThat(identities.assess(plain, true)).isEmpty();
        source();
        var unused = pipeline("transforms: [{id: expanded, type: unwind, from: orders, path: items, include_array_index: position}]", "orders");
        assertThat(identities.relevant(unused)).isFalse();
        assertThat(identities.assess(unused, true)).isEmpty();
    }

    @Test
    void inlineAndSharedExpansionsAreFoundThroughTheirActualSyncWiring() {
        source();
        var inline = pipeline("transforms: [{id: expanded, type: unwind, from: orders, path: items, include_array_index: position}]", "expanded");
        assertThat(identities.relevant(inline)).isTrue();
        assertThat(identities.relevant(pipeline("transforms: [{id: expanded, type: unwind, from: orders, path: items, include_array_index: position}]",
                "/^expanded$/"))).isTrue();
        assertThat(identities.relevant(pipeline("transforms: [{id: expanded, type: unwind, from: orders, path: items, include_array_index: position}]",
                "/^orders$/"))).isFalse();
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: transform
                id: shared
                type: unwind
                path: items
                include_array_index: position
                """));
        var shared = pipeline("transforms: [{id: expanded, use: shared, from: orders}]", "expanded");
        assertThat(identities.relevant(shared)).isTrue();
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: serve
                id: delivery
                sync: [{id: out, source: src, write_mode: upsert}]
                """));
        PipelineResource referencedServe = new PipelineResource(shared.id(), shared.metadata(), shared.sources(),
                shared.transforms(), shared.view(),
                new io.tapstate.core.model.ServeBlock.Use("out", "delivery", ((io.tapstate.core.model.ServeBlock.Inline) shared.serve()).from()),
                shared.settings(), shared.experimental());
        assertThat(identities.relevant(referencedServe)).isTrue();
    }

    @Test
    void previewSeparatesThePreservedLogicalKeyFromTheGeneratedIdentityAnnotation() {
        var target = new io.tapstate.spi.sink.TargetTable("orders", java.util.List.of());
        var key = java.util.List.of("original_id", "position");
        var plan = new io.tapstate.adapters.pdk.TargetIdentityPlan("automatic", "generated-target-identity",
                target, "_id", "original_id", key);
        var assessment = new io.tapstate.control.core.PipelineTargetIdentities.Assessment(
                "out", "target", "orders", plan.status(), plan.reason(), plan.sourceField(), plan.preservedField(), key);
        var payload = StoreBackedTargetIdentities.payload(new StoreBackedTargetIdentities.Resolved(assessment, "expanded", plan));
        assertThat(payload).containsEntry("generatedFields", java.util.List.of("_id"))
                .containsEntry("preservedField", "original_id").containsEntry("writeKey", key)
                .containsEntry("stream", "expanded").doesNotContainKey("documents");
        var unchanged = new io.tapstate.adapters.pdk.TargetIdentityPlan("not-required", "compatible-key", target, null, null, key);
        assertThat(StoreBackedTargetIdentities.payload(new StoreBackedTargetIdentities.Resolved(
                new io.tapstate.control.core.PipelineTargetIdentities.Assessment("out", "target", "orders",
                        unchanged.status(), unchanged.reason(), null, null, key), "expanded", unchanged)))
                .containsEntry("generatedFields", java.util.List.of());
    }

    private void source() {
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: src
                connector: mongodb
                tables: [orders]
                """));
    }

    private PipelineResource pipeline(String transforms, String served) {
        return (PipelineResource) parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: pl
                source: src
                %s
                serve: {from: %s, sync: [{id: out, source: src, write_mode: upsert}]}
                """.formatted(transforms, served));
    }
}
