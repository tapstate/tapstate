package io.tapstate.control.core;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineDraftCompilerTest {

    private final PipelineDraftCompiler compiler = new PipelineDraftCompiler();

    @Test
    void compilesWizardTreeWithStableAliasesAndRecursiveShapes() {
        PipelineDraft draft = wizardDraft();

        PipelineResource compiled = compiler.compile(draft);
        Step.Inline nest = (Step.Inline) compiled.transforms().stream()
                .filter(Step.Inline.class::isInstance)
                .map(Step.Inline.class::cast)
                .filter(step -> step.body() instanceof TransformBody.Nest)
                .findFirst()
                .orElseThrow();
        TransformBody.Nest body = (TransformBody.Nest) nest.body();

        assertThat(nest.id()).isEqualTo("customer-orders__nest");
        assertThat(body.root().from()).isEqualTo("orders");
        assertThat(body.root().embed()).extracting(Embed::from).containsExactly("policy__pre");
        Embed policy = body.root().embed().getFirst();
        assertThat(policy.as()).isEqualTo(EmbedAs.OBJECT);
        assertThat(policy.path()).isEqualTo("policy");
        assertThat(policy.embed()).extracting(Embed::from).containsExactly("claim");
        assertThat(policy.embed().getFirst().as()).isEqualTo(EmbedAs.ARRAY);
        assertThat(policy.embed().getFirst().path()).isEqualTo("claims");
    }

    @Test
    void compileIsAStableCanonicalFixedPoint() {
        PipelineDraft draft = wizardDraft();
        CanonicalWriter writer = new CanonicalWriter();

        String first = writer.write(compiler.compile(draft));
        String second = writer.write(compiler.compile(draft));

        assertThat(first).isEqualTo(second);
        assertThat(CanonicalHash.of(first)).hasSize(64).isEqualTo(CanonicalHash.of(second));
    }

    private static PipelineDraft wizardDraft() {
        PipelineDraft.Transform branchMap = new PipelineDraft.Transform(
                "rename-policy", "map", Map.of("policy_id", "$id"));
        PipelineDraft.Transform postFilter = new PipelineDraft.Transform(
                "only-active", "filter", Map.of("expr", "active == true"));
        PipelineDraft.Related policy = new PipelineDraft.Related(
                "policy", "orders", "crm", "policies",
                new PipelineDraft.Relation(List.of(new PipelineDraft.FieldPair("order_id", "id")),
                        PipelineDraft.Shape.OBJECT, "policy", List.of()), List.of(branchMap));
        PipelineDraft.Related claim = new PipelineDraft.Related(
                "claim", "policy", "crm", "claims",
                new PipelineDraft.Relation(List.of(new PipelineDraft.FieldPair("policy_id", "id")),
                        PipelineDraft.Shape.ARRAY, "claims", List.of("claim_id")), List.of());
        PipelineDraft.Wizard wizard = new PipelineDraft.Wizard(
                new PipelineDraft.Root("orders", "crm", "orders", List.of("id"), List.of()),
                List.of(policy, claim), List.of(postFilter),
                new PipelineDraft.Output("atlas", Map.of()));
        return new PipelineDraft("customer-orders", 1, 1, PipelineDraft.Mode.WIZARD,
                "Customer orders", "", null, wizard, null, null, null,
                java.time.Instant.parse("2026-09-21T00:00:00Z"),
                java.time.Instant.parse("2026-09-21T00:00:00Z"), "test");
    }
}
