package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TableRename;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(body.root().embed()).extracting(Embed::from).containsExactly("policy");
        Embed policy = body.root().embed().getFirst();
        assertThat(policy.as()).isEqualTo(EmbedAs.OBJECT);
        assertThat(policy.path()).isEqualTo("policy");
        assertThat(policy.embed()).extracting(Embed::from).containsExactly("claim");
        assertThat(policy.embed().getFirst().as()).isEqualTo(EmbedAs.ARRAY);
        assertThat(policy.embed().getFirst().path()).isEqualTo("claims");
        assertThat(((FromClause.Aliases) nest.from()).aliases())
                .containsEntry("policy", FromRef.literal("policy__pre"))
                .containsEntry("claim", FromRef.literal("crm.claims"));
        assertThat(compiled.serve()).isEqualTo(new ServeBlock.Inline(
                "atlas", io.tapstate.core.model.FromRef.literal("only-active"),
                List.of(new io.tapstate.core.model.SyncElement(
                        "atlas_customer_orders", "atlas", io.tapstate.core.model.WriteMode.UPSERT,
                        new RenameSpec(Map.of("orders", "customer_orders"), null, null, null), null)), null, null));
    }

    @Test
    void compileIsAStableCanonicalFixedPoint() {
        PipelineDraft draft = wizardDraft();
        CanonicalWriter writer = new CanonicalWriter();

        String first = writer.write(compiler.compile(draft));
        String second = writer.write(compiler.compile(draft));

        assertThat(first).isEqualTo(second);
        assertThat(CanonicalHash.ofText(first)).hasSize(64).isEqualTo(CanonicalHash.ofText(second));
    }

    @Test
    void compilesFlatRelationsAsPathlessNestEmbedsAndRoundTripsDsl() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Wizard wizard = template.wizard();
        PipelineDraft.Related flatProfile = new PipelineDraft.Related(
                "profile", "orders", "crm", "profiles",
                new PipelineDraft.Relation(List.of(new PipelineDraft.FieldPair("customer_id", "customer_id")),
                        PipelineDraft.Shape.FLAT, null, List.of("id"), List.of()),
                List.of(new PipelineDraft.Transform("normalize-profile", "map", Map.of("name", "$full_name"))));
        PipelineDraft flatDraft = new PipelineDraft(
                template.pipelineId(), template.schemaVersion(), template.revision(), template.mode(),
                template.name(), template.description(), template.graph(),
                new PipelineDraft.Wizard(wizard.root(), List.of(flatProfile), List.of(), wizard.output()),
                template.baseArtifactHash(), template.publishedDraftRevision(), template.publishedArtifactHash(),
                template.createdAt(), template.updatedAt(), template.updatedBy());

        PipelineResource compiled = compiler.compile(flatDraft);
        Step.Inline nest = compiled.transforms().stream()
                .filter(Step.Inline.class::isInstance)
                .map(Step.Inline.class::cast)
                .filter(step -> step.body() instanceof TransformBody.Nest)
                .findFirst()
                .orElseThrow();
        Embed embed = ((TransformBody.Nest) nest.body()).root().embed().getFirst();
        String canonical = new CanonicalWriter().write(compiled);

        assertThat(embed.as()).isEqualTo(EmbedAs.FLAT);
        assertThat(embed.path()).isNull();
        assertThat(embed.arrayKey()).isNull();
        assertThat(embed.key()).containsExactly("id");
        assertThat(((FromClause.Aliases) nest.from()).aliases())
                .containsEntry("profile", FromRef.literal("profile__pre"));
        assertThat(new CanonicalWriter().write(new DslParser().parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void compilesSourceTransformsWithoutInventingAnEmptyNest() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Wizard wizard = template.wizard();
        PipelineDraft sourceOnly = new PipelineDraft(
                template.pipelineId(), template.schemaVersion(), template.revision(), template.mode(),
                template.name(), template.description(), template.graph(),
                new PipelineDraft.Wizard(wizard.root(), List.of(),
                        List.of(new PipelineDraft.Transform("active-orders", "filter", Map.of("expr", "active == true"))),
                        new PipelineDraft.Output("atlas", Map.of("sourceId", "atlas", "table", "orders_archive"))),
                template.baseArtifactHash(), template.publishedDraftRevision(), template.publishedArtifactHash(),
                template.createdAt(), template.updatedAt(), template.updatedBy());

        PipelineResource compiled = compiler.compile(sourceOnly);

        assertThat(compiled.transforms()).extracting(Step::id).containsExactly("active-orders");
        assertThat(compiled.sourceIds()).containsExactly("crm");
        Step.Inline filter = (Step.Inline) compiled.transforms().getFirst();
        assertThat(((io.tapstate.core.model.FromClause.Flow) filter.from()).refs())
                .containsExactly(io.tapstate.core.model.FromRef.literal("crm.orders"));
        assertThat(compiled.serve()).isInstanceOf(ServeBlock.Inline.class);
        assertThat(((io.tapstate.core.model.FromClause.Flow) ((ServeBlock.Inline) compiled.serve()).from()).refs())
                .containsExactly(io.tapstate.core.model.FromRef.literal("active-orders"));
        assertThat(((ServeBlock.Inline) compiled.serve()).sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("orders", "orders_archive"), null, null, null));
    }

    @Test
    void compilesOnPremWizardOutputToTheExistingGenericServeSyncShape() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Wizard wizard = template.wizard();
        PipelineDraft draft = new PipelineDraft(
                template.pipelineId(), template.schemaVersion(), template.revision(), template.mode(),
                template.name(), template.description(), template.graph(),
                new PipelineDraft.Wizard(wizard.root(), List.of(),
                        List.of(new PipelineDraft.Transform("keep-active", "filter", Map.of("expr", "active == true"))),
                        new PipelineDraft.Output("source", Map.of(
                                "sourceId", "warehouse", "table", "orders_archive", "writeMode", "append"))),
                template.baseArtifactHash(), template.publishedDraftRevision(), template.publishedArtifactHash(),
                template.createdAt(), template.updatedAt(), template.updatedBy());

        PipelineResource compiled = compiler.compile(draft);

        assertThat(compiled.serve()).isInstanceOf(ServeBlock.Inline.class);
        ServeBlock.Inline serve = (ServeBlock.Inline) compiled.serve();
        assertThat(serve.id()).isEqualTo("target");
        assertThat(serve.sync().getFirst().source()).isEqualTo("warehouse");
        assertThat(serve.sync().getFirst().writeMode().yaml()).isEqualTo("append");
        assertThat(serve.sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("orders", "orders_archive"), null, null, null));
        assertThat(compiled.transforms()).extracting(Step::id).containsExactly("keep-active");
    }

    @Test
    void compilesTheK2WizardShapeWithTheTargetModelsUnqualifiedRenameKey() {
        PipelineDraft template = wizardDraft();
        PipelineDraft draft = new PipelineDraft("k2", template.schemaVersion(), template.revision(),
                PipelineDraft.Mode.WIZARD, "k2", "", null,
                new PipelineDraft.Wizard(
                        new PipelineDraft.Root("root", "mysql", "AA_0716", List.of("ID"), List.of()),
                        List.of(), List.of(),
                        new PipelineDraft.Output("source", Map.of(
                                "sourceId", "mongo", "table", "k2", "writeMode", "upsert"))),
                null, null, null, template.createdAt(), template.updatedAt(), template.updatedBy());

        PipelineResource compiled = compiler.compile(draft);
        ServeBlock.Inline serve = (ServeBlock.Inline) compiled.serve();

        assertThat(((FromClause.Flow) serve.from()).refs())
                .containsExactly(FromRef.literal("mysql.AA_0716"));
        assertThat(serve.sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("AA_0716", "k2"), null, null, null));
        assertThat(TableRename.apply("AA_0716", serve.sync().getFirst().rename())).isEqualTo("k2");
        PipelineResource roundTripped = (PipelineResource) new DslParser().parse(new CanonicalWriter().write(compiled));
        assertThat(((ServeBlock.Inline) roundTripped.serve()).sync().getFirst().rename())
                .isEqualTo(serve.sync().getFirst().rename());
    }

    @Test
    void refusesWizardCompilationWithoutAnOutput() {
        PipelineDraft complete = wizardDraft();
        PipelineDraft.Wizard wizard = complete.wizard();
        PipelineDraft incomplete = new PipelineDraft(
                complete.pipelineId(), complete.schemaVersion(), complete.revision(), complete.mode(),
                complete.name(), complete.description(), complete.graph(),
                new PipelineDraft.Wizard(wizard.root(), wizard.related(), wizard.transforms(), null),
                complete.baseArtifactHash(), complete.publishedDraftRevision(), complete.publishedArtifactHash(),
                complete.createdAt(), complete.updatedAt(), complete.updatedBy());

        assertThatThrownBy(() -> compiler.compile(incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("wizard output is required for publication");
    }

    @Test
    void refusesUnsupportedWizardOutputKinds() {
        PipelineDraft complete = wizardDraft();
        PipelineDraft.Wizard wizard = complete.wizard();
        PipelineDraft incomplete = new PipelineDraft(
                complete.pipelineId(), complete.schemaVersion(), complete.revision(), complete.mode(),
                complete.name(), complete.description(), complete.graph(),
                new PipelineDraft.Wizard(wizard.root(), wizard.related(), wizard.transforms(),
                        new PipelineDraft.Output("kafka", Map.of("sourceId", "kafka", "table", "orders"))),
                complete.baseArtifactHash(), complete.publishedDraftRevision(), complete.publishedArtifactHash(),
                complete.createdAt(), complete.updatedAt(), complete.updatedBy());

        assertThatThrownBy(() -> compiler.compile(incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unsupported wizard output: kafka");
    }

    @Test
    void compilesCanvasGraphIntoViewAndTargetPublication() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source-orders", "source", "crm", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("active-orders", "filter", null, null,
                        Map.of("expr", "active == true"), Map.of()),
                new PipelineDraft.Node("orders-view", "view", null, null, Map.of(), Map.of("viewId", "orders_view")),
                new PipelineDraft.Node("warehouse-orders", "target", "warehouse", "orders_archive",
                        Map.of("writeMode", "append"), Map.of())),
                List.of(
                        new PipelineDraft.Edge("source-to-filter", "source-orders", "active-orders"),
                        new PipelineDraft.Edge("filter-to-view", "active-orders", "orders-view"),
                        new PipelineDraft.Edge("filter-to-target", "active-orders", "warehouse-orders")),
                new PipelineDraft.Viewport(0, 0, 1));
        PipelineDraft draft = new PipelineDraft("orders", 1, 1, PipelineDraft.Mode.DAG,
                "Orders", "", graph, null, null, null, null,
                java.time.Instant.parse("2026-09-21T00:00:00Z"),
                java.time.Instant.parse("2026-09-21T00:00:00Z"), "test");

        PipelineResource compiled = compiler.compile(draft);

        assertThat(compiled.sources()).extracting(source -> source.id()).containsExactly("crm");
        assertThat(compiled.transforms()).extracting(Step::id).containsExactly("active-orders");
        assertThat(compiled.view()).isEqualTo(new ViewBlock.Inline(
                "orders_view", io.tapstate.core.model.FromRef.literal("active-orders"), null, null));
        assertThat(compiled.serve()).isInstanceOf(ServeBlock.Inline.class);
        ServeBlock.Inline serve = (ServeBlock.Inline) compiled.serve();
        assertThat(serve.sync()).extracting(sync -> sync.source()).containsExactly("warehouse");
        assertThat(serve.sync().getFirst().writeMode().yaml()).isEqualTo("append");
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
                new PipelineDraft.Output("atlas", Map.of("sourceId", "atlas", "table", "customer_orders")));
        return new PipelineDraft("customer-orders", 1, 1, PipelineDraft.Mode.WIZARD,
                "Customer orders", "", null, wizard, null, null, null,
                java.time.Instant.parse("2026-09-21T00:00:00Z"),
                java.time.Instant.parse("2026-09-21T00:00:00Z"), "test");
    }
}
