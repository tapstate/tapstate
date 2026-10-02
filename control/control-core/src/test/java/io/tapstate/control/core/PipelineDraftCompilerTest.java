package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FieldRule;
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
    void rejectsDuplicateChildFieldsInOneAssociation() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Wizard wizard = template.wizard();
        PipelineDraft.Related duplicate = new PipelineDraft.Related("duplicate", "orders", "crm", "items",
                new PipelineDraft.Relation(List.of(
                        new PipelineDraft.FieldPair("order_id", "id"),
                        new PipelineDraft.FieldPair("order_id", "tenant_id")),
                        PipelineDraft.Shape.ARRAY, "items", List.of(), List.of("id")), List.of());
        PipelineDraft draft = withWizard(template, wizard.root(), List.of(duplicate), List.of(), wizard.output());

        assertCompileFails(draft, "related association contains duplicate child field: order_id");
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
    void compilesComputedMapValuesAsDslExpressionsInsteadOfLiterals() {
        PipelineDraft template = wizardDraft();
        PipelineDraft draft = withWizard(template, template.wizard().root(), List.of(),
                List.of(new PipelineDraft.Transform("project-fields", "map", Map.of(
                        "display_name", "=1 + 1",
                        "source_name", "$name",
                        "removed", false,
                        "region", "north"))),
                template.wizard().output());

        PipelineResource compiled = compiler.compile(draft);
        Step.Inline map = (Step.Inline) compiled.transforms().getFirst();
        Map<String, FieldRule> fields = ((TransformBody.MapProjection) map.body()).fields();
        String canonical = new CanonicalWriter().write(compiled);

        assertThat(fields)
                .containsEntry("display_name", FieldRule.computed("1 + 1"))
                .containsEntry("source_name", FieldRule.rename("name"))
                .containsEntry("removed", FieldRule.drop())
                .containsEntry("region", FieldRule.literal("north"));
        assertThat(canonical).contains("display_name: \"=1 + 1\"");
        assertThat(new CanonicalWriter().write(new DslParser().parse(canonical))).isEqualTo(canonical);
        assertThatThrownBy(() -> compiler.compile(withWizard(template, template.wizard().root(), List.of(),
                List.of(new PipelineDraft.Transform("empty-expression", "map", Map.of("display_name", "="))),
                template.wizard().output())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("computed map expression must be non-blank: display_name");
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
        assertThat(serve.id()).isNotEqualTo(serve.sync().getFirst().id());
        assertThat(serve.sync().getFirst().id()).isEqualTo("warehouse-orders");
        assertThat(serve.sync()).extracting(sync -> sync.source()).containsExactly("warehouse");
        assertThat(serve.sync().getFirst().writeMode().yaml()).isEqualTo("append");
        assertThat(serve.sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("orders", "orders_archive"), null, null, null));
    }

    @Test
    void compilesDirectDagSourceToTargetWithDistinctIdsAndTargetTableRename() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("draft:source:1", "source", "mysql", "ai_fulfillment_orders", Map.of(), Map.of()),
                new PipelineDraft.Node("draft:target:1", "target", "mongo", "p1_view_1", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-target", "draft:source:1", "draft:target:1")),
                new PipelineDraft.Viewport(0, 0, 1));
        PipelineResource compiled = compiler.compile(dagDraft(graph));
        ServeBlock.Inline serve = (ServeBlock.Inline) compiled.serve();

        assertThat(serve.id()).isEqualTo("draft:target:1__serve");
        assertThat(serve.sync().getFirst().id()).isEqualTo("draft:target:1");
        assertThat(TableRename.apply("ai_fulfillment_orders", serve.sync().getFirst().rename()))
                .isEqualTo("p1_view_1");
        String canonical = new CanonicalWriter().write(compiled);
        assertThat(new CanonicalWriter().write(new DslParser().parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void compilesDagTransformEditorsIntoTypedDslAndUsesConfiguredStepIds() {
        Map<String, Object> nestBody = Map.of("root", Map.of(
                "from", "orders",
                "key", List.of("id"),
                "embed", List.of(Map.of(
                        "from", "items",
                        "on", Map.of("order_id", "id"),
                        "as", "array",
                        "path", "items"))));
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("orders-source", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("archive-source", "source", "mysql", "archive_orders", Map.of(), Map.of()),
                new PipelineDraft.Node("customers-source", "source", "mysql", "customers", Map.of(), Map.of()),
                new PipelineDraft.Node("items-source", "source", "mysql", "order_items", Map.of(), Map.of()),
                new PipelineDraft.Node("draft:transform:map", "map", null, null,
                        Map.of("fields", Map.of("customer_id", Map.of("sourceField", "customerId"),
                                "obsolete", false)), Map.of("stepId", "normalize_orders")),
                new PipelineDraft.Node("draft:transform:filter", "filter", null, null,
                        Map.of("expr", "after.active == true"), Map.of("stepId", "active_orders")),
                new PipelineDraft.Node("draft:transform:js", "js", null, null,
                        Map.of("script", "function process(record) { return record; }"),
                        Map.of("stepId", "scripted_orders")),
                new PipelineDraft.Node("draft:transform:union", "union", null, null,
                        Map.of("from", List.of("mysql.orders", "mysql.archive_orders")),
                        Map.of("stepId", "all_orders")),
                new PipelineDraft.Node("draft:transform:join", "join", null, null,
                        Map.of("from", Map.of("o", "mysql.orders", "c", "mysql.customers"),
                                "engine", "builtin", "sql", "SELECT o.id AS id FROM o LEFT JOIN c ON o.customer_id = c.id"),
                        Map.of("stepId", "joined_orders")),
                new PipelineDraft.Node("draft:transform:nest", "nest", null, null,
                        Map.of("from", Map.of("orders", "mysql.orders", "items", "mysql.order_items"),
                                "root", nestBody.get("root")), Map.of("stepId", "customer_document")),
                new PipelineDraft.Node("draft:transform:unwind", "unwind", null, null,
                        Map.of("path", "items", "includeArrayIndex", "item_index",
                                "preserveNullAndEmptyArrays", true), Map.of("stepId", "expanded_orders"))),
                List.of(
                        new PipelineDraft.Edge("orders-to-map", "orders-source", "draft:transform:map"),
                        new PipelineDraft.Edge("map-to-filter", "draft:transform:map", "draft:transform:filter"),
                        new PipelineDraft.Edge("filter-to-js", "draft:transform:filter", "draft:transform:js"),
                        new PipelineDraft.Edge("orders-to-union", "orders-source", "draft:transform:union"),
                        new PipelineDraft.Edge("archive-to-union", "archive-source", "draft:transform:union"),
                        new PipelineDraft.Edge("orders-to-join", "orders-source", "draft:transform:join"),
                        new PipelineDraft.Edge("customers-to-join", "customers-source", "draft:transform:join"),
                        new PipelineDraft.Edge("orders-to-nest", "orders-source", "draft:transform:nest"),
                        new PipelineDraft.Edge("items-to-nest", "items-source", "draft:transform:nest"),
                        new PipelineDraft.Edge("orders-to-unwind", "orders-source", "draft:transform:unwind")),
                new PipelineDraft.Viewport(0, 0, 1));

        PipelineResource compiled = compiler.compile(dagDraft(graph));

        assertThat(compiled.transforms()).extracting(Step::id).containsExactly(
                "normalize_orders", "active_orders", "scripted_orders", "all_orders", "joined_orders",
                "customer_document", "expanded_orders");
        Step.Inline map = (Step.Inline) compiled.transforms().getFirst();
        assertThat(((TransformBody.MapProjection) map.body()).fields())
                .containsEntry("customer_id", FieldRule.rename("customerId"))
                .containsEntry("obsolete", FieldRule.drop());
        assertThat(((TransformBody.Filter) ((Step.Inline) compiled.transforms().get(1)).body()).expr())
                .isEqualTo("after.active == true");
        assertThat(((TransformBody.Js) ((Step.Inline) compiled.transforms().get(2)).body()).script())
                .isEqualTo("function process(record) { return record; }");
        Step.Inline union = (Step.Inline) compiled.transforms().get(3);
        assertThat(union.body()).isInstanceOf(TransformBody.Union.class);
        assertThat(((FromClause.Flow) union.from()).refs()).containsExactly(
                FromRef.literal("mysql.orders"), FromRef.literal("mysql.archive_orders"));
        Step.Inline join = (Step.Inline) compiled.transforms().get(4);
        assertThat(join.body()).isEqualTo(new TransformBody.Join(
                io.tapstate.core.model.JoinEngine.BUILTIN,
                "SELECT o.id AS id FROM o LEFT JOIN c ON o.customer_id = c.id"));
        assertThat(((FromClause.Aliases) join.from()).aliases())
                .containsEntry("o", FromRef.literal("mysql.orders"))
                .containsEntry("c", FromRef.literal("mysql.customers"));
        Step.Inline nest = (Step.Inline) compiled.transforms().get(5);
        assertThat(((TransformBody.Nest) nest.body()).root().from()).isEqualTo("orders");
        assertThat(((FromClause.Aliases) nest.from()).aliases())
                .containsEntry("orders", FromRef.literal("mysql.orders"))
                .containsEntry("items", FromRef.literal("mysql.order_items"));
        TransformBody.Unwind unwind = (TransformBody.Unwind) ((Step.Inline) compiled.transforms().get(6)).body();
        assertThat(unwind.path()).isEqualTo("items");
        assertThat(unwind.includeArrayIndex()).isEqualTo("item_index");
        assertThat(unwind.preserveNullAndEmptyArrays()).isTrue();

        String canonical = new CanonicalWriter().write(compiled);
        assertThat(new CanonicalWriter().write(new DslParser().parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void compilesRegexAndMultipleSelectedTablesFromOneSourceNode() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders_.*",
                        Map.of("tables", List.of(
                                Map.of("table", "orders_.*", "tableKind", "regex"),
                                Map.of("table", "archive_orders"))), Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-target", "source", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();

        assertThat(((FromClause.Flow) serve.from()).refs()).containsExactly(
                FromRef.regex("\\Qmysql\\E\\.orders_.*"), FromRef.literal("mysql.archive_orders"));
        String canonical = new CanonicalWriter().write(compiler.compile(dagDraft(graph)));
        assertThat(new CanonicalWriter().write(new DslParser().parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void removesWholeTableRegexAnchorsBeforeAddingTheSourcePrefix() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "^orders_\\d+$",
                        Map.of("tables", List.of(Map.of("table", "^orders_\\d+$", "tableKind", "regex"))),
                        Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-target", "source", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();

        assertThat(((FromClause.Flow) serve.from()).refs())
                .containsExactly(FromRef.regex("\\Qmysql\\E\\.orders_\\d+"));
    }

    @Test
    void scopesEveryRegexAlternativeToTheSelectedSource() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "crm", "orders|users",
                        Map.of("tableKind", "regex", "tables", List.of(
                                Map.of("table", "orders|users", "tableKind", "regex"),
                                Map.of("table", "^orders$|^users$", "tableKind", "regex"))), Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-target", "source", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();
        List<FromRef> refs = ((FromClause.Flow) serve.from()).refs();
        assertThat(refs).hasSize(2);
        for (FromRef ref : refs) {
            String regex = ((FromRef.Regex) ref).pattern();
            assertThat(java.util.regex.Pattern.compile(regex).matcher("crm.orders").matches()).isTrue();
            assertThat(java.util.regex.Pattern.compile(regex).matcher("crm.users").matches()).isTrue();
            assertThat(java.util.regex.Pattern.compile(regex).matcher("users").matches()).isFalse();
            assertThat(java.util.regex.Pattern.compile(regex).matcher("other.users").matches()).isFalse();
        }
    }

    @Test
    void rejectsExplicitNullMapLiteralAsMalformedDraft() {
        Map<String, Object> literal = new java.util.LinkedHashMap<>();
        literal.put("value", null);
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "crm", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("map", "map", null, null,
                        Map.of("fields", Map.of("deleted_at", literal)), Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-map", "source", "map"),
                        new PipelineDraft.Edge("map-target", "map", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        assertThatThrownBy(() -> compiler.compile(dagDraft(graph)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deleted_at");
    }

    @Test
    void targetRenameAfterATransformUsesTheUnderlyingSourceTableName() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("map-node", "map", null, null,
                        Map.of("fields", Map.of("id", "$order_id"),
                                "experimental", Map.of("field-mode", "permissive")),
                        Map.of("stepId", "normalized")),
                new PipelineDraft.Node("target", "target", "mongo", "orders_archive", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-map", "source", "map-node"),
                        new PipelineDraft.Edge("map-target", "map-node", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        PipelineResource compiled = compiler.compile(dagDraft(graph));
        ServeBlock.Inline serve = (ServeBlock.Inline) compiled.serve();
        Step.Inline map = (Step.Inline) compiled.transforms().getFirst();

        assertThat(serve.sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("orders", "orders_archive"), null, null, null));
        assertThat(map.experimental()).containsEntry("field-mode", "permissive");
    }

    @Test
    void targetRenameAfterANestUsesTheConfiguredRootTable() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("orders", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("items", "source", "mysql", "order_items", Map.of(), Map.of()),
                new PipelineDraft.Node("nest", "nest", null, null, Map.of(
                        "from", Map.of("orders", "mysql.orders", "items", "mysql.order_items"),
                        "root", Map.of("from", "orders", "key", List.of("id"), "embed", List.of())),
                        Map.of("stepId", "assembled")),
                new PipelineDraft.Node("target", "target", "mongo", "orders_archive", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("orders-nest", "orders", "nest"),
                        new PipelineDraft.Edge("items-nest", "items", "nest"),
                        new PipelineDraft.Edge("nest-target", "nest", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();

        assertThat(serve.sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("orders", "orders_archive"), null, null, null));
    }

    @Test
    void targetRenameAfterAJoinUsesTheSqlFactTable() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("orders", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("customers", "source", "mysql", "customers", Map.of(), Map.of()),
                new PipelineDraft.Node("join", "join", null, null, Map.of(
                        "from", Map.of("o", "mysql.orders", "c", "mysql.customers"),
                        "engine", "builtin",
                        "sql", "SELECT o.id FROM o LEFT JOIN c ON o.customer_id = c.id"),
                        Map.of("stepId", "joined-orders")),
                new PipelineDraft.Node("target", "target", "mongo", "orders_archive", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("orders-join", "orders", "join"),
                        new PipelineDraft.Edge("customers-join", "customers", "join"),
                        new PipelineDraft.Edge("join-target", "join", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();

        assertThat(serve.sync().getFirst().rename())
                .isEqualTo(new RenameSpec(Map.of("orders", "orders_archive"), null, null, null));
    }

    @Test
    void compilesMultipleDagSourceTablesAndExplicitTargetMappings() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders",
                        Map.of("tables", List.of(Map.of("table", "orders"),
                                Map.of("table", "archive_orders"))), Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", "orders_out",
                        Map.of("tableMappings", List.of(
                                Map.of("sourceTable", "orders", "targetTable", "orders_out"),
                                Map.of("sourceTable", "archive_orders", "targetTable", "archive_out"))), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-target", "source", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();

        assertThat(((FromClause.Flow) serve.from()).refs()).containsExactly(
                FromRef.literal("mysql.orders"), FromRef.literal("mysql.archive_orders"));
        assertThat(serve.sync().getFirst().rename().map())
                .containsExactlyInAnyOrderEntriesOf(Map.of("orders", "orders_out", "archive_orders", "archive_out"));
    }

    @Test
    void usesGraphViewNodeIdWhenViewMetadataOmitsViewId() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source-orders", "source", "crm", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("orders-view", "view", null, null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-view", "source-orders", "orders-view")),
                new PipelineDraft.Viewport(0, 0, 1));

        assertThat(((ViewBlock.Inline) compiler.compile(dagDraft(graph)).view()).id()).isEqualTo("orders-view");
    }

    @Test
    void avoidsGeneratedServeIdCollisionsWithOtherGraphNodes() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("target__serve", "filter", null, null,
                        Map.of("expr", "active == true"), Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", "orders", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-filter", "source", "target__serve"),
                        new PipelineDraft.Edge("filter-to-target", "target__serve", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(dagDraft(graph)).serve();
        assertThat(serve.id()).isEqualTo("target__serve_");
        assertThat(serve.sync().getFirst().rename()).isNull();
    }

    @Test
    void refusesAmbiguousDagTargetTableForMultipleSourceTables() {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("orders", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("customers", "source", "mysql", "customers", Map.of(), Map.of()),
                new PipelineDraft.Node("target", "target", "mongo", "report", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("orders-to-target", "orders", "target"),
                        new PipelineDraft.Edge("customers-to-target", "customers", "target")),
                new PipelineDraft.Viewport(0, 0, 1));

        assertThatThrownBy(() -> compiler.compile(dagDraft(graph)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target table requires exactly one upstream source table: target");
    }

    @Test
    void rejectsMalformedWizardRootsRelationsAndParentGraphs() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Wizard base = template.wizard();
        assertThatThrownBy(() -> compiler.compile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("draft");
        assertCompileFails(withWizard(template, new PipelineDraft.Root("root", " ", "orders", List.of(), List.of()),
                List.of(), List.of(), base.output()), "wizard field must be non-blank: wizard root source");
        assertCompileFails(withWizard(template, new PipelineDraft.Root("root", "crm", " ", List.of(), List.of()),
                List.of(), List.of(), base.output()), "wizard field must be non-blank: wizard root table");

        PipelineDraft.Related missingParent = related("child", "absent", PipelineDraft.Shape.OBJECT,
                "child", List.of("id"), List.of(new PipelineDraft.FieldPair("parent_id", "id")));
        assertCompileFails(withWizard(template, base.root(), List.of(missingParent), List.of(), base.output()),
                "related table parent does not exist: absent");

        PipelineDraft.Related selfParent = related("child", "child", PipelineDraft.Shape.OBJECT,
                "child", List.of("id"), List.of(new PipelineDraft.FieldPair("parent_id", "id")));
        assertCompileFails(withWizard(template, base.root(), List.of(selfParent), List.of(), base.output()),
                "wizard relation cannot point to itself: child");

        PipelineDraft.Related first = related("first", "second", PipelineDraft.Shape.OBJECT,
                "first", List.of("id"), List.of(new PipelineDraft.FieldPair("id", "id")));
        PipelineDraft.Related second = related("second", "first", PipelineDraft.Shape.OBJECT,
                "second", List.of("id"), List.of(new PipelineDraft.FieldPair("id", "id")));
        assertCompileFails(withWizard(template, base.root(), List.of(first, second), List.of(), base.output()),
                "wizard related table cycle at: first");

        PipelineDraft.Related duplicate = related("orders", "root", PipelineDraft.Shape.OBJECT,
                "orders", List.of("id"), List.of(new PipelineDraft.FieldPair("id", "id")));
        assertCompileFails(withWizard(template, base.root(), List.of(duplicate), List.of(), base.output()),
                "duplicate wizard id: orders");
    }

    @Test
    void rejectsIncompleteRelationShapesAndAssociationFields() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Root root = template.wizard().root();
        PipelineDraft.Related arrayWithoutKey = new PipelineDraft.Related("child", "orders", "crm", "child_table",
                new PipelineDraft.Relation(List.of(new PipelineDraft.FieldPair("parent_id", "id")),
                        PipelineDraft.Shape.ARRAY, "child", List.of(), List.of()), List.of());
        assertCompileFails(withWizard(template, root, List.of(arrayWithoutKey), List.of(), template.wizard().output()),
                "array relation requires an array key");

        PipelineDraft.Related flatWithoutPath = related("child", "orders", PipelineDraft.Shape.FLAT,
                "", List.of("id"), List.of(new PipelineDraft.FieldPair("parent_id", "id")));
        PipelineResource flatCompiled = compiler.compile(withWizard(template, root, List.of(flatWithoutPath),
                List.of(), template.wizard().output()));
        TransformBody.Nest flatNest = (TransformBody.Nest) ((Step.Inline) flatCompiled.transforms().getFirst()).body();
        assertThat(flatNest.root().embed().getFirst().path()).isNull();

        PipelineDraft.Related missingPath = related("child", "orders", PipelineDraft.Shape.OBJECT,
                null, List.of("id"), List.of(new PipelineDraft.FieldPair("parent_id", "id")));
        assertCompileFails(withWizard(template, root, List.of(missingPath), List.of(), template.wizard().output()),
                "object and array relations require a target path");

        PipelineDraft.Related noConditions = related("child", "orders", PipelineDraft.Shape.OBJECT,
                "child", List.of("id"), List.of());
        assertCompileFails(withWizard(template, root, List.of(noConditions), List.of(), template.wizard().output()),
                "relation requires at least one association condition");

        PipelineDraft.Related blankField = related("child", "orders", PipelineDraft.Shape.OBJECT,
                "child", List.of("id"), List.of(new PipelineDraft.FieldPair(" ", "id")));
        assertCompileFails(withWizard(template, root, List.of(blankField), List.of(), template.wizard().output()),
                "wizard field must be non-blank: related child field");
    }

    @Test
    void compilesWizardOutputAliasesAndRejectsUnsupportedModes() {
        PipelineDraft template = wizardDraft();
        PipelineDraft.Output aliasedOutput = new PipelineDraft.Output("source", Map.of(
                "source", "warehouse", "destinationTable", "archive", "write_mode", "append", "syncId", "sink"));
        ServeBlock.Inline serve = (ServeBlock.Inline) compiler.compile(withWizard(template,
                template.wizard().root(), List.of(), List.of(), aliasedOutput)).serve();
        assertThat(serve.sync().getFirst().id()).isEqualTo("sink");
        assertThat(serve.sync().getFirst().source()).isEqualTo("warehouse");
        assertThat(serve.sync().getFirst().writeMode().yaml()).isEqualTo("append");
        assertThat(serve.sync().getFirst().rename()).isNotNull();

        PipelineDraft.Output invalidMode = new PipelineDraft.Output("atlas",
                Map.of("sourceId", "atlas", "table", "archive", "writeMode", "merge"));
        assertCompileFails(withWizard(template, template.wizard().root(), List.of(), List.of(), invalidMode),
                "unsupported wizard output write mode: merge");
        assertCompileFails(withWizard(template, template.wizard().root(), List.of(), List.of(),
                new PipelineDraft.Output("source", Map.of("sourceId", "warehouse"))),
                "wizard output requires sourceId and table");
    }

    @Test
    void rejectsUnsupportedTransformsAndMalformedGraphNodes() {
        PipelineDraft template = wizardDraft();
        assertCompileFails(withWizard(template, template.wizard().root(), List.of(),
                List.of(new PipelineDraft.Transform("step", "union", Map.of())), template.wizard().output()),
                "unsupported wizard transform: union");
        assertCompileFails(withWizard(template, template.wizard().root(), List.of(),
                List.of(new PipelineDraft.Transform("step", "filter", Map.of())), template.wizard().output()),
                "transform field must be non-blank: expr");

        PipelineDraft.Graph unsupported = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("unknown", "custom", null, null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("edge", "source", "unknown")), new PipelineDraft.Viewport(0, 0, 1));
        assertCompileFails(dagDraft(unsupported), "unsupported graph node: custom");

        PipelineDraft.Graph unknownEdge = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("edge", "source", "missing")), new PipelineDraft.Viewport(0, 0, 1));
        assertCompileFails(dagDraft(unknownEdge), "graph edge references an unknown node: edge");

        PipelineDraft.Graph noInput = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("filter", "filter", null, null, Map.of("expr", "active"), Map.of())),
                List.of(), new PipelineDraft.Viewport(0, 0, 1));
        assertCompileFails(dagDraft(noInput), "graph node requires an input: filter");
    }

    @Test
    void rejectsGraphCyclesAndMultipleTerminalNodes() {
        PipelineDraft.Graph cycle = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("filter-a", "filter", null, null, Map.of("expr", "a"), Map.of()),
                new PipelineDraft.Node("filter-b", "filter", null, null, Map.of("expr", "b"), Map.of())),
                List.of(new PipelineDraft.Edge("a-b", "filter-a", "filter-b"),
                        new PipelineDraft.Edge("b-a", "filter-b", "filter-a")),
                new PipelineDraft.Viewport(0, 0, 1));
        assertCompileFails(dagDraft(cycle), "graph contains a cycle at: filter-a");

        PipelineDraft.Graph duplicateViews = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("view-a", "view", null, null, Map.of(), Map.of()),
                new PipelineDraft.Node("view-b", "view", null, null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-a", "source", "view-a"),
                        new PipelineDraft.Edge("source-b", "source", "view-b")),
                new PipelineDraft.Viewport(0, 0, 1));
        assertCompileFails(dagDraft(duplicateViews), "graph has more than one view node");

        PipelineDraft.Graph duplicateTargets = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source", "source", "mysql", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("target-a", "target", "mongo", null, Map.of(), Map.of()),
                new PipelineDraft.Node("target-b", "target", "mongo", null, Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-a", "source", "target-a"),
                        new PipelineDraft.Edge("source-b", "source", "target-b")),
                new PipelineDraft.Viewport(0, 0, 1));
        assertCompileFails(dagDraft(duplicateTargets), "graph has more than one target node");
    }

    private void assertCompileFails(PipelineDraft draft, String message) {
        assertThatThrownBy(() -> compiler.compile(draft))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static PipelineDraft withWizard(PipelineDraft template, PipelineDraft.Root root,
            List<PipelineDraft.Related> related, List<PipelineDraft.Transform> transforms,
            PipelineDraft.Output output) {
        return new PipelineDraft(template.pipelineId(), template.schemaVersion(), template.revision(),
                PipelineDraft.Mode.WIZARD, template.name(), template.description(), null,
                new PipelineDraft.Wizard(root, related, transforms, output),
                template.baseArtifactHash(), template.publishedDraftRevision(), template.publishedArtifactHash(),
                template.createdAt(), template.updatedAt(), template.updatedBy());
    }

    private static PipelineDraft.Related related(String id, String parentId, PipelineDraft.Shape shape,
            String path, List<String> keys, List<PipelineDraft.FieldPair> conditions) {
        return new PipelineDraft.Related(id, parentId, "crm", "child_table",
                new PipelineDraft.Relation(conditions, shape, path, keys,
                        shape == PipelineDraft.Shape.ARRAY ? List.of("id") : List.of()), List.of());
    }

    private static PipelineDraft dagDraft(PipelineDraft.Graph graph) {
        return new PipelineDraft("p1", 1, 1, PipelineDraft.Mode.DAG, "p1", "", graph, null,
                null, null, null, java.time.Instant.parse("2026-09-21T00:00:00Z"),
                java.time.Instant.parse("2026-09-21T00:00:00Z"), "test");
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
