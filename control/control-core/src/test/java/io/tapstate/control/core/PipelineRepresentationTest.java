package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ErrorPolicy;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.NestOrder;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewSchema;
import io.tapstate.core.model.WriteMode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineRepresentationTest {

    private final PipelineRepresentation representation = new PipelineRepresentation();

    @Test
    void mapsTheCompleteEditorPayloadToTheCanonicalPipelineModel() {
        Map<String, Object> mapFields = new LinkedHashMap<>();
        mapFields.put("removed", false);
        mapFields.put("customer_id", "$id");
        mapFields.put("total", "=price * quantity");
        mapFields.put("region", Map.of("code", "EU", "priority", List.of(1, 2)));

        Map<String, Object> nestRoot = Map.of(
                "from", "orders",
                "key", List.of("id"),
                "mode", "upsert",
                "track_key_changes", true,
                "embed", List.of(Map.of(
                        "from", "lines",
                        "on", Map.of("order_id", "id"),
                        "as", "array",
                        "path", "lines",
                        "array_key", List.of("line_id"),
                        "ignore_updates", false,
                        "track_key_changes", true,
                        "embed", List.of(Map.of(
                                "from", "products",
                                "on", Map.of("sku", "sku"),
                                "as", "object",
                                "path", "product")))));

        PipelineInput input = new PipelineInput(
                "orders_sync",
                new Metadata(Map.of("team", "analytics"), "Orders"),
                List.of("mysql_orders", "postgres_customers"),
                List.of(
                        Map.of(
                                "id", "shared_cleanup",
                                "use", "cleanup",
                                "from", "orders"),
                        Map.of(
                                "id", "scripted",
                                "type", "js",
                                "from", List.of("shared_cleanup"),
                                "body", Map.of("script", "return event;"),
                                "experimental", Map.of("preview", true)),
                        Map.of(
                                "id", "projected",
                                "type", "map",
                                "from", Map.of("refs", List.of(Map.of("ref", "scripted"))),
                                "fields", mapFields),
                        Map.of(
                                "id", "active",
                                "type", "filter",
                                "from", "/orders_.*/",
                                "expr", "status == 'active'"),
                        Map.of(
                                "id", "combined",
                                "type", "union",
                                "from", List.of("active", Map.of("pattern", "archive_.*"))),
                        Map.of(
                                "id", "nested",
                                "type", "nest",
                                "from", Map.of("orders", "combined", "lines", "order_lines", "products", "products"),
                                "primaryKey", "id",
                                "order", "sub_first",
                                "entriesInMemory", 250,
                                "max_elements_per_document", 500,
                                "root", nestRoot),
                        Map.of(
                                "id", "joined",
                                "type", "join",
                                "from", Map.of("orders", "nested", "customers", "customers"),
                                "engine", "BUILTIN",
                                "sql", "select * from orders join customers")),
                Map.of(
                        "from", Map.of("ref", "joined"),
                        "primaryKey", "id",
                        "storage", Map.of(
                                "hot", Map.of("ttl", "15m"),
                                "warm", Map.of("collection", "orders", "indexes", List.of("customer_id")),
                                "cold", Map.of("partitionBy", List.of("region"))),
                        "schema", Map.of("enforce", true, "evolution", "additive")),
                Map.of(
                        "from", List.of("view", "/backfill_.*/"),
                        "sync", List.of(Map.of(
                                "id", "warehouse",
                                "source", "mongodb_target",
                                "write_mode", "append",
                                "rename", Map.of(
                                        "map", Map.of("orders", "orders_v2"),
                                        "case", "upper",
                                        "prefix", "tap_",
                                        "suffix", "_archive"),
                                "ddl", "apply")),
                        "query", List.of(
                                Map.of("type", "rest", "backend", "warehouse"),
                                Map.of("type", "MCP")),
                        "push", List.of(
                                Map.of(
                                        "id", "events",
                                        "source", "kafka_target",
                                        "topic", "orders",
                                        "format", "=event.after"),
                                Map.of(
                                        "source", "audit_target",
                                        "format", Map.of("id", "$order_id", "deleted", false)))),
                Map.of(
                        "errorPolicy", "dead_letter",
                        "batch_size", 500,
                        "parallelism", 4,
                        "schedule", "0 2 * * *",
                        "readMode", "CDC_ONLY",
                        "start_from", "earliest"),
                Map.of("preview", List.of("orders")));

        PipelineResource model = representation.toModel(input, null);

        assertThat(model.id()).isEqualTo("orders_sync");
        assertThat(model.sources()).containsExactly(
                SourceRef.bare("mysql_orders"), SourceRef.bare("postgres_customers"));
        assertThat(model.transforms()).containsExactly(
                Step.use("shared_cleanup", "cleanup", FromClause.list(FromRef.literal("orders"))),
                Step.inline("scripted", FromClause.list(FromRef.literal("shared_cleanup")),
                        new TransformBody.Js("return event;"), Map.of("preview", true)),
                Step.inline("projected", FromClause.list(FromRef.literal("scripted")),
                        new TransformBody.MapProjection(Map.of(
                                "removed", FieldRule.drop(),
                                "customer_id", FieldRule.rename("id"),
                                "total", FieldRule.computed("price * quantity"),
                                "region", FieldRule.literal(Map.of("code", "EU", "priority", List.of(1, 2))))), null),
                Step.inline("active", FromClause.list(FromRef.regex("orders_.*")),
                        new TransformBody.Filter("status == 'active'"), null),
                Step.inline("combined", FromClause.list(FromRef.literal("active"), FromRef.regex("archive_.*")),
                        new TransformBody.Union(), null),
                Step.inline("nested", FromClause.aliases(Map.of(
                                "orders", FromRef.literal("combined"),
                                "lines", FromRef.literal("order_lines"),
                                "products", FromRef.literal("products"))),
                        new TransformBody.Nest(
                                "id", NestOrder.SUB_FIRST, 250, 500,
                                new NestRoot(
                                        "orders", List.of("id"), "upsert", true,
                                        List.of(new Embed(
                                                "lines", Map.of("order_id", "id"), EmbedAs.ARRAY, "lines",
                                                List.of("line_id"), false, true,
                                                List.of(new Embed(
                                                        "products", Map.of("sku", "sku"), EmbedAs.OBJECT,
                                                        "product", null, null, null, null)))))), null),
                Step.inline("joined", FromClause.aliases(Map.of(
                                "orders", FromRef.literal("nested"),
                                "customers", FromRef.literal("customers"))),
                        new TransformBody.Join(JoinEngine.BUILTIN, "select * from orders join customers"), null));
        assertThat(model.view()).isEqualTo(new ViewBlock.Inline(
                "view",
                FromRef.literal("joined"),
                "id",
                new Storage(
                        new Storage.Hot("15m"),
                        new Storage.Warm("orders", List.of("customer_id")),
                        new Storage.Cold(List.of("region"))),
                new ViewSchema(true, "additive")));
        assertThat(model.serve()).isEqualTo(new ServeBlock.Inline(
                "serve",
                FromClause.list(FromRef.literal("view"), FromRef.regex("backfill_.*")),
                List.of(new SyncElement(
                        "warehouse",
                        "mongodb_target",
                        WriteMode.APPEND,
                        new RenameSpec(
                                Map.of("orders", "orders_v2"), RenameCase.UPPER, "tap_", "_archive"),
                        DdlPolicy.APPLY)),
                List.of(new QueryElement(QueryType.REST, "warehouse"), new QueryElement(QueryType.MCP, null)),
                List.of(
                        new PushElement(
                                "events", "kafka_target", "orders", PushFormat.cel("event.after")),
                        new PushElement(
                                null,
                                "audit_target",
                                null,
                                PushFormat.fields(Map.of(
                                        "id", FieldRule.rename("order_id"),
                                        "deleted", FieldRule.drop()))))));
        assertThat(model.settings()).isEqualTo(new Settings(
                ErrorPolicy.DEAD_LETTER, 500, 4, "0 2 * * *", ReadMode.CDC_ONLY, "earliest"));
        assertThat(model.experimental()).isEqualTo(Map.of("preview", List.of("orders")));

        PipelineView projected = representation.toView(
                model,
                "d".repeat(64),
                List.of(
                        new PipelineSourceSummary("mysql_orders", null, "mysql"),
                        new PipelineSourceSummary("postgres_customers", null, "postgres")));
        PipelineResource roundTripped = representation.toModel(new PipelineInput(
                projected.id(),
                projected.metadata(),
                new ArrayList<Object>(projected.sources()),
                projected.transforms(),
                projected.view(),
                projected.serve(),
                projected.settings(),
                projected.experimental()), model);

        assertThat(roundTripped).isEqualTo(model);
    }

    @Test
    void mapsReusableViewAndServeBlocksAndPreservesTheirDefaultIds() {
        PipelineInput input = new PipelineInput(
                "orders_sync",
                null,
                List.of("orders"),
                null,
                Map.of("use", "shared_view", "from", "/orders_.*/"),
                Map.of("use", "shared_api", "from", "shared_view"),
                null,
                null);

        PipelineResource model = representation.toModel(input, null);

        assertThat(model.view()).isEqualTo(new ViewBlock.Use(
                null, "shared_view", FromRef.regex("orders_.*")));
        assertThat(model.serve()).isEqualTo(new ServeBlock.Use(
                null, "shared_api", FromRef.literal("shared_view")));
    }

    @Test
    void rejectsAliasMapsForServeFromBecauseCanonicalDslOnlyAcceptsAFlow() {
        PipelineInput input = new PipelineInput(
                "orders_sync", null, List.of("orders"), null, null,
                Map.of("from", Map.of("left", "orders")), null, null);

        assertThatThrownBy(() -> representation.toModel(input, null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error.args()).containsEntry("reason", "serve.from must be a string or list");
                });
    }

    @Test
    void translatesModelInvariantFailuresIntoTheCodedMalformedRequest() {
        PipelineInput input = new PipelineInput(
                "invalid", null, List.of(),
                List.of(Map.of(
                        "id", "nested",
                        "type", "nest",
                        "from", List.of("orders"),
                        "root", Map.of("from", "orders"))),
                null, null, null, null);

        assertThatThrownBy(() -> representation.toModel(input, null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error.args()).containsKey("reason");
                });
    }

    /**
     * The two directions of this face are written separately and only one of them is checked by the
     * compiler. What a body becomes on the way out is an exhaustive switch over the kinds, so a kind
     * added to the grammar stops the build until somebody answers for it; what a body is read back
     * from is a switch over the type name as text, which compiles whatever it does not cover and
     * refuses that type at runtime. So a kind can be wholly present outbound and wholly absent
     * inbound, and the shape that takes is an editor that displays a pipeline it then cannot save.
     */
    @Test
    void readsBackAnExpansionItJustWroteOut() {
        PipelineResource pipeline = expanding();

        PipelineView view = representation.toView(
                pipeline, "e".repeat(64), List.of(new PipelineSourceSummary("orders", null, "mysql")));

        assertThat(view.transforms()).singleElement().satisfies(step -> {
            assertThat(step).containsEntry("type", "unwind");
            assertThat(step).containsEntry("path", "items");
            assertThat(step).containsEntry("elementKey", "sku");
        });
        PipelineResource roundTripped = representation.toModel(new PipelineInput(
                view.id(), view.metadata(), new ArrayList<Object>(view.sources()), view.transforms(),
                view.view(), view.serve(), view.settings(), view.experimental()), pipeline);

        assertThat(roundTripped).isEqualTo(pipeline);
    }

    /**
     * The keys arrive in whichever spelling the caller used - this face answers in one and every
     * other body here accepts both, so an expansion accepting only one would be the odd one out in
     * a way nothing points at.
     */
    @Test
    void readsAnExpansionWrittenInEitherSpelling() {
        Map<String, Object> underscored = new LinkedHashMap<>();
        underscored.put("id", "explode");
        underscored.put("type", "unwind");
        underscored.put("from", List.of("orders"));
        underscored.put("path", "items");
        underscored.put("include_array_index", "item_no");
        underscored.put("preserve_null_and_empty_arrays", true);
        underscored.put("element_key", "sku");
        underscored.put("element_type", "json");

        PipelineResource model = representation.toModel(new PipelineInput(
                "orders_sync", null, new ArrayList<Object>(List.of("orders")), List.of(underscored),
                null, null, null, null), expanding());

        assertThat(model.transforms()).singleElement().satisfies(step ->
                assertThat(((Step.Inline) step).body())
                        .isEqualTo(new TransformBody.Unwind("items", "item_no", true, "sku", "json")));
    }

    /** One source, one expansion, nothing else - the smallest artifact that carries one. */
    private static PipelineResource expanding() {
        return new PipelineResource(
                "orders_sync",
                new Metadata(Map.of(), "Order lines"),
                List.of(SourceRef.bare("orders")),
                List.of(Step.inline(
                        "explode",
                        FromClause.list(FromRef.literal("orders")),
                        new TransformBody.Unwind("items", "item_no", true, "sku", "json"),
                        null)),
                null,
                new ServeBlock.Use("lines_api", "warehouse_api", FromRef.literal("explode")),
                null,
                null);
    }

    @Test
    void mapsTheStaticPipelineArtifactAndItsReferencedSourceSummaries() {
        PipelineResource pipeline = pipeline(List.of("orders", "customers"));

        PipelineView view = representation.toView(
                pipeline,
                "a".repeat(64),
                List.of(
                        new PipelineSourceSummary(
                                "orders", new Metadata(Map.of("team", "sales"), "Orders"), "mysql"),
                        new PipelineSourceSummary(
                                "customers", new Metadata(Map.of(), "Customers"), "postgres")));

        assertThat(view.id()).isEqualTo("orders_sync");
        assertThat(view.metadata()).isEqualTo(new Metadata(Map.of("team", "analytics"), "Orders to warehouse"));
        assertThat(view.sources()).containsExactly(
                new PipelineSourceSummary(
                        "orders", new Metadata(Map.of("team", "sales"), "Orders"), "mysql"),
                new PipelineSourceSummary(
                        "customers", new Metadata(Map.of(), "Customers"), "postgres"));
        assertThat(view.transforms()).singleElement().satisfies(step -> {
            assertThat(step).containsEntry("id", "active_orders");
            assertThat(step).containsEntry("type", "filter");
            assertThat(step).containsEntry("expr", "status == 'active'");
        });
        PipelineResource roundTripped = representation.toModel(new PipelineInput(
                view.id(),
                view.metadata(),
                new ArrayList<Object>(view.sources()),
                view.transforms(),
                view.view(),
                view.serve(),
                view.settings(),
                view.experimental()), pipeline);
        assertThat(roundTripped).isEqualTo(pipeline);
        assertThat(view.experimental()).isEqualTo(Map.of("preview", List.of("orders")));
        assertThat(view.contentHash()).isEqualTo("a".repeat(64));
    }

    @Test
    void defensivelyCopiesSourceSummariesAndExperimentalJsonValues() {
        List<PipelineSourceSummary> sources = new ArrayList<>(List.of(
                new PipelineSourceSummary("orders", null, "mysql")));
        List<Object> preview = new ArrayList<>(List.of("first"));
        PipelineResource pipeline = new PipelineResource(
                "orders_sync",
                null,
                refs("orders"),
                null,
                null,
                null,
                null,
                Map.of("preview", preview));

        PipelineView view = representation.toView(pipeline, "b".repeat(64), sources);
        sources.clear();
        preview.add("late");

        assertThat(view.sources()).containsExactly(new PipelineSourceSummary("orders", null, "mysql"));
        assertThat(view.experimental()).containsEntry("preview", List.of("first"));
        assertThatThrownBy(() -> view.sources().add(new PipelineSourceSummary("other", null, "mysql")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((List<Object>) view.experimental().get("preview")).add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsMissingOrReorderedSourceSummariesInsteadOfSilentlyDroppingReferences() {
        PipelineResource pipeline = pipeline(List.of("orders", "customers"));

        assertThatThrownBy(() -> representation.toView(
                pipeline,
                "c".repeat(64),
                List.of(new PipelineSourceSummary("orders", null, "mysql"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source summaries");

        assertThatThrownBy(() -> representation.toView(
                pipeline,
                "c".repeat(64),
                List.of(
                        new PipelineSourceSummary("customers", null, "postgres"),
                        new PipelineSourceSummary("orders", null, "mysql"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source summaries");
    }

    @Test
    void keepsRuntimeObservationOutOfTheStaticArtifactView() {
        assertThat(Arrays.stream(PipelineView.class.getRecordComponents())
                .map(component -> component.getName())
                .toList())
                .isNotEmpty()
                .doesNotContain("state", "failure", "metrics", "snapshot", "positions");
    }

    @Test
    void projectsNormalizedFromWiringIntoANonVisualDag() {
        PipelineResource pipeline = new PipelineResource(
                "orders_sync",
                null,
                refs("mysql_orders", "mysql_customers"),
                List.of(
                        Step.inline(
                                "active_orders",
                                FromClause.list(FromRef.literal("orders")),
                                new TransformBody.Filter("status == 'active'"),
                                null),
                        Step.inline(
                                "orders_with_customer",
                                FromClause.aliases(orderCustomerAliases()),
                                new TransformBody.Join(JoinEngine.BUILTIN, "select * from order"),
                                null)),
                new ViewBlock.Use("warehouse_orders", "warehouse_orders", FromRef.literal("orders_with_customer")),
                new ServeBlock.Use("orders_api", "orders_api", FromRef.literal("warehouse_orders")),
                null,
                null);

        PipelineView view = representation.toView(
                pipeline,
                "d".repeat(64),
                List.of(
                        new PipelineSourceSummary("mysql_orders", null, "mysql"),
                        new PipelineSourceSummary("mysql_customers", null, "mysql")));

        assertThat(view.dag().nodes()).containsExactly(
                new PipelineDagNode("source:orders", "source", "orders", null),
                new PipelineDagNode("source:customers", "source", "customers", null),
                new PipelineDagNode("transform:active_orders", "transform", "active_orders", "filter"),
                new PipelineDagNode("transform:orders_with_customer", "transform",
                        "orders_with_customer", "join"),
                new PipelineDagNode("view:warehouse_orders", "view", "warehouse_orders", null));
        assertThat(view.dag().edges()).containsExactly(
                new PipelineDagEdge("source:orders->transform:active_orders", "source:orders", "transform:active_orders", null),
                new PipelineDagEdge("transform:active_orders->transform:orders_with_customer:order",
                        "transform:active_orders", "transform:orders_with_customer", "order"),
                new PipelineDagEdge("source:customers->transform:orders_with_customer:customer",
                        "source:customers", "transform:orders_with_customer", "customer"),
                new PipelineDagEdge("transform:orders_with_customer->view:warehouse_orders",
                        "transform:orders_with_customer", "view:warehouse_orders", null));
    }

    @Test
    void preservesRegexReferencesAsSourceNodesRatherThanPretendingTheyAreStaticTables() {
        PipelineResource pipeline = new PipelineResource(
                "orders_sync",
                null,
                refs("mysql"),
                List.of(Step.inline(
                        "all_orders",
                        FromClause.list(FromRef.regex("orders_.*")),
                        new TransformBody.Union(),
                        null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("all_orders"), null, null, null),
                null,
                null);

        PipelineView view = representation.toView(
                pipeline,
                "e".repeat(64),
                List.of(new PipelineSourceSummary("mysql", null, "mysql")));

        assertThat(view.dag().nodes()).contains(
                new PipelineDagNode("source:mysql:/orders_.*/", "source", "/orders_.*/", "mysql"));
        assertThat(view.dag().edges()).contains(
                new PipelineDagEdge("source:mysql:/orders_.*/->transform:all_orders",
                        "source:mysql:/orders_.*/", "transform:all_orders", null));
    }

    @Test
    void projectsSyncAsADirectSourceTableToTargetTableFlowWithoutAServeNode() {
        PipelineResource pipeline = new PipelineResource(
                "mysql_to_mongodb",
                null,
                refs("mysql_feynman"),
                null,
                null,
                new ServeBlock.Inline(
                        "serve",
                        FromRef.literal("Player"),
                        List.of(new SyncElement("mongodb_player", "mongodb_target", null, null, null)),
                        null,
                        null),
                null,
                null);

        PipelineView view = representation.toView(
                pipeline,
                "f".repeat(64),
                List.of(new PipelineSourceSummary("mysql_feynman", null, "mysql")));

        assertThat(view.dag().nodes()).containsExactly(
                new PipelineDagNode("source:mysql_feynman:Player", "source", "Player", "mysql_feynman"),
                new PipelineDagNode("target:mongodb_target:Player", "target", "Player", "mongodb_target"));
        assertThat(view.dag().edges()).containsExactly(
                new PipelineDagEdge("source:mysql_feynman:Player->target:mongodb_target:Player:mongodb_player",
                        "source:mysql_feynman:Player", "target:mongodb_target:Player", "mongodb_player"));
    }

    @Test
    void projectsExplicitMultiTableServeWiringAsOneTargetWithStableTableRefs() {
        PipelineResource pipeline = new PipelineResource(
                "mysql_to_mongodb",
                null,
                refs("mysql_feynman"),
                null,
                null,
                new ServeBlock.Inline(
                        "serve",
                        FromClause.list(
                                FromRef.literal("mysql_feynman.Player"),
                                FromRef.literal("mysql_feynman.PlayerAddress")),
                        List.of(new SyncElement("mongodb_players", "mongodb_target", null, null, null)),
                        null,
                        null),
                null,
                null);

        PipelineView view = representation.toView(
                pipeline,
                "m".repeat(64),
                List.of(new PipelineSourceSummary("mysql_feynman", null, "mysql")));

        assertThat(view.dag().nodes()).containsExactly(
                new PipelineDagNode("source:mysql_feynman:Player", "source", "Player", "mysql_feynman"),
                new PipelineDagNode("source:mysql_feynman:PlayerAddress", "source", "PlayerAddress", "mysql_feynman"),
                new PipelineDagNode("target:mongodb_target:Player", "target", "Player", "mongodb_target",
                        List.of("Player", "PlayerAddress")));
        assertThat(view.dag().edges()).containsExactly(
                new PipelineDagEdge(
                        "source:mysql_feynman:Player->target:mongodb_target:Player:mongodb_players",
                        "source:mysql_feynman:Player", "target:mongodb_target:Player", "mongodb_players"),
                new PipelineDagEdge(
                        "source:mysql_feynman:PlayerAddress->target:mongodb_target:Player:mongodb_players",
                        "source:mysql_feynman:PlayerAddress", "target:mongodb_target:Player", "mongodb_players"));
    }

    @Test
    void appliesSyncRenameWhenNamingTheTargetTableNode() {
        PipelineResource pipeline = new PipelineResource(
                "mysql_to_mongodb",
                null,
                refs("mysql_feynman"),
                null,
                null,
                new ServeBlock.Inline(
                        "serve",
                        FromRef.literal("Player"),
                        List.of(new SyncElement("mongodb_player", "mongodb_target", null,
                                new RenameSpec(Map.of("Player", "players_v2"), null, null, null), null)),
                        null,
                        null),
                null,
                null);

        PipelineView view = representation.toView(
                pipeline,
                "g".repeat(64),
                List.of(new PipelineSourceSummary("mysql_feynman", null, "mysql")));

        assertThat(view.dag().nodes()).containsExactly(
                new PipelineDagNode("source:mysql_feynman:Player", "source", "Player", "mysql_feynman"),
                new PipelineDagNode("target:mongodb_target:players_v2", "target", "players_v2", "mongodb_target"));
        assertThat(view.dag().edges()).containsExactly(
                new PipelineDagEdge("source:mysql_feynman:Player->target:mongodb_target:players_v2:mongodb_player",
                        "source:mysql_feynman:Player", "target:mongodb_target:players_v2", "mongodb_player"));
    }

    @Test
    void connectsASyncTargetToTheViewItServesInsteadOfInventingASourceOrServeNode() {
        PipelineResource pipeline = new PipelineResource(
                "mysql_to_mongodb",
                null,
                refs("mysql_feynman"),
                null,
                new ViewBlock.Inline("players_view", FromRef.literal("Player"), null, null, null),
                new ServeBlock.Inline(
                        "serve",
                        FromRef.literal("players_view"),
                        List.of(new SyncElement("mongodb_player", "mongodb_target", null, null, null)),
                        null,
                        null),
                null,
                null);

        PipelineView view = representation.toView(
                pipeline,
                "h".repeat(64),
                List.of(new PipelineSourceSummary("mysql_feynman", null, "mysql")));

        assertThat(view.dag().nodes()).containsExactly(
                new PipelineDagNode("source:mysql_feynman:Player", "source", "Player", "mysql_feynman"),
                new PipelineDagNode("view:players_view", "view", "players_view", null),
                new PipelineDagNode("target:mongodb_target:players_view", "target", "players_view", "mongodb_target"));
        assertThat(view.dag().edges()).containsExactly(
                new PipelineDagEdge("source:mysql_feynman:Player->view:players_view",
                        "source:mysql_feynman:Player", "view:players_view", null),
                new PipelineDagEdge("view:players_view->target:mongodb_target:players_view:mongodb_player",
                        "view:players_view", "target:mongodb_target:players_view", "mongodb_player"));
    }

    private static PipelineResource pipeline(List<String> sourceIds) {
        return new PipelineResource(
                "orders_sync",
                new Metadata(Map.of("team", "analytics"), "Orders to warehouse"),
                sourceIds.stream().map(id -> (SourceRef) SourceRef.bare(id)).toList(),
                List.of(Step.inline(
                        "active_orders",
                        FromClause.list(FromRef.literal("orders")),
                        new TransformBody.Filter("status == 'active'"),
                        Map.of("preview", false))),
                new ViewBlock.Use("orders_view", "warehouse_orders", FromRef.literal("active_orders")),
                new ServeBlock.Use("orders_api", "warehouse_api", FromRef.literal("orders_view")),
                new Settings(
                        ErrorPolicy.DEAD_LETTER, 500, 2, "0 2 * * *", ReadMode.CDC_ONLY, "earliest"),
                Map.of("preview", List.of("orders")));
    }

    private static Map<String, FromRef> orderCustomerAliases() {
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("active_orders"));
        aliases.put("customer", FromRef.literal("customers"));
        return aliases;
    }

    private static List<SourceRef> refs(String... ids) {
        return Arrays.stream(ids).map(id -> (SourceRef) SourceRef.bare(id)).toList();
    }
}
