package io.tapstate.control.core;

import io.tapstate.core.model.ErrorPolicy;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.SyncElement;
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
        assertThat(view.transforms()).containsExactlyElementsOf(pipeline.transforms());
        assertThat(view.view()).isEqualTo(pipeline.view());
        assertThat(view.serve()).isEqualTo(pipeline.serve());
        assertThat(view.settings()).isEqualTo(pipeline.settings());
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
                List.of("orders"),
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
                .doesNotContain("state", "failure", "metrics", "snapshot", "positions");
    }

    @Test
    void projectsNormalizedFromWiringIntoANonVisualDag() {
        PipelineResource pipeline = new PipelineResource(
                "orders_sync",
                null,
                List.of("mysql_orders", "mysql_customers"),
                List.of(
                        Step.inline(
                                "active_orders",
                                FromClause.list(FromRef.literal("orders")),
                                new TransformBody.Filter("status == 'active'"),
                                null,
                                null),
                        Step.inline(
                                "orders_with_customer",
                                FromClause.aliases(orderCustomerAliases()),
                                new TransformBody.Join("duckdb", "select * from order"),
                                null,
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
                List.of("mysql"),
                List.of(Step.inline(
                        "all_orders",
                        FromClause.list(FromRef.regex("orders_.*")),
                        new TransformBody.Union(),
                        null,
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
                List.of("mysql_feynman"),
                null,
                null,
                new ServeBlock.Inline(
                        "serve",
                        FromRef.literal("Player"),
                        List.of(new SyncElement("mongodb_player", "mongodb_target", null, null, null, null)),
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
    void appliesSyncRenameWhenNamingTheTargetTableNode() {
        PipelineResource pipeline = new PipelineResource(
                "mysql_to_mongodb",
                null,
                List.of("mysql_feynman"),
                null,
                null,
                new ServeBlock.Inline(
                        "serve",
                        FromRef.literal("Player"),
                        List.of(new SyncElement("mongodb_player", "mongodb_target", null,
                                new RenameSpec(Map.of("Player", "players_v2"), null, null, null), null, null)),
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
                List.of("mysql_feynman"),
                null,
                new ViewBlock.Inline("players_view", FromRef.literal("Player"), null, null, null),
                new ServeBlock.Inline(
                        "serve",
                        FromRef.literal("players_view"),
                        List.of(new SyncElement("mongodb_player", "mongodb_target", null, null, null, null)),
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
                sourceIds,
                List.of(Step.inline(
                        "active_orders",
                        FromClause.list(FromRef.literal("orders")),
                        new TransformBody.Filter("status == 'active'"),
                        Map.of("strict", true),
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
}
