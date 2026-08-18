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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
}
