package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiWorkspaceSnapshotTest {

    @Test
    void projectsSourcesPipelinesConnectorsAndOutputSurfaces(@TempDir Path workspace) throws Exception {
        Files.createDirectories(workspace.resolve("source"));
        Files.createDirectories(workspace.resolve("pipeline"));
        Files.writeString(workspace.resolve("source/src_orders.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                config: {}
                mode: cdc
                tables: [orders]
                """);
        Files.writeString(workspace.resolve("pipeline/orders.tap.yml"), """
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: src_orders
                view:
                  id: orders_view
                  from: /.*/
                  primary_key: id
                serve:
                  id: serve
                  from: orders_view
                  sync:
                    - id: target
                      source: src_orders
                """);

        List<TuiDashboard.ResourceSummary> summaries = TuiWorkspaceSnapshot.scan(workspace);

        assertThat(summaries).extracting(TuiDashboard.ResourceSummary::kind)
                .containsExactly("source", "pipeline");
        TuiDashboard.ResourceSummary source = summaries.getFirst();
        assertThat(source.id()).isEqualTo("src_orders");
        assertThat(source.connector()).isEqualTo("mysql");
        assertThat(source.detail()).isEqualTo("mysql · cdc");
        assertThat(source.readable()).isTrue();
        TuiDashboard.ResourceSummary pipeline = summaries.get(1);
        assertThat(pipeline.id()).isEqualTo("orders");
        assertThat(pipeline.detail()).isEqualTo("1 source · view · serve");
    }

    @Test
    void keepsUnreadableAndMisplacedArtifactsVisible(@TempDir Path workspace) throws Exception {
        Files.createDirectories(workspace.resolve("source"));
        Files.createDirectories(workspace.resolve("pipeline"));
        Files.writeString(workspace.resolve("source/broken.tap.yml"), "[unterminated\n");
        Files.writeString(workspace.resolve("pipeline/stray.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: stray_source
                connector: mysql
                mode: cdc
                """);

        List<TuiDashboard.ResourceSummary> summaries = TuiWorkspaceSnapshot.scan(workspace);

        assertThat(summaries).hasSize(2);
        assertThat(summaries.get(0).id()).isEqualTo("broken");
        assertThat(summaries.get(0).readable()).isFalse();
        assertThat(summaries.get(0).detail()).isEqualTo("unreadable");
        assertThat(summaries.get(1).kind()).isEqualTo("pipeline");
        assertThat(summaries.get(1).id()).isEqualTo("stray_source");
        assertThat(summaries.get(1).misplaced()).isTrue();
        assertThat(summaries.get(1).detail()).contains("declares 'source'");
        assertThat(summaries.get(1).connector()).isNull();
    }
}
