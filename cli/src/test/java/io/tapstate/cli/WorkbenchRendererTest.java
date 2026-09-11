package io.tapstate.cli;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Modifier;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.CharWidth;
import dev.tamboui.tui.event.KeyEvent;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchRendererTest {

    @Test
    void tooSmallFrameShowsOnlyLiveAndRequiredDimensions() {
        Rendered rendered = render(83, 53, accepted(snapshot(new WorkbenchRemoteState.Available(0), List.of())));

        assertThat(rendered.text())
                .contains("Terminal size too small:")
                .contains("Width = 83  Height = 53")
                .contains("Needed for current config:")
                .contains("Width = 88  Height = 24")
                .doesNotContain("TapState", "Overview", "q quit");
        assertThat(rendered.layout().tooSmall()).isTrue();
        assertThat(rendered.layout().visibleRowCapacity()).isZero();
        assertThat(rendered.layout().tabAt(0, 0)).isEmpty();
        assertThat(rendered.layout().rowAt(0, 0)).isEmpty();
    }

    @Test
    void frameBelowMinimumHeightAlsoUsesTheTooSmallLayout() {
        Rendered rendered = render(88, 23, WorkbenchState.initial());

        assertThat(rendered.text())
                .contains("Width = 88  Height = 23")
                .contains("Width = 88  Height = 24")
                .doesNotContain("TapState");
        assertThat(rendered.layout().tooSmall()).isTrue();
    }

    @Test
    void minimumFrameRendersSanitizedHeaderTabsOverviewNotificationAndCurrentFooter() {
        String secret = "uri-password-token-31ac";
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                Path.of("/work/catalog"),
                Optional.of("dev"),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.SIGNED_IN,
                Optional.of("alice"),
                Optional.of(URI.create("https://alice:" + secret + "@tapstate.example:9443/admin?token=" + secret)),
                "tapstate test");
        WorkbenchSnapshot snapshot = snapshot(
                session,
                new WorkbenchRemoteState.Available(2),
                List.of(row("source", "orders", WorkbenchAlignment.IN_SYNC, "source/orders.tap.yml", true),
                        row("pipeline", "daily", WorkbenchAlignment.LOCAL_ONLY, "pipeline/daily.tap.yml", false)));

        Rendered rendered = render(88, 24, accepted(snapshot));

        assertThat(rendered.text())
                .contains("Tapstate", "ctx: dev", "connected", "auth: alice", "workspace: /work/catalog")
                .contains("1 Overview", "2 Workspace", "3 Sources", "4 Pipelines", "0 More")
                .contains("Resources")
                .contains("source", "pipeline", "in sync 1", "local only 1")
                .contains("1-4  views", "c  context", "a  auth", "0  more", "r  refresh", "q  quit")
                .doesNotContain(secret, "/admin", "explicit", "Remote artifacts:",
                        "F1", "F2", "command palette",
                        "Up/Down select", "[2]", "[1]", "[0]");
        assertThat(rendered.layout().tooSmall()).isFalse();
        assertThat(rendered.layout().wide()).isFalse();
        assertThat(rendered.layout().visibleRowCapacity()).isPositive();
    }

    @Test
    void frameUsesCamelHeaderTabStatisticsEmojiAndBorderedContentGeometry() {
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                Path.of("/work/catalog"),
                Optional.of("dev"),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.SIGNED_IN,
                Optional.of("alice"),
                Optional.of(URI.create("https://tapstate.example:9443")),
                "v0.4.4");
        WorkbenchSnapshot snapshot = snapshot(
                session,
                new WorkbenchRemoteState.Available(2),
                List.of(row("source", "orders", WorkbenchAlignment.IN_SYNC, "source/orders.tap.yml", true),
                        row("pipeline", "daily", WorkbenchAlignment.LOCAL_ONLY, "pipeline/daily.tap.yml", false)));

        Rendered rendered = render(100, 24, accepted(snapshot));

        String header = lineOf(rendered.buffer(), 0);
        assertThat(header)
                .contains("Tapstate", "ctx: dev", "connected", "workspace: /work/catalog", "auth: alice");
        assertThat(header.indexOf("ctx: dev")).isLessThan(header.indexOf("connected"));
        assertThat(header.indexOf("connected")).isLessThan(header.indexOf("workspace: /work/catalog"));
        assertThat(header.indexOf("workspace: /work/catalog")).isLessThan(header.indexOf("auth: alice"));
        assertThat(lineOf(rendered.buffer(), 1)).contains("(2)", "(1)", "(0)");
        assertThat(lineOf(rendered.buffer(), 2))
                .contains("🌊", "1 Overview", "💻", "2 Workspace", "🔌", "3 Sources",
                        "🔀", "4 Pipelines", "📂", "0 More ▾")
                .doesNotContain("[2]", "[1]", "[0]");
        assertThat(lineOf(rendered.buffer(), 3)).startsWith("╭").contains(" Overview ");
        assertThat(lineOf(rendered.buffer(), 22)).startsWith("╰");
        assertThat(lineOf(rendered.buffer(), 23)).contains("c  context", "a  auth", "q  quit");
        assertThat(rendered.layout().actionHits())
                .extracting(hit -> hit.launcher().name())
                .contains("CONTEXT", "AUTH", "MORE");
    }

    @Test
    void widthBoundarySelectsCompactAndWideTableBranchesWithoutOverflow() {
        WorkbenchState state = accepted(snapshot(
                        new WorkbenchRemoteState.Available(1),
                List.of(row("source", "orders", WorkbenchAlignment.IN_SYNC,
                                "source/a-very-long-directory/orders.tap.yml", true))))
                .select(WorkbenchState.WorkbenchTab.SOURCES);

        Rendered compact = render(156, 24, state);
        Rendered wide = render(157, 24, state);

        assertThat(compact.layout().wide()).isFalse();
        assertThat(wide.layout().wide()).isTrue();
        assertThat(compact.text())
                .contains("IDENTIFIER▼", "STATE", "WORKSPACE")
                .doesNotContain("KIND", "REMOTE");
        assertThat(wide.text())
                .contains("IDENTIFIER▼", "| STATE", "| WORKSPACE")
                .doesNotContain("KIND", "REMOTE");
        assertOccupiedToRightEdge(compact);
        assertOccupiedToRightEdge(wide);
    }

    @Test
    void everyTabRendersOnlyItsSnapshotProjection() {
        List<WorkbenchArtifactRow> rows = List.of(
                row("source", "customer-source", WorkbenchAlignment.IN_SYNC, "source/customer.tap.yml", true),
                row("pipeline", "billing-pipeline", WorkbenchAlignment.DRIFTED, "pipeline/billing.tap.yml", true),
                row("view", "operations-view", WorkbenchAlignment.LOCAL_ONLY, "view/operations.tap.yml", false));
        WorkbenchSnapshot snapshot = snapshot(new WorkbenchRemoteState.Available(2), rows);

        assertThat(render(100, 28, accepted(snapshot)).text())
                .contains("Resources", "source", "pipeline", "drifted 1")
                .doesNotContain("customer-source", "billing-pipeline", "operations-view");
        assertThat(render(100, 28, accepted(snapshot).select(WorkbenchState.WorkbenchTab.WORKSPACE)).text())
                .contains("customer.tap.yml", "billing.tap.yml", "Path: pipeline/billing.tap.yml")
                .doesNotContain("🔌 source/customer.tap.yml", "🔀 pipeline/billing.tap.yml")
                .doesNotContain("operations-view");
        assertThat(render(100, 28, accepted(snapshot).select(WorkbenchState.WorkbenchTab.SOURCES)).text())
                .contains("customer-source")
                .doesNotContain("billing-pipeline", "operations-view");
        assertThat(render(100, 28, accepted(snapshot).select(WorkbenchState.WorkbenchTab.PIPELINES)).text())
                .contains("billing-pipeline")
                .doesNotContain("customer-source", "operations-view");
    }

    @Test
    void overviewMakesTheDemoAndExistingServerPathsLegibleForFirstRun() {
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                Path.of("/work/empty"),
                Optional.empty(),
                Optional.empty(),
                WorkbenchConnection.NO_CONTEXT,
                WorkbenchAuthentication.NOT_APPLICABLE,
                Optional.empty(),
                Optional.empty(),
                "v0.4.4");
        WorkbenchSnapshot snapshot = snapshot(session, new WorkbenchRemoteState.NotConfigured(), List.of());

        assertThat(render(100, 28, accepted(snapshot)).text())
                .contains("No Pipeline Activity Found", "How to get started:",
                        "Run the guided demo workspace:", "> tapstate demo -w demo", "> cd demo && tapstate",
                        "Or connect an existing Tapstate Server:",
                        "create or choose a context", "sign in when the context is selected");
    }

    @Test
    void sourceAndPipelineRowsShowCamelSelectionMarkerAfterKeyboardNavigation() {
        List<WorkbenchArtifactRow> rows = List.of(
                row("source", "alpha", WorkbenchAlignment.IN_SYNC, "source/alpha.tap.yml", true),
                row("source", "beta", WorkbenchAlignment.IN_SYNC, "source/beta.tap.yml", true),
                row("pipeline", "daily", WorkbenchAlignment.IN_SYNC, "pipeline/daily.tap.yml", true),
                row("pipeline", "nightly", WorkbenchAlignment.IN_SYNC, "pipeline/nightly.tap.yml", true));
        WorkbenchSnapshot snapshot = snapshot(new WorkbenchRemoteState.Available(4), rows);

        WorkbenchState sources = accepted(snapshot)
                .select(WorkbenchState.WorkbenchTab.SOURCES)
                .reduce(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.DOWN), 10);
        WorkbenchState pipelines = accepted(snapshot)
                .select(WorkbenchState.WorkbenchTab.PIPELINES)
                .reduce(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.DOWN), 10);

        assertThat(render(120, 28, sources).text()).contains(">> beta");
        assertThat(render(120, 28, pipelines).text()).contains(">> nightly");
    }

    @Test
    void overviewKeepsAlignmentSummaryAboveReservedNotificationRows() {
        List<WorkbenchKindCount> kinds = List.of(
                new WorkbenchKindCount("source", 7, OptionalInt.of(8)),
                new WorkbenchKindCount("pipeline", 2, OptionalInt.of(3)));
        WorkbenchAlignmentCounts alignment = new WorkbenchAlignmentCounts(1, 2, 3, 4, 5, 6);
        WorkbenchRemoteState remote = new WorkbenchRemoteState.Available(16);
        WorkbenchSnapshot snapshot = new WorkbenchSnapshot(
                1,
                1,
                WorkbenchSessionSnapshot.empty(),
                new WorkbenchOverviewSnapshot(kinds, alignment),
                new WorkbenchWorkspaceSnapshot(remote, List.of()),
                new WorkbenchResourceListSnapshot("source", remote, List.of()),
                new WorkbenchResourceListSnapshot("pipeline", remote, List.of()));

        Rendered rendered = render(88, 24, accepted(snapshot));

        assertThat(lineOf(rendered.buffer(), 18))
                .contains("Alignment: local only 1 | remote only 2 | in sync 3");
        assertThat(lineOf(rendered.buffer(), 19))
                .contains("drifted 4 | invalid local 5 | unknown 6");
        assertThat(lineOf(rendered.buffer(), 21)).doesNotContain("Remote artifacts:");
        assertThat(lineOf(rendered.buffer(), 22)).startsWith("╰");
        assertThat(lineOf(rendered.buffer(), 23))
                .contains("1-4  views", "c  context", "a  auth", "0  more", "r  refresh", "q  quit");
        assertThat(rendered.text())
                .contains("source", "local 7", "remote 8", "pipeline", "local 2", "remote 3");
    }

    @Test
    void workspaceRowsShowIdentityAlignmentSafePathAndRemoteMarker() {
        List<WorkbenchArtifactRow> rows = List.of(
                row("source", "local", WorkbenchAlignment.LOCAL_ONLY, "source/local.tap.yml", false),
                row("pipeline", "remote", WorkbenchAlignment.REMOTE_ONLY, null, true),
                row("view", "broken", WorkbenchAlignment.INVALID_LOCAL, "view/broken.tap.yml", false));

        Rendered rendered = render(157, 28, accepted(snapshot(new WorkbenchRemoteState.Available(1), rows))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE));

        assertThat(rendered.text())
                .contains("Files", "Info", "YAML", "🔌", "local.tap.yml", "○ absent")
                .contains("Path: source/local.tap.yml")
                .doesNotContain("🔌 source/local.tap.yml")
                .doesNotContain("remote only", "view/broken.tap.yml", "broken");
    }

    @Test
    void workspaceMirrorsCamelFilesInfoViewerAndRemoteDot() {
        WorkbenchArtifactRow source = row(
                "source", "orders", WorkbenchAlignment.IN_SYNC, "source/orders.tap.yml", true);
        WorkbenchState state = accepted(snapshot(
                        new WorkbenchRemoteState.Available(1), List.of(source)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE)
                .withWorkspaceView(WorkbenchWorkspaceState.empty().open(
                        Path.of("source/orders.tap.yml"),
                        "apiVersion: tapstate/v1\nkind: Source\nmetadata:\n  name: orders\n# note\n"));

        Rendered rendered = render(120, 30, state);

        assertThat(rendered.text())
                .contains("╭ Files ", "╭ Info ", "╭ YAML [orders.tap.yml] ")
                .contains("orders.tap.yml", "Path: source/orders.tap.yml", "●", "Remote: ● present")
                .contains(">> 1 apiVersion: tapstate/v1", "2 kind: Source")
                .contains("F4  edit", "Tab  files")
                .doesNotContain("Enter  open", "Tab  viewer", "Remote artifacts:");

        int keyColumn = findColumn(rendered.buffer(), findLine(rendered.buffer(), "apiVersion"), "apiVersion");
        int valueColumn = findColumn(rendered.buffer(), findLine(rendered.buffer(), "tapstate/v1"), "tapstate/v1");
        assertThat(rendered.buffer().get(keyColumn, findLine(rendered.buffer(), "apiVersion")).style().fg())
                .isEqualTo(WorkbenchTheme.dark().codeKey().fg());
        assertThat(rendered.buffer().get(valueColumn, findLine(rendered.buffer(), "tapstate/v1")).style().fg())
                .isEqualTo(WorkbenchTheme.dark().warning().fg());
        int commentY = findLine(rendered.buffer(), "# note");
        int commentX = findColumn(rendered.buffer(), commentY, "# note");
        assertThat(rendered.buffer().get(commentX, commentY).style().fg())
                .isEqualTo(WorkbenchTheme.dark().muted().fg());
        assertThat(rendered.buffer().get(keyColumn, findLine(rendered.buffer(), "apiVersion")).style().bg())
                .isEqualTo(WorkbenchTheme.dark().selection().bg());
        int inactiveKeyY = findLine(rendered.buffer(), "kind: Source");
        int inactiveKeyX = findColumn(rendered.buffer(), inactiveKeyY, "kind");
        assertThat(rendered.buffer().get(inactiveKeyX, inactiveKeyY).style().bg())
                .isEqualTo(WorkbenchTheme.dark().base().bg());
    }

    @Test
    void workspaceQuickDocUsesCamelMainBottomDividerInsteadOfAnInlineToggle() {
        WorkbenchArtifactRow source = row(
                "source", "orders", WorkbenchAlignment.IN_SYNC, "source/orders.tap.yml", true);
        WorkbenchWorkspaceState workspace = WorkbenchWorkspaceState.empty()
                .open(Path.of("source/orders.tap.yml"), "kind: source\nid: orders\n")
                .navigate(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.DOWN));
        WorkbenchState state = accepted(snapshot(
                        new WorkbenchRemoteState.Available(1), List.of(source)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE)
                .withWorkspaceView(workspace);

        Rendered rendered = render(120, 30, state);

        assertThat(rendered.text())
                .contains("─── source.id", "id: orders")
                .doesNotContain("Quick Doc", "i  quick doc");
    }

    @Test
    void editorUsesPlainTextGutterAndNearestYamlScopeLikeCamel() {
        WorkbenchArtifactRow source = row(
                "source", "orders", WorkbenchAlignment.IN_SYNC, "source/orders.tap.yml", true);
        WorkbenchWorkspaceState workspace = WorkbenchWorkspaceState.empty()
                .open(Path.of("source/orders.tap.yml"), "spec:\n  config:\n    batchSize: 100\n")
                .navigate(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.DOWN))
                .navigate(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.DOWN))
                .edit();
        WorkbenchState state = accepted(snapshot(
                        new WorkbenchRemoteState.Available(1), List.of(source)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE)
                .withWorkspaceView(workspace);

        Rendered rendered = render(120, 30, state);
        int scopeY = findLine(rendered.buffer(), "config:");
        int scopeX = findColumn(rendered.buffer(), scopeY, "config:");
        int cursorY = findLine(rendered.buffer(), "3 |    batchSize: 100");
        int cursorX = findColumn(rendered.buffer(), cursorY, "batchSize");

        assertThat(rendered.text()).doesNotContain(">>");
        assertThat(rendered.buffer().get(scopeX, scopeY).style().fg())
                .isEqualTo(WorkbenchTheme.dark().accent().fg());
        assertThat(rendered.buffer().get(cursorX, cursorY).style().fg())
                .isEqualTo(WorkbenchTheme.dark().base().fg());
        assertThat(rendered.buffer().get(cursorX, cursorY)
                .style().bg()).isEqualTo(WorkbenchTheme.dark().selection().bg());
    }

    @Test
    void tableHighlightsOnlyTheActiveSortColumnAndFooterOwnsSortAction() {
        WorkbenchSnapshot snapshot = snapshot(
                new WorkbenchRemoteState.Available(2),
                List.of(
                        row("source", "zeta", WorkbenchAlignment.IN_SYNC,
                                "source/zeta.tap.yml", true),
                        row("source", "alpha", WorkbenchAlignment.DRIFTED,
                                "source/alpha.tap.yml", true)));
        WorkbenchState state = accepted(snapshot)
                .select(WorkbenchState.WorkbenchTab.SOURCES)
                .reduce(KeyEvent.ofChar('s'), 10);

        Rendered rendered = render(157, 28, state);
        String header = lineOf(rendered.buffer(), 4);
        assertThat(header).contains("STATE▼");
        assertThat(rendered.text().indexOf("alpha"))
                .isLessThan(rendered.text().indexOf("zeta"));
        assertThat(rendered.text()).contains("s  sort", "Esc  back", "r  refresh");

        int identifierColumn = header.indexOf("IDENTIFIER");
        int stateColumn = header.indexOf("STATE");
        assertThat(rendered.buffer().get(identifierColumn, 4).style().fg())
                .isEqualTo(WorkbenchTheme.dark().base().fg());
        assertThat(rendered.buffer().get(stateColumn, 4).style().effectiveModifiers())
                .contains(Modifier.BOLD);
        assertThat(rendered.buffer().get(stateColumn, 4).style().fg())
                .isEqualTo(WorkbenchTheme.dark().label().fg());
        String footer = lineOf(rendered.buffer(), 27);
        int sortKey = footer.indexOf("s  sort");
        assertThat(rendered.buffer().get(sortKey, 27).style().bg())
                .isEqualTo(WorkbenchTheme.dark().hintKey().bg());
        assertThat(rendered.buffer().get(sortKey + 2, 27).style().bg())
                .isEqualTo(WorkbenchTheme.dark().base().bg());
        assertThat(footer).contains("s  sort");
        assertThat(header).doesNotContain("KIND", "REMOTE");
    }

    @Test
    void dirtyWorkspaceEditorMarksTheFileAndRendersCamelDiscardConfirmation() {
        WorkbenchArtifactRow source = row(
                "source", "orders", WorkbenchAlignment.IN_SYNC, "source/orders.tap.yml", true);
        WorkbenchWorkspaceState workspace = WorkbenchWorkspaceState.empty()
                .open(Path.of("source/orders.tap.yml"), "apiVersion: tapstate/v1\nmetadata:\n  name: orders\n")
                .edit()
                .edit(KeyEvent.ofChar('#'));
        WorkbenchState state = accepted(snapshot(
                        new WorkbenchRemoteState.Available(1), List.of(source)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE)
                .withWorkspaceView(workspace)
                .withOverlay(new WorkbenchOverlayState.Confirm(
                        WorkbenchOverlayState.Confirm.Intent.DiscardChanges.INSTANCE,
                        "Discard Changes?",
                        "Unsaved changes will be lost.",
                        false,
                        Optional.empty()));

        Rendered rendered = render(120, 30, state);

        assertThat(rendered.text())
                .contains("orders.tap.yml *", "Edit [orders.tap.yml *]")
                .contains("Discard Changes?", "Unsaved changes will be lost.")
                .contains("Enter  confirm", "Esc  cancel")
                .doesNotContain("Remote artifacts:");
    }

    @Test
    void remoteStatesRemainHonestAndDoNotHideLocalRows() {
        WorkbenchArtifactRow local = row(
                "source", "local-orders", WorkbenchAlignment.UNKNOWN, "source/local-orders.tap.yml", false);

        assertStatus(new WorkbenchRemoteState.NotConfigured(), "No context selected", local);
        assertStatus(new WorkbenchRemoteState.SignedOut(), "Signed out", local);
        assertStatus(new WorkbenchRemoteState.Offline(), "Server offline", local);
        assertStatus(new WorkbenchRemoteState.Rejected("access-denied", "Access denied"),
                "Remote rejected: access-denied, Access denied", local);
        assertStatus(new WorkbenchRemoteState.Diagnostic(CliError.WORKBENCH_UNAVAILABLE, Map.of()),
                "Diagnostic: cli.workbench-unavailable", local);
        assertStatus(new WorkbenchRemoteState.Available(0), "Remote workspace is empty", local);
    }

    @Test
    void emptyTableTabsRenderTheirTrueEmptyStateWithoutSelectionShortcut() {
        WorkbenchSnapshot snapshot = snapshot(new WorkbenchRemoteState.Available(0), List.of());

        assertEmptyTable(snapshot, WorkbenchState.WorkbenchTab.WORKSPACE, "No workspace files.");
        assertEmptyTable(snapshot, WorkbenchState.WorkbenchTab.SOURCES, "No sources.");
        assertEmptyTable(snapshot, WorkbenchState.WorkbenchTab.PIPELINES, "No pipelines.");
    }

    @Test
    void nonEmptyTableTabShowsSelectionShortcut() {
        WorkbenchSnapshot snapshot = snapshot(
                new WorkbenchRemoteState.Available(0),
                List.of(row("source", "orders", WorkbenchAlignment.IN_SYNC,
                        "source/orders.tap.yml", true)));

        Rendered rendered = render(88, 24, accepted(snapshot)
                .select(WorkbenchState.WorkbenchTab.SOURCES));

        assertThat(rendered.text())
                .contains("↑↓  navigate", "Esc  back", "s  sort", "r  refresh", "q  quit")
                .doesNotContain("Left/Right switch");
    }

    @Test
    void unknownAlignmentIsRenderedAsUnknown() {
        WorkbenchArtifactRow unknown = row(
                "source", "orders", WorkbenchAlignment.UNKNOWN, "source/orders.tap.yml", false);

        Rendered rendered = render(100, 24, accepted(snapshot(
                        new WorkbenchRemoteState.Offline(), List.of(unknown)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE));

        assertThat(rendered.text()).contains("orders", "unknown", "Server offline");
    }

    @Test
    void initialAndLoadingStatesAreDistinct() {
        Rendered initial = render(88, 24, WorkbenchState.initial());
        Rendered loading = render(88, 24,
                WorkbenchState.initial().expectSnapshot(new WorkbenchSnapshot(1, 1)));

        assertThat(initial.text()).contains("No snapshot loaded. Press r to refresh.");
        assertThat(loading.text()).contains("Loading workbench snapshot...");
        assertThat(initial.text()).doesNotContain("Loading workbench snapshot...");
    }

    @Test
    void effectiveScrollKeepsSelectionVisibleAndRowHitsMapToSnapshotIndices() {
        List<WorkbenchArtifactRow> rows = IntStream.range(0, 24)
                .mapToObj(index -> row(
                        "source", "source-%02d".formatted(index), WorkbenchAlignment.LOCAL_ONLY,
                        "source/source-%02d.tap.yml".formatted(index), false))
                .toList();
        WorkbenchSnapshot snapshot = snapshot(new WorkbenchRemoteState.Available(0), rows);
        WorkbenchState state = accepted(snapshot)
                .select(WorkbenchState.WorkbenchTab.WORKSPACE)
                .selectRow(WorkbenchState.WorkbenchTab.WORKSPACE, 23, 3);

        Rendered rendered = render(88, 24, state);
        WorkbenchRenderer.RenderLayout layout = rendered.layout();

        assertThat(layout.visibleRowCapacity()).isGreaterThan(3);
        assertThat(rendered.text()).contains("source-23").doesNotContain("source-00");
        WorkbenchRenderer.RowHit selected = layout.rowHits().stream()
                .filter(hit -> hit.rowIndex() == 23)
                .findFirst()
                .orElseThrow();
        assertThat(layout.rowAt(selected.area().x(), selected.area().y())).contains(selected);
        assertThat(rendered.buffer().get(selected.area().x(), selected.area().y()).style().effectiveModifiers())
                .contains(Modifier.BOLD);
        assertThat(state.workspaceTable().scrollOffset()).isEqualTo(21);
    }

    @Test
    void hitMapAcceptsOnlyRenderedLabelsAndRows() {
        WorkbenchState state = accepted(snapshot(
                        new WorkbenchRemoteState.Available(1),
                        List.of(row("source", "orders", WorkbenchAlignment.IN_SYNC,
                                "source/orders.tap.yml", true))))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE);
        Rendered rendered = render(100, 24, state);
        WorkbenchRenderer.RenderLayout layout = rendered.layout();

        for (WorkbenchRenderer.TabHit hit : layout.tabHits()) {
            assertThat(layout.tabAt(hit.area().x(), hit.area().y())).contains(hit.tab());
            assertThat(layout.tabAt(hit.area().right() - 1, hit.area().y())).contains(hit.tab());
        }
        WorkbenchRenderer.TabHit first = layout.tabHits().getFirst();
        WorkbenchRenderer.TabHit second = layout.tabHits().get(1);
        assertThat(layout.tabAt(first.area().right(), first.area().y())).isEmpty();
        assertThat(layout.tabAt(second.area().x() - 1, second.area().y())).isEmpty();
        assertThat(layout.tabAt(first.area().x(), first.area().y() + 1)).isEmpty();

        WorkbenchRenderer.RowHit row = layout.rowHits().getFirst();
        assertThat(layout.rowAt(row.area().x(), row.area().y())).contains(row);
        assertThat(layout.rowAt(row.area().right(), row.area().y())).isEmpty();
        assertThat(layout.rowAt(row.area().x(), row.area().y() - 1)).isEmpty();
        WorkbenchRenderer.ActionHit context = layout.actionHits().stream()
                .filter(hit -> hit.launcher() == WorkbenchRenderer.Launcher.CONTEXT)
                .findFirst()
                .orElseThrow();
        WorkbenchRenderer.ActionHit more = layout.actionHits().stream()
                .filter(hit -> hit.launcher() == WorkbenchRenderer.Launcher.MORE)
                .findFirst()
                .orElseThrow();
        assertThat(layout.actionAt(context.area().x(), context.area().y()))
                .contains(WorkbenchRenderer.Launcher.CONTEXT);
        assertThat(layout.actionAt(more.area().x(), more.area().y()))
                .contains(WorkbenchRenderer.Launcher.MORE);
        assertThat(layout.footerHits()).isNotEmpty().isUnmodifiable();
        for (WorkbenchRenderer.FooterHit hit : layout.footerHits()) {
            assertThat(layout.footerActionAt(hit.area().x(), hit.area().y())).contains(hit.action());
            assertThat(layout.footerActionAt(hit.area().right() - 1, hit.area().y())).contains(hit.action());
        }
        assertThat(layout.tabHits()).isUnmodifiable();
        assertThat(layout.rowHits()).isUnmodifiable();
        assertThat(layout.actionHits()).isUnmodifiable();
        assertThat(rendered.text()).contains("0 More");
    }

    @Test
    void moreOverlayRendersAboveTheViewWithClickableEntries() {
        WorkbenchState state = accepted(snapshot(new WorkbenchRemoteState.Available(0), List.of()))
                .withOverlay(new WorkbenchOverlayState.More(0));

        Rendered rendered = render(100, 24, state);

        assertThat(rendered.text()).contains("More", "Context", "Authentication", "Help");
        assertThat(rendered.layout().overlayHits()).hasSize(3).isUnmodifiable();
        for (WorkbenchRenderer.OverlayHit hit : rendered.layout().overlayHits()) {
            assertThat(rendered.layout().overlayIndexAt(hit.area().x(), hit.area().y()))
                    .hasValue(hit.index());
        }
    }

    @Test
    void contextFormUsesCamelRoundedPopupAndFocusedFieldTreatment() {
        WorkbenchState state = accepted(snapshot(new WorkbenchRemoteState.Available(0), List.of()))
                .withOverlay(new WorkbenchOverlayState.ContextCreate(
                        WorkbenchOverlayState.ContextCreate.Stage.SERVER,
                        "dev",
                        "http://127.0.0.1:7900",
                        true,
                        false,
                        Optional.empty()));

        Rendered rendered = render(100, 24, state);

        assertThat(rendered.text())
                .contains("╭", " New Context ", "Name:", "Server:", "Verify TLS:")
                .doesNotContain("+----------------------------------------------------------+");
        int serverY = findLine(rendered.buffer(), "Server:");
        int nameY = findLine(rendered.buffer(), "Name:");
        assertThat(rendered.buffer().get(findColumn(rendered.buffer(), serverY, "Server:"), serverY)
                .style().effectiveModifiers()).contains(Modifier.BOLD);
        assertThat(rendered.buffer().get(findColumn(rendered.buffer(), nameY, "Name:"), nameY)
                .style().effectiveModifiers()).doesNotContain(Modifier.BOLD);
        assertThat(rendered.buffer().get(50, 15).style().bg())
                .isEqualTo(WorkbenchTheme.dark().base().bg());
    }

    @Test
    void everyRenderedLineIsClippedAtTheRightEdge() {
        String longId = "pipeline-" + "x".repeat(160);
        String longPath = "pipeline/" + "nested/".repeat(30) + "artifact.tap.yml";
        WorkbenchArtifactRow row = row("pipeline", longId, WorkbenchAlignment.DRIFTED, longPath, true);
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                Path.of("/workspace/" + "wide/".repeat(40)),
                Optional.of("context-" + "c".repeat(120)),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.MACHINE,
                Optional.of("principal-" + "p".repeat(120)),
                Optional.of(URI.create("https://tapstate.example:9443")),
                "tapstate test");
        WorkbenchState state = accepted(snapshot(
                        session, new WorkbenchRemoteState.Available(1), List.of(row)))
                .select(WorkbenchState.WorkbenchTab.PIPELINES);

        for (int width : List.of(88, 156, 157)) {
            Rendered rendered = render(width, 24, state);
            String renderedRow = lineOf(rendered.buffer(), 5);
            assertThat(renderedRow)
                    .contains("pipeline", "pipeline-")
                    .doesNotContain(longId, longPath, "artifact.tap.yml");
            assertOccupiedToRightEdge(rendered);
        }
    }

    @Test
    void clippingPaddingAndHitWidthUseTamboUiCharacterWidths() {
        String arabicSymbol = "\u06DE";
        String hangulLeadingConsonant = "\u1100";
        assertThat(CharWidth.of(arabicSymbol)).isEqualTo(1);
        assertThat(CharWidth.of(hangulLeadingConsonant)).isEqualTo(1);

        assertUnicodeColumnBoundary("a".repeat(39) + arabicSymbol + "X", arabicSymbol);
        assertUnicodeColumnBoundary(
                "a".repeat(39) + hangulLeadingConsonant + "X", hangulLeadingConsonant);
    }

    @Test
    void displayedDataCannotInjectControlsOrExposeAnAbsoluteLocalPath() {
        String escape = "\u001b[31m";
        WorkbenchArtifactRow unsafe = new WorkbenchArtifactRow(
                new WorkbenchArtifactKey("source", "orders\nforged"),
                List.of(new WorkbenchLocalArtifact(
                        Path.of("/private/tenant/source/orders.tap.yml"),
                        Optional.of("source"), true, false, true)),
                List.of(),
                WorkbenchAlignment.LOCAL_ONLY);
        WorkbenchRemoteState rejected = new WorkbenchRemoteState.Rejected(
                "denied", escape + "denied\nforged");

        Rendered rendered = render(100, 24, accepted(snapshot(rejected, List.of(unsafe)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE));

        assertThat(rendered.text())
                .contains("orders forged", "orders.tap.yml", "Remote rejected: denied,  [31mdenied forged")
                .doesNotContain("\u001b", "/private/tenant");
    }

    private static void assertStatus(
            WorkbenchRemoteState remoteState, String message, WorkbenchArtifactRow local) {
        Rendered rendered = render(100, 24, accepted(snapshot(remoteState, List.of(local)))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE));

        assertThat(rendered.text()).contains(message, "local-orders", "source/local-orders.tap.yml");
    }

    private static void assertEmptyTable(
            WorkbenchSnapshot snapshot,
            WorkbenchState.WorkbenchTab tab,
            String emptyMessage) {
        Rendered rendered = render(88, 24, accepted(snapshot).select(tab));

        assertThat(rendered.text())
                .contains(emptyMessage, "Remote workspace is empty")
                .contains("Esc  back", "r  refresh", "q  quit")
                .doesNotContain("↑↓  navigate");
        assertThat(rendered.layout().rowHits()).isEmpty();
    }

    private static void assertUnicodeColumnBoundary(String identifier, String boundarySymbol) {
        WorkbenchArtifactRow row = row(
                "source", identifier, WorkbenchAlignment.LOCAL_ONLY, "source/orders.tap.yml", true);
        Rendered rendered = render(88, 24, accepted(snapshot(
                        new WorkbenchRemoteState.Available(0), List.of(row)))
                .select(WorkbenchState.WorkbenchTab.SOURCES));

        int symbolX = lineOf(rendered.buffer(), 5).indexOf(boundarySymbol);
        assertThat(symbolX).isGreaterThanOrEqualTo(0);
        assertThat(rendered.buffer().get(symbolX, 5).symbol()).isEqualTo(boundarySymbol);
        assertThat(rendered.buffer().get(symbolX + 1, 5).symbol()).isEqualTo(" ");
        assertThat(lineOf(rendered.buffer(), 5)).doesNotContain("X");
        assertOccupiedToRightEdge(rendered);
    }

    private static void assertOccupiedToRightEdge(Rendered rendered) {
        WorkbenchRenderer.RowHit hit = rendered.layout().rowHits().getFirst();
        int rightmost = rendered.buffer().width() - 1;

        assertThat(hit.area().x()).isEqualTo(1);
        assertThat(hit.area().width()).isEqualTo(rendered.buffer().width() - 2);
        assertThat(hit.area().right()).isEqualTo(rendered.buffer().width() - 1);
        assertThat(rendered.buffer().get(rightmost - 1, hit.area().y()).style().effectiveModifiers())
                .contains(Modifier.BOLD);
        assertThat(rendered.layout().rowAt(rightmost - 1, hit.area().y())).contains(hit);
        assertThat(rendered.layout().rowAt(rightmost, hit.area().y())).isEmpty();
    }

    private static WorkbenchState accepted(WorkbenchSnapshot snapshot) {
        return WorkbenchState.initial().expectSnapshot(snapshot).acceptSnapshot(snapshot);
    }

    private static WorkbenchSnapshot snapshot(
            WorkbenchRemoteState remoteState, List<WorkbenchArtifactRow> rows) {
        return snapshot(WorkbenchSessionSnapshot.empty(), remoteState, rows);
    }

    private static WorkbenchSnapshot snapshot(
            WorkbenchSessionSnapshot session,
            WorkbenchRemoteState remoteState,
            List<WorkbenchArtifactRow> rows) {
        List<WorkbenchArtifactRow> workspace = rows.stream()
                .filter(row -> WorkbenchProjection.VISIBLE_KINDS.contains(row.key().kind()))
                .filter(row -> !row.local().isEmpty())
                .toList();
        List<WorkbenchArtifactRow> sources = rows.stream()
                .filter(row -> row.key().kind().equals("source") && !row.remote().isEmpty())
                .toList();
        List<WorkbenchArtifactRow> pipelines = rows.stream()
                .filter(row -> row.key().kind().equals("pipeline") && !row.remote().isEmpty())
                .toList();
        WorkbenchOverviewSnapshot overview = overview(workspace, remoteState, rows);
        return new WorkbenchSnapshot(
                1,
                1,
                session,
                overview,
                new WorkbenchWorkspaceSnapshot(remoteState, workspace),
                new WorkbenchResourceListSnapshot("source", remoteState, sources),
                new WorkbenchResourceListSnapshot("pipeline", remoteState, pipelines));
    }

    private static WorkbenchOverviewSnapshot overview(
            List<WorkbenchArtifactRow> workspace,
            WorkbenchRemoteState remoteState,
            List<WorkbenchArtifactRow> allRows) {
        List<WorkbenchKindCount> kinds = new ArrayList<>();
        for (String kind : WorkbenchProjection.VISIBLE_KINDS) {
            int local = (int) workspace.stream()
                    .filter(row -> row.key().kind().equals(kind) && !row.local().isEmpty())
                    .count();
            int remote = (int) allRows.stream()
                    .filter(row -> row.key().kind().equals(kind) && !row.remote().isEmpty())
                    .count();
            kinds.add(new WorkbenchKindCount(
                    kind,
                    local,
                    remoteState instanceof WorkbenchRemoteState.Available
                            ? OptionalInt.of(remote)
                            : OptionalInt.empty()));
        }
        return new WorkbenchOverviewSnapshot(kinds, new WorkbenchAlignmentCounts(
                count(workspace, WorkbenchAlignment.LOCAL_ONLY),
                count(workspace, WorkbenchAlignment.REMOTE_ONLY),
                count(workspace, WorkbenchAlignment.IN_SYNC),
                count(workspace, WorkbenchAlignment.DRIFTED),
                count(workspace, WorkbenchAlignment.INVALID_LOCAL),
                count(workspace, WorkbenchAlignment.UNKNOWN)));
    }

    private static int count(List<WorkbenchArtifactRow> rows, WorkbenchAlignment alignment) {
        return (int) rows.stream().filter(row -> row.alignment() == alignment).count();
    }

    private static WorkbenchArtifactRow row(
            String kind,
            String id,
            WorkbenchAlignment alignment,
            String relativePath,
            boolean remote) {
        List<WorkbenchLocalArtifact> local = relativePath == null
                ? List.of()
                : List.of(new WorkbenchLocalArtifact(
                        Path.of(relativePath), Optional.of(kind), true, false, true));
        List<WorkbenchRemoteArtifact> remoteArtifacts = remote
                ? List.of(new WorkbenchRemoteArtifact(true, true))
                : List.of();
        return new WorkbenchArtifactRow(
                new WorkbenchArtifactKey(kind, id), local, remoteArtifacts, alignment);
    }

    private static Rendered render(int width, int height, WorkbenchState state) {
        Buffer buffer = Buffer.empty(new Rect(0, 0, width, height));
        WorkbenchRenderer.RenderLayout layout = WorkbenchRenderer.render(Frame.forTesting(buffer), state);
        return new Rendered(buffer, layout, textOf(buffer));
    }

    private static String textOf(Buffer buffer) {
        StringBuilder text = new StringBuilder();
        for (int y = 0; y < buffer.height(); y++) {
            for (int x = 0; x < buffer.width(); x++) {
                String symbol = buffer.get(x, y).symbol();
                text.append(symbol.isEmpty() ? ' ' : symbol);
            }
            text.append('\n');
        }
        return text.toString();
    }

    private static String lineOf(Buffer buffer, int y) {
        StringBuilder line = new StringBuilder();
        for (int x = 0; x < buffer.width(); x++) {
            String symbol = buffer.get(x, y).symbol();
            line.append(symbol.isEmpty() ? ' ' : symbol);
        }
        return line.toString();
    }

    private static int findLine(Buffer buffer, String text) {
        for (int y = 0; y < buffer.height(); y++) {
            if (lineOf(buffer, y).contains(text)) {
                return y;
            }
        }
        throw new AssertionError("Text is not rendered: " + text);
    }

    private static int findColumn(Buffer buffer, int y, String text) {
        int column = lineOf(buffer, y).indexOf(text);
        if (column < 0) {
            throw new AssertionError("Text is not rendered: " + text);
        }
        return column;
    }

    private record Rendered(Buffer buffer, WorkbenchRenderer.RenderLayout layout, String text) {
    }
}
