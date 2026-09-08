package io.tapstate.cli;

import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchStateTest {

    @Test
    void exposesFourTabsInShortcutOrderAndCyclesThem() {
        assertThat(WorkbenchState.WorkbenchTab.values())
                .containsExactly(
                        WorkbenchState.WorkbenchTab.OVERVIEW,
                        WorkbenchState.WorkbenchTab.WORKSPACE,
                        WorkbenchState.WorkbenchTab.SOURCES,
                        WorkbenchState.WorkbenchTab.PIPELINES);

        WorkbenchState state = WorkbenchState.initial();
        assertThat(state.reduce(KeyEvent.ofChar('2')).selectedTab())
                .isEqualTo(WorkbenchState.WorkbenchTab.WORKSPACE);
        assertThat(state.reduce(KeyEvent.ofChar('3')).selectedTab())
                .isEqualTo(WorkbenchState.WorkbenchTab.SOURCES);
        assertThat(state.reduce(KeyEvent.ofChar('4')).selectedTab())
                .isEqualTo(WorkbenchState.WorkbenchTab.PIPELINES);
        assertThat(state.reduce(KeyEvent.ofKey(KeyCode.LEFT)).selectedTab())
                .isEqualTo(WorkbenchState.WorkbenchTab.PIPELINES);
        assertThat(state.select(WorkbenchState.WorkbenchTab.PIPELINES)
                        .reduce(KeyEvent.ofKey(KeyCode.RIGHT))
                        .selectedTab())
                .isEqualTo(WorkbenchState.WorkbenchTab.OVERVIEW);
        assertThat(state.select(WorkbenchState.WorkbenchTab.SOURCES)
                        .reduce(KeyEvent.ofKey(KeyCode.ESCAPE))
                        .selectedTab())
                .isEqualTo(WorkbenchState.WorkbenchTab.OVERVIEW);
    }

    @Test
    void tableViewportsRemainIndependentAcrossTabSwitches() {
        WorkbenchState state = accepted(snapshot(3, 7, 6, 5, 4))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE)
                .reduce(KeyEvent.ofKey(KeyCode.DOWN), 2)
                .reduce(KeyEvent.ofKey(KeyCode.DOWN), 2)
                .select(WorkbenchState.WorkbenchTab.SOURCES)
                .reduce(KeyEvent.ofKey(KeyCode.DOWN), 3)
                .select(WorkbenchState.WorkbenchTab.PIPELINES)
                .reduce(KeyEvent.ofKey(KeyCode.PAGE_DOWN), 2)
                .select(WorkbenchState.WorkbenchTab.WORKSPACE);

        assertThat(state.workspaceTable()).isEqualTo(new WorkbenchTableState(2, 1));
        assertThat(state.sourcesTable()).isEqualTo(new WorkbenchTableState(1, 0));
        assertThat(state.pipelinesTable()).isEqualTo(new WorkbenchTableState(2, 1));
    }

    @Test
    void navigationRespectsBoundsAndKeepsSelectionInsideViewport() {
        WorkbenchState state = accepted(snapshot(3, 7, 6, 0, 0))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE);

        assertThat(state.workspaceTable()).isEqualTo(new WorkbenchTableState(0, 0));
        assertThat(state.reduce(KeyEvent.ofKey(KeyCode.UP), 2)).isSameAs(state);

        state = state.reduce(KeyEvent.ofKey(KeyCode.DOWN), 2)
                .reduce(KeyEvent.ofChar('j'), 2);
        assertThat(state.workspaceTable()).isEqualTo(new WorkbenchTableState(2, 1));

        state = state.reduce(KeyEvent.ofKey(KeyCode.PAGE_DOWN), 2)
                .reduce(KeyEvent.ofKey(KeyCode.PAGE_DOWN), 2)
                .reduce(KeyEvent.ofKey(KeyCode.DOWN), 2);
        assertThat(state.workspaceTable()).isEqualTo(new WorkbenchTableState(5, 4));

        state = state.reduce(KeyEvent.ofKey(KeyCode.PAGE_UP), 2)
                .reduce(KeyEvent.ofChar('k'), 2);
        assertThat(state.workspaceTable()).isEqualTo(new WorkbenchTableState(2, 2));

        state = state.selectRow(WorkbenchState.WorkbenchTab.WORKSPACE, 5, 2);
        assertThat(state.workspaceTable()).isEqualTo(new WorkbenchTableState(5, 4));
    }

    @Test
    void emptyTableIgnoresSelectionNavigationAndMouseSelection() {
        WorkbenchState state = accepted(snapshot(3, 7, 0, 0, 0))
                .select(WorkbenchState.WorkbenchTab.WORKSPACE);

        assertThat(state.workspaceTable()).isEqualTo(WorkbenchTableState.empty());
        assertThat(state.reduce(KeyEvent.ofKey(KeyCode.DOWN), 3)).isSameAs(state);
        assertThat(state.selectRow(WorkbenchState.WorkbenchTab.WORKSPACE, 0, 3)).isSameAs(state);
    }

    @Test
    void replacementSnapshotClampsEveryViewportToItsNewRows() {
        WorkbenchState state = accepted(snapshot(3, 7, 6, 5, 4))
                .selectRow(WorkbenchState.WorkbenchTab.WORKSPACE, 5, 2)
                .selectRow(WorkbenchState.WorkbenchTab.SOURCES, 4, 2)
                .selectRow(WorkbenchState.WorkbenchTab.PIPELINES, 3, 2);
        WorkbenchSnapshot replacement = snapshot(3, 8, 2, 1, 0);

        WorkbenchState changed = state.expectSnapshot(replacement).acceptSnapshot(replacement);

        assertThat(changed.workspaceTable()).isEqualTo(new WorkbenchTableState(1, 1));
        assertThat(changed.sourcesTable()).isEqualTo(new WorkbenchTableState(0, 0));
        assertThat(changed.pipelinesTable()).isEqualTo(WorkbenchTableState.empty());
    }

    @Test
    void contextChangeClearsAcceptedSnapshotAndResetsViewports() {
        WorkbenchSnapshot accepted = snapshot(3, 7, 4, 3, 2);
        WorkbenchSnapshot nextContext = snapshot(4, 8, 0, 0, 0);
        WorkbenchState state = accepted(accepted)
                .selectRow(WorkbenchState.WorkbenchTab.WORKSPACE, 3, 2)
                .selectRow(WorkbenchState.WorkbenchTab.SOURCES, 2, 2)
                .selectRow(WorkbenchState.WorkbenchTab.PIPELINES, 1, 2);

        WorkbenchState changed = state.expectSnapshot(nextContext);

        assertThat(changed.expectedSnapshot()).contains(nextContext);
        assertThat(changed.snapshot()).isEmpty();
        assertThat(changed.workspaceTable()).isEqualTo(WorkbenchTableState.empty());
        assertThat(changed.sourcesTable()).isEqualTo(WorkbenchTableState.empty());
        assertThat(changed.pipelinesTable()).isEqualTo(WorkbenchTableState.empty());
    }

    @Test
    void sameContextRefreshRetainsAcceptedSnapshotAndViewportsUntilReplacementArrives() {
        WorkbenchSnapshot accepted = snapshot(3, 7, 4, 3, 2);
        WorkbenchSnapshot nextRequest = snapshot(3, 8, 0, 0, 0);
        WorkbenchState state = accepted(accepted)
                .selectRow(WorkbenchState.WorkbenchTab.WORKSPACE, 3, 2)
                .selectRow(WorkbenchState.WorkbenchTab.SOURCES, 2, 2)
                .selectRow(WorkbenchState.WorkbenchTab.PIPELINES, 1, 2);

        WorkbenchState changed = state.expectSnapshot(nextRequest);

        assertThat(changed.expectedSnapshot()).contains(nextRequest);
        assertThat(changed.snapshot()).contains(accepted);
        assertThat(changed.workspaceTable()).isEqualTo(state.workspaceTable());
        assertThat(changed.sourcesTable()).isEqualTo(state.sourcesTable());
        assertThat(changed.pipelinesTable()).isEqualTo(state.pipelinesTable());
    }

    @Test
    void placeholderExpectationAcceptsPopulatedSnapshotWithTheSameIdentity() {
        WorkbenchSnapshot accepted = snapshot(3, 7, 1, 1, 1);
        WorkbenchSnapshot placeholder = new WorkbenchSnapshot(3, 8);
        WorkbenchSnapshot populated = snapshot(3, 8, 2, 2, 2);
        WorkbenchState awaiting = accepted(accepted).expectSnapshot(placeholder);

        WorkbenchState published = awaiting.acceptSnapshot(populated);

        assertThat(placeholder).isNotEqualTo(populated);
        assertThat(placeholder.identity()).isEqualTo(populated.identity());
        assertThat(published.expectedSnapshot()).contains(placeholder);
        assertThat(published.snapshot()).contains(populated);
        assertThat(published.workspaceTable()).isEqualTo(new WorkbenchTableState(0, 0));
        assertThat(published.sourcesTable()).isEqualTo(new WorkbenchTableState(0, 0));
        assertThat(published.pipelinesTable()).isEqualTo(new WorkbenchTableState(0, 0));
    }

    @Test
    void populatedSnapshotWithDifferentIdentityIsRejected() {
        WorkbenchSnapshot expected = new WorkbenchSnapshot(3, 8);
        WorkbenchSnapshot wrongRequest = snapshot(3, 9, 1, 1, 1);
        WorkbenchState awaiting = WorkbenchState.initial().expectSnapshot(expected);

        WorkbenchState unchanged = awaiting.acceptSnapshot(wrongRequest);

        assertThat(unchanged).isSameAs(awaiting);
        assertThat(unchanged.snapshot()).isEmpty();
    }

    private static WorkbenchState accepted(WorkbenchSnapshot snapshot) {
        return WorkbenchState.initial().expectSnapshot(snapshot).acceptSnapshot(snapshot);
    }

    private static WorkbenchSnapshot snapshot(
            long generation,
            long sequence,
            int workspaceCount,
            int sourceCount,
            int pipelineCount) {
        List<WorkbenchArtifactRow> sources = rows("source", sourceCount);
        List<WorkbenchArtifactRow> pipelines = rows("pipeline", pipelineCount);
        List<WorkbenchArtifactRow> workspace = rows("view", workspaceCount);
        WorkbenchRemoteState remote = new WorkbenchRemoteState.Available(workspaceCount);
        return new WorkbenchSnapshot(
                generation,
                sequence,
                WorkbenchSessionSnapshot.empty(),
                WorkbenchOverviewSnapshot.empty(),
                new WorkbenchWorkspaceSnapshot(remote, workspace),
                new WorkbenchResourceListSnapshot("source", remote, sources),
                new WorkbenchResourceListSnapshot("pipeline", remote, pipelines));
    }

    private static List<WorkbenchArtifactRow> rows(String kind, int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> new WorkbenchArtifactRow(
                        new WorkbenchArtifactKey(kind, kind + '-' + index),
                        List.of(),
                        List.of(),
                        WorkbenchAlignment.UNKNOWN))
                .toList();
    }
}
