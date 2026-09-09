package io.tapstate.cli;

import dev.tamboui.tui.event.KeyEvent;

import java.util.Objects;
import java.util.Optional;

/** Immutable UI state reduced only by the TamboUI runner thread. */
record WorkbenchState(
        WorkbenchTab selectedTab,
        Optional<WorkbenchSnapshot> expectedSnapshot,
        Optional<WorkbenchSnapshot> snapshot,
        Optional<WorkbenchOverlayState> overlay,
        WorkbenchTableState workspaceTable,
        WorkbenchWorkspaceState workspaceView,
        WorkbenchTableState sourcesTable,
        WorkbenchTableState pipelinesTable) {

    WorkbenchState {
        Objects.requireNonNull(selectedTab, "selectedTab");
        Objects.requireNonNull(expectedSnapshot, "expectedSnapshot");
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(overlay, "overlay");
        Objects.requireNonNull(workspaceTable, "workspaceTable");
        Objects.requireNonNull(workspaceView, "workspaceView");
        Objects.requireNonNull(sourcesTable, "sourcesTable");
        Objects.requireNonNull(pipelinesTable, "pipelinesTable");
    }

    static WorkbenchState initial() {
        return new WorkbenchState(
                WorkbenchTab.OVERVIEW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                WorkbenchTableState.empty(),
                WorkbenchWorkspaceState.empty(),
                WorkbenchTableState.empty(),
                WorkbenchTableState.empty());
    }

    WorkbenchState reduce(KeyEvent key) {
        return reduce(key, 1);
    }

    WorkbenchState reduce(KeyEvent key, int visibleRows) {
        Objects.requireNonNull(key, "key");
        requireVisibleRows(visibleRows);
        WorkbenchTab next = switchTab(key);
        if (next != selectedTab) {
            return copy(next, workspaceTable, sourcesTable, pipelinesTable);
        }
        if (selectedTab == WorkbenchTab.WORKSPACE
                && workspaceView.focus() == WorkbenchWorkspaceState.Focus.VIEWER) {
            return this;
        }
        if (selectedTab != WorkbenchTab.OVERVIEW
                && selectedTab != WorkbenchTab.WORKSPACE
                && key.isChar('s')) {
            return withTable(selectedTab, table(selectedTab).cycleSort());
        }
        if (selectedTab != WorkbenchTab.OVERVIEW
                && selectedTab != WorkbenchTab.WORKSPACE
                && key.isChar('S')) {
            return withTable(selectedTab, table(selectedTab).reverseSort());
        }
        int delta = navigationDelta(key, visibleRows);
        if (delta == 0 || !selectedTab.hasTable()) {
            return this;
        }
        int rowCount = rowCount(selectedTab);
        WorkbenchTableState current = table(selectedTab);
        WorkbenchTableState moved = current.move(delta, rowCount, visibleRows);
        return moved.equals(current) ? this : withTable(selectedTab, moved);
    }

    WorkbenchState select(WorkbenchTab tab) {
        Objects.requireNonNull(tab, "tab");
        return tab == selectedTab ? this : copy(tab, workspaceTable, sourcesTable, pipelinesTable);
    }

    WorkbenchState withOverlay(WorkbenchOverlayState nextOverlay) {
        Objects.requireNonNull(nextOverlay, "nextOverlay");
        return new WorkbenchState(
                selectedTab,
                expectedSnapshot,
                snapshot,
                Optional.of(nextOverlay),
                workspaceTable,
                workspaceView,
                sourcesTable,
                pipelinesTable);
    }

    WorkbenchState closeOverlay() {
        return overlay.isEmpty() ? this : new WorkbenchState(
                selectedTab,
                expectedSnapshot,
                snapshot,
                Optional.empty(),
                workspaceTable,
                workspaceView,
                sourcesTable,
                pipelinesTable);
    }

    WorkbenchState selectRow(WorkbenchTab tab, int index, int visibleRows) {
        Objects.requireNonNull(tab, "tab");
        requireVisibleRows(visibleRows);
        if (!tab.hasTable()) {
            return this;
        }
        WorkbenchTableState current = table(tab);
        WorkbenchTableState selected = current.select(index, rowCount(tab), visibleRows);
        return selected.equals(current) ? this : withTable(tab, selected);
    }

    WorkbenchState expectSnapshot(WorkbenchSnapshot expected) {
        Objects.requireNonNull(expected, "expected");
        if (expectedSnapshot
                .map(WorkbenchSnapshot::identity)
                .filter(expected.identity()::equals)
                .isPresent()) {
            return this;
        }
        boolean contextChanged = expectedSnapshot
                .map(WorkbenchSnapshot::identity)
                .filter(current -> current.contextGeneration() != expected.identity().contextGeneration())
                .isPresent();
        return new WorkbenchState(
                selectedTab,
                Optional.of(expected),
                contextChanged ? Optional.empty() : snapshot,
                overlay,
                contextChanged ? WorkbenchTableState.empty() : workspaceTable,
                contextChanged ? WorkbenchWorkspaceState.empty() : workspaceView,
                contextChanged ? WorkbenchTableState.empty() : sourcesTable,
                contextChanged ? WorkbenchTableState.empty() : pipelinesTable);
    }

    WorkbenchState acceptSnapshot(WorkbenchSnapshot published) {
        Objects.requireNonNull(published, "published");
        if (expectedSnapshot
                        .map(WorkbenchSnapshot::identity)
                        .filter(published.identity()::equals)
                        .isEmpty()
                || snapshot
                        .map(WorkbenchSnapshot::identity)
                        .filter(published.identity()::equals)
                        .isPresent()) {
            return this;
        }
        return new WorkbenchState(
                selectedTab,
                expectedSnapshot,
                Optional.of(published),
                overlay,
                workspaceTable.clamp(published.workspace().rows().size()),
                workspaceView,
                sourcesTable.clamp(published.sources().rows().size()),
                pipelinesTable.clamp(published.pipelines().rows().size()));
    }

    private WorkbenchState withTable(WorkbenchTab tab, WorkbenchTableState table) {
        return switch (tab) {
            case OVERVIEW -> this;
            case WORKSPACE -> copy(selectedTab, table, sourcesTable, pipelinesTable);
            case SOURCES -> copy(selectedTab, workspaceTable, table, pipelinesTable);
            case PIPELINES -> copy(selectedTab, workspaceTable, sourcesTable, table);
        };
    }

    private WorkbenchState copy(
            WorkbenchTab tab,
            WorkbenchTableState workspace,
            WorkbenchTableState sources,
            WorkbenchTableState pipelines) {
        return new WorkbenchState(
                tab, expectedSnapshot, snapshot, overlay,
                workspace, workspaceView, sources, pipelines);
    }

    WorkbenchState withWorkspaceView(WorkbenchWorkspaceState view) {
        Objects.requireNonNull(view, "view");
        return new WorkbenchState(
                selectedTab, expectedSnapshot, snapshot, overlay,
                workspaceTable, view, sourcesTable, pipelinesTable);
    }

    private WorkbenchTableState table(WorkbenchTab tab) {
        return switch (tab) {
            case OVERVIEW -> WorkbenchTableState.empty();
            case WORKSPACE -> workspaceTable;
            case SOURCES -> sourcesTable;
            case PIPELINES -> pipelinesTable;
        };
    }

    private int rowCount(WorkbenchTab tab) {
        if (snapshot.isEmpty()) {
            return 0;
        }
        WorkbenchSnapshot current = snapshot.orElseThrow();
        return switch (tab) {
            case OVERVIEW -> 0;
            case WORKSPACE -> current.workspace().rows().size();
            case SOURCES -> current.sources().rows().size();
            case PIPELINES -> current.pipelines().rows().size();
        };
    }

    private WorkbenchTab switchTab(KeyEvent key) {
        if (key.isChar('1')) {
            return WorkbenchTab.OVERVIEW;
        }
        if (key.isChar('2')) {
            return WorkbenchTab.WORKSPACE;
        }
        if (key.isChar('3')) {
            return WorkbenchTab.SOURCES;
        }
        if (key.isChar('4')) {
            return WorkbenchTab.PIPELINES;
        }
        if (key.isCancel()) {
            return WorkbenchTab.OVERVIEW;
        }
        return selectedTab;
    }

    private static int navigationDelta(KeyEvent key, int visibleRows) {
        if (key.isUp() || key.isCharIgnoreCase('k')) {
            return -1;
        }
        if (key.isDown() || key.isCharIgnoreCase('j')) {
            return 1;
        }
        if (key.isPageUp()) {
            return -visibleRows;
        }
        if (key.isPageDown()) {
            return visibleRows;
        }
        return 0;
    }

    private static void requireVisibleRows(int visibleRows) {
        if (visibleRows <= 0) {
            throw new IllegalArgumentException("Visible row capacity must be positive");
        }
    }

    enum WorkbenchTab {
        OVERVIEW('1', "Overview", "No workbench snapshot is loaded yet."),
        WORKSPACE('2', "Workspace", "No workspace artifacts are loaded yet."),
        SOURCES('3', "Sources", "No source snapshot is loaded yet."),
        PIPELINES('4', "Pipelines", "No pipeline snapshot is loaded yet.");

        private final char shortcut;
        private final String label;
        private final String emptyMessage;

        WorkbenchTab(char shortcut, String label, String emptyMessage) {
            this.shortcut = shortcut;
            this.label = label;
            this.emptyMessage = emptyMessage;
        }

        char shortcut() {
            return shortcut;
        }

        String label() {
            return label;
        }

        String emptyMessage() {
            return emptyMessage;
        }

        String displayLabel() {
            return shortcut + " " + label;
        }

        boolean hasTable() {
            return this != OVERVIEW;
        }

    }
}

/** Immutable selection and scroll state for one table tab. */
record WorkbenchTableState(
        int selectedIndex,
        int scrollOffset,
        WorkbenchSortColumn sortColumn,
        boolean sortReversed) {

    WorkbenchTableState {
        Objects.requireNonNull(sortColumn, "sortColumn");
        if (selectedIndex < -1) {
            throw new IllegalArgumentException("Selected index must be -1 or greater");
        }
        if (scrollOffset < 0) {
            throw new IllegalArgumentException("Scroll offset must not be negative");
        }
        if (selectedIndex == -1 && scrollOffset != 0) {
            throw new IllegalArgumentException("An empty selection cannot be scrolled");
        }
    }

    WorkbenchTableState(int selectedIndex, int scrollOffset) {
        this(selectedIndex, scrollOffset, WorkbenchSortColumn.IDENTIFIER, false);
    }

    static WorkbenchTableState empty() {
        return new WorkbenchTableState(-1, 0, WorkbenchSortColumn.IDENTIFIER, false);
    }

    WorkbenchTableState clamp(int rowCount) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("Row count must not be negative");
        }
        if (rowCount == 0) {
            return empty();
        }
        int selected = Math.clamp(selectedIndex, 0, rowCount - 1);
        int scroll = Math.min(scrollOffset, selected);
        return selected == selectedIndex && scroll == scrollOffset
                ? this
                : new WorkbenchTableState(selected, scroll, sortColumn, sortReversed);
    }

    WorkbenchTableState move(int delta, int rowCount, int visibleRows) {
        if (rowCount <= 0) {
            return this;
        }
        int current = Math.clamp(selectedIndex, 0, rowCount - 1);
        int selected = Math.clamp(current + delta, 0, rowCount - 1);
        return placeSelection(selected, rowCount, visibleRows);
    }

    WorkbenchTableState select(int index, int rowCount, int visibleRows) {
        if (rowCount <= 0 || index < 0 || index >= rowCount) {
            return this;
        }
        return placeSelection(index, rowCount, visibleRows);
    }

    private WorkbenchTableState placeSelection(int selected, int rowCount, int visibleRows) {
        int maximumScroll = Math.max(0, rowCount - visibleRows);
        int scroll = Math.min(scrollOffset, maximumScroll);
        if (selected < scroll) {
            scroll = selected;
        } else if (selected >= scroll + visibleRows) {
            scroll = selected - visibleRows + 1;
        }
        return selected == selectedIndex && scroll == scrollOffset
                ? this
                : new WorkbenchTableState(selected, scroll, sortColumn, sortReversed);
    }

    WorkbenchTableState cycleSort() {
        WorkbenchSortColumn[] columns = WorkbenchSortColumn.values();
        WorkbenchSortColumn next = columns[(sortColumn.ordinal() + 1) % columns.length];
        return new WorkbenchTableState(selectedIndex, scrollOffset, next, false);
    }

    WorkbenchTableState reverseSort() {
        return new WorkbenchTableState(selectedIndex, scrollOffset, sortColumn, !sortReversed);
    }
}

enum WorkbenchSortColumn {
    IDENTIFIER("IDENTIFIER"),
    ALIGNMENT("STATE"),
    LOCAL("WORKSPACE");

    private final String label;

    WorkbenchSortColumn(String label) {
        this.label = label;
    }

    String label() {
        return label;
    }
}
