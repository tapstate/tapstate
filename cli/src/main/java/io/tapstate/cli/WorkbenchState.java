package io.tapstate.cli;

import dev.tamboui.tui.event.KeyEvent;

/** Immutable UI state reduced only by the TamboUI runner thread. */
record WorkbenchState(WorkbenchTab selectedTab) {

    static WorkbenchState initial() {
        return new WorkbenchState(WorkbenchTab.OVERVIEW);
    }

    WorkbenchState reduce(KeyEvent key) {
        WorkbenchTab next = switchTab(key);
        return next == selectedTab ? this : new WorkbenchState(next);
    }

    private WorkbenchTab switchTab(KeyEvent key) {
        if (key.isChar('1')) {
            return WorkbenchTab.OVERVIEW;
        }
        if (key.isChar('2')) {
            return WorkbenchTab.PIPELINES;
        }
        if (key.isChar('3')) {
            return WorkbenchTab.SOURCES;
        }
        if (key.isLeft()) {
            return selectedTab.previous();
        }
        if (key.isRight()) {
            return selectedTab.next();
        }
        if (key.isCancel()) {
            return WorkbenchTab.OVERVIEW;
        }
        return selectedTab;
    }

    enum WorkbenchTab {
        OVERVIEW('1', "Overview", "No workbench snapshot is loaded yet."),
        PIPELINES('2', "Pipelines", "No pipeline snapshot is loaded yet."),
        SOURCES('3', "Sources", "No source snapshot is loaded yet.");

        private static final WorkbenchTab[] TABS = values();

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

        WorkbenchTab previous() {
            return TABS[(ordinal() + TABS.length - 1) % TABS.length];
        }

        WorkbenchTab next() {
            return TABS[(ordinal() + 1) % TABS.length];
        }
    }
}
