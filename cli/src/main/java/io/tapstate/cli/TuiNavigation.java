package io.tapstate.cli;

import java.util.List;

/** Immutable selection state for resource lists and their detail pane. */
record TuiNavigation(List<String> items, int selectedIndex, boolean detailOpen) {

    TuiNavigation {
        items = List.copyOf(items == null ? List.of() : items);
        selectedIndex = items.isEmpty() ? 0 : Math.max(0, Math.min(selectedIndex, items.size() - 1));
        if (items.isEmpty()) {
            detailOpen = false;
        }
    }

    static TuiNavigation initial(List<String> items) {
        return new TuiNavigation(items, 0, false);
    }

    String selected() {
        return items.isEmpty() ? "" : items.get(selectedIndex);
    }

    TuiNavigation move(int delta) {
        return new TuiNavigation(items, selectedIndex + delta, detailOpen);
    }

    TuiNavigation open() {
        return new TuiNavigation(items, selectedIndex, !items.isEmpty());
    }

    TuiNavigation back() {
        return new TuiNavigation(items, selectedIndex, false);
    }
}
