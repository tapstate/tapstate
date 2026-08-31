package io.tapstate.cli;

import java.util.ArrayList;
import java.util.List;

/**
 * Small, bounded history model for the TUI command bar. It keeps the draft being edited separate from
 * recorded commands, so pressing Down after browsing history returns the unfinished line.
 */
final class TuiCommandHistory {

    static final int DEFAULT_LIMIT = 50;

    private final int limit;
    private final List<String> entries = new ArrayList<>();
    private int cursor;
    private String draft = "";

    TuiCommandHistory() {
        this(DEFAULT_LIMIT);
    }

    TuiCommandHistory(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("history limit must be positive");
        }
        this.limit = limit;
        cursor = 0;
    }

    /** Records a non-empty command, ignoring an immediate duplicate. */
    void record(String command) {
        String normalized = command == null ? "" : command.trim();
        if (normalized.isEmpty()) {
            reset();
            return;
        }
        if (entries.isEmpty() || !entries.getLast().equals(normalized)) {
            entries.add(normalized);
            if (entries.size() > limit) {
                entries.removeFirst();
            }
        }
        reset();
    }

    /** Moves to the previous command, preserving the current unfinished draft on first entry. */
    String previous(String current) {
        if (entries.isEmpty()) {
            return current == null ? "" : current;
        }
        if (cursor == entries.size()) {
            draft = current == null ? "" : current;
        }
        cursor = Math.max(0, cursor - 1);
        return entries.get(cursor);
    }

    /** Moves to the next command, returning the saved draft after the newest entry. */
    String next() {
        if (entries.isEmpty()) {
            return draft;
        }
        if (cursor < entries.size() - 1) {
            cursor++;
            return entries.get(cursor);
        }
        cursor = entries.size();
        return draft;
    }

    void reset() {
        cursor = entries.size();
        draft = "";
    }

    List<String> entries() {
        return List.copyOf(entries);
    }
}
