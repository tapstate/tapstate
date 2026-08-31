package io.tapstate.cli;

/**
 * Pure keyboard reducer for the TUI command bar. Terminal escape sequences are decoded by the runtime,
 * while this class owns the state transitions that can be exercised without a terminal.
 */
final class TuiCommandBar {

    static final int ENTER = 10;
    static final int CARRIAGE_RETURN = 13;
    static final int BACKSPACE = 8;
    static final int DELETE = 127;
    static final int ESCAPE = 27;
    static final int CTRL_C = 3;
    static final int CTRL_D = 4;
    static final int CTRL_P = 16;

    enum Event {
        NONE, SUBMIT, CANCEL, QUIT, PALETTE
    }

    record Update(String value, Event event) {
        Update {
            value = value == null ? "" : value;
            if (event == null) {
                event = Event.NONE;
            }
        }
    }

    private TuiCommandBar() {
    }

    static Update accept(String current, int code) {
        String value = current == null ? "" : current;
        if (code == CTRL_C) {
            return new Update(value, Event.CANCEL);
        }
        if (code == CTRL_D) {
            return new Update(value, value.isEmpty() ? Event.QUIT : Event.NONE);
        }
        if (code == CTRL_P) {
            return new Update(value, Event.PALETTE);
        }
        if (code == ENTER || code == CARRIAGE_RETURN) {
            return new Update(value, Event.SUBMIT);
        }
        if (code == ESCAPE) {
            return new Update("", Event.NONE);
        }
        if (code == BACKSPACE || code == DELETE) {
            if (value.isEmpty()) {
                return new Update("", Event.NONE);
            }
            int start = value.offsetByCodePoints(value.length(), -1);
            return new Update(value.substring(0, start), Event.NONE);
        }
        if (code >= 32 && code != 127 && !Character.isISOControl(code)) {
            return new Update(value + new String(Character.toChars(code)), Event.NONE);
        }
        return new Update(value, Event.NONE);
    }
}
