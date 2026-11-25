package io.tapstate.cli;

import dev.tamboui.tui.event.Event;

/** Requests a managed TamboUI redraw after an asynchronous state transition. */
enum WorkbenchRedrawEvent implements Event {
    INSTANCE
}
