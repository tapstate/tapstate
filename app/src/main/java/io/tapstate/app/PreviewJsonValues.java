package io.tapstate.app;

import io.tapstate.core.event.EventJsonValues;

/** Converts connector values to a bounded JSON-safe logical preview representation. */
final class PreviewJsonValues {

    private PreviewJsonValues() {
    }

    static Object normalize(Object value) {
        return EventJsonValues.normalize(value);
    }
}
