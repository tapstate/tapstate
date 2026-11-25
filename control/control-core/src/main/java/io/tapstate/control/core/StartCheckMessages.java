package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;

import java.util.Map;

/**
 * The presentation seam that renders a start check's text from its code and named values, through the
 * shared catalog every face renders codes with.
 */
@FunctionalInterface
public interface StartCheckMessages {

    String render(TapstateErrorCode code, Map<String, Object> params);

    /** Render only declared text parameters while keeping report metadata in its original payload. */
    default String renderText(TapstateErrorCode code, Map<String, Object> params) {
        Map<String, Object> text = new java.util.LinkedHashMap<>();
        for (String name : code.placeholders()) {
            if (params.containsKey(name)) text.put(name, params.get(name));
        }
        return render(code, text);
    }
}
