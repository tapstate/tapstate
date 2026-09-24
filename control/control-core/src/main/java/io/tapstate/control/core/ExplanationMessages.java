package io.tapstate.control.core;

import java.util.Map;

/** The presentation seam that renders explain text from shared catalog keys and named arguments. */
@FunctionalInterface
public interface ExplanationMessages {

    String render(String key, Map<String, Object> args);
}
