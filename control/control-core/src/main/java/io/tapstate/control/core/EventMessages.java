package io.tapstate.control.core;

import java.util.Map;

/** Presentation seam for fixed event wording and carried coded failures. */
@FunctionalInterface
public interface EventMessages {

    String render(String key, Map<String, Object> args);
}
