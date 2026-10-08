package io.tapstate.control.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Plaintext result shape reachable only after the dedicated reveal authorizer returns successfully. */
public record SourceConfigRevealResult(String id, Map<String, Object> config) {

    public SourceConfigRevealResult {
        config = Collections.unmodifiableMap(new LinkedHashMap<>(config));
    }
}
