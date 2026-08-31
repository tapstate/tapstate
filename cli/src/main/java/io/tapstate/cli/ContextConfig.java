package io.tapstate.cli;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** The complete non-secret context configuration persisted by the CLI. */
record ContextConfig(
        int version,
        String lastContext,
        Map<String, ContextDefinition> contexts,
        Map<String, String> workspaceBindings) {

    static final int CURRENT_VERSION = 1;

    ContextConfig {
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException("configuration must use the current schema version");
        }
        contexts = Map.copyOf(new LinkedHashMap<>(Objects.requireNonNull(contexts, "contexts")));
        workspaceBindings = Map.copyOf(new LinkedHashMap<>(
                Objects.requireNonNull(workspaceBindings, "workspaceBindings")));
        if (lastContext != null && !contexts.containsKey(lastContext)) {
            throw new IllegalArgumentException("lastContext must name an existing context");
        }
        Set<Object> ids = new HashSet<>();
        Set<Object> authRefs = new HashSet<>();
        for (Map.Entry<String, ContextDefinition> entry : contexts.entrySet()) {
            requireName(entry.getKey());
            ContextDefinition definition = Objects.requireNonNull(entry.getValue(), "context definition");
            if (!ids.add(definition.id())) {
                throw new IllegalArgumentException("context ids must be unique");
            }
            if (!authRefs.add(definition.authRef())) {
                throw new IllegalArgumentException("authRef values must be unique");
            }
        }
        for (Map.Entry<String, String> entry : workspaceBindings.entrySet()) {
            Path path = Path.of(entry.getKey());
            if (!path.isAbsolute() || !path.normalize().equals(path)) {
                throw new IllegalArgumentException("workspace binding keys must be normalized absolute paths");
            }
            if (!contexts.containsKey(entry.getValue())) {
                throw new IllegalArgumentException("workspace binding must name an existing context");
            }
        }
    }

    static ContextConfig empty() {
        return new ContextConfig(CURRENT_VERSION, null, Map.of(), Map.of());
    }

    static void requireName(String name) {
        if (name == null || !name.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
            throw new IllegalArgumentException("context name must use letters, digits, dot, underscore, or dash");
        }
    }
}
