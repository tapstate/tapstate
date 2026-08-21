package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/** The shared context-management API for terminal and full-screen presentation surfaces. */
final class ContextManager {

    private final ContextConfigStore store;
    private final Supplier<UUID> ids;

    ContextManager(ContextConfigStore store) {
        this(store, UUID::randomUUID);
    }

    ContextManager(ContextConfigStore store, Supplier<UUID> ids) {
        this.store = store;
        this.ids = ids;
    }

    synchronized ContextDefinition create(String name, List<URI> seeds, boolean verifyTls) {
        validateName(name);
        ContextConfig current = store.load();
        if (current.contexts().containsKey(name)) {
            throw new TapstateException(CliError.CONTEXT_ALREADY_EXISTS, Map.of("name", name), null);
        }
        ContextDefinition definition = definition(name, ids.get(), seeds, verifyTls, ids.get());
        Map<String, ContextDefinition> contexts = new LinkedHashMap<>(current.contexts());
        contexts.put(name, definition);
        store.save(new ContextConfig(current.version(), current.lastContext(), contexts,
                current.workspaceBindings()));
        return definition;
    }

    synchronized ContextDefinition edit(String name, List<URI> seeds, boolean verifyTls) {
        ContextConfig current = store.load();
        ContextDefinition previous = required(current, name);
        ContextDefinition replacement = definition(name, previous.id(), seeds, verifyTls, previous.authRef());
        Map<String, ContextDefinition> contexts = new LinkedHashMap<>(current.contexts());
        contexts.put(name, replacement);
        store.save(new ContextConfig(current.version(), current.lastContext(), contexts,
                current.workspaceBindings()));
        return replacement;
    }

    synchronized List<ContextChoice> suggestions() {
        ContextConfig current = store.load();
        String suggested = current.lastContext();
        return current.contexts().entrySet().stream()
                .map(entry -> new ContextChoice(entry.getKey(), entry.getValue(), entry.getKey().equals(suggested)))
                .sorted(Comparator.comparing(ContextChoice::suggested).reversed()
                        .thenComparing(ContextChoice::name))
                .toList();
    }

    synchronized void choose(String name) {
        ContextConfig current = store.load();
        required(current, name);
        // This is picker ordering only. Target resolution never consults lastContext.
        store.save(new ContextConfig(current.version(), name, current.contexts(), current.workspaceBindings()));
    }

    synchronized void bind(Path workspaceRoot, String name) {
        ContextConfig current = store.load();
        required(current, name);
        Path canonical = canonical(workspaceRoot);
        Map<String, String> bindings = new LinkedHashMap<>(current.workspaceBindings());
        bindings.put(canonical.toString(), name);
        store.save(new ContextConfig(current.version(), current.lastContext(), current.contexts(), bindings));
    }

    synchronized Optional<String> unbind(Path workspaceRoot) {
        ContextConfig current = store.load();
        Path canonical = canonical(workspaceRoot);
        Map<String, String> bindings = new LinkedHashMap<>(current.workspaceBindings());
        String removed = bindings.remove(canonical.toString());
        if (removed != null) {
            store.save(new ContextConfig(current.version(), current.lastContext(), current.contexts(), bindings));
        }
        return Optional.ofNullable(removed);
    }

    synchronized Optional<String> contextBoundExactlyTo(Path workspaceRoot) {
        ContextConfig current = store.load();
        return Optional.ofNullable(current.workspaceBindings().get(canonical(workspaceRoot).toString()));
    }

    synchronized DeletionImpact previewDelete(String name) {
        ContextConfig current = store.load();
        ContextDefinition definition = required(current, name);
        List<Path> bindings = current.workspaceBindings().entrySet().stream()
                .filter(entry -> entry.getValue().equals(name))
                .map(Map.Entry::getKey)
                .map(Path::of)
                .sorted()
                .toList();
        return new DeletionImpact(name, definition.authRef(), bindings);
    }

    synchronized void delete(String name) {
        ContextConfig current = store.load();
        required(current, name);
        Map<String, ContextDefinition> contexts = new LinkedHashMap<>(current.contexts());
        contexts.remove(name);
        Map<String, String> bindings = new LinkedHashMap<>(current.workspaceBindings());
        bindings.entrySet().removeIf(entry -> entry.getValue().equals(name));
        String last = name.equals(current.lastContext()) ? null : current.lastContext();
        store.save(new ContextConfig(current.version(), last, contexts, bindings));
    }

    private static ContextDefinition required(ContextConfig config, String name) {
        ContextDefinition definition = config.contexts().get(name);
        if (definition == null) {
            throw new TapstateException(CliError.CONTEXT_NOT_FOUND, Map.of("name", String.valueOf(name)), null);
        }
        return definition;
    }

    private static ContextDefinition definition(
            String name, UUID id, List<URI> seeds, boolean verifyTls, UUID authRef) {
        try {
            return new ContextDefinition(id, seeds, new ContextTls(verifyTls), authRef);
        } catch (RuntimeException invalid) {
            throw new TapstateException(CliError.CONTEXT_INVALID,
                    Map.of("name", String.valueOf(name), "reason", reason(invalid)), invalid);
        }
    }

    private static void validateName(String name) {
        try {
            ContextConfig.requireName(name);
        } catch (IllegalArgumentException invalid) {
            throw new TapstateException(CliError.CONTEXT_INVALID,
                    Map.of("name", String.valueOf(name), "reason", reason(invalid)), invalid);
        }
    }

    private static Path canonical(Path workspaceRoot) {
        try {
            return workspaceRoot.toRealPath();
        } catch (IOException failure) {
            throw new TapstateException(CliError.CONTEXT_INVALID,
                    Map.of("name", workspaceRoot, "reason", reason(failure)), failure);
        }
    }

    private static String reason(Throwable failure) {
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    }

    record ContextChoice(String name, ContextDefinition definition, boolean suggested) {
    }

    record DeletionImpact(String name, UUID authRef, List<Path> workspaceBindings) {
        DeletionImpact {
            workspaceBindings = List.copyOf(new ArrayList<>(workspaceBindings));
        }
    }
}
