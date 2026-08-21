package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/** The only resolver from launch, environment, and workspace state to a server target. */
final class ContextResolver {

    private final Supplier<ContextConfig> config;
    private final UnaryOperator<String> env;

    ContextResolver(ContextConfigStore store, UnaryOperator<String> env) {
        this(store::load, env);
    }

    ContextResolver(Supplier<ContextConfig> config, UnaryOperator<String> env) {
        this.config = config;
        this.env = env;
    }

    /**
     * Resolves one target in strict source order. Configuration is loaded only after a temporary target
     * has been ruled out, and {@code lastContext} is deliberately never consulted.
     */
    Optional<ResolvedContext> resolve(String connect, String explicitContext, Path workspaceRoot) {
        boolean hasConnect = present(connect);
        boolean hasExplicit = present(explicitContext);
        if (hasConnect && hasExplicit) {
            throw new TapstateException(CliError.CONTEXT_SOURCE_CONFLICT, Map.of(), null);
        }
        if (hasConnect) {
            return Optional.of(new ResolvedContext.Temporary(connect));
        }

        ContextConfig current = config.get();
        if (hasExplicit) {
            return Optional.of(named(current, explicitContext, ResolvedContext.Source.EXPLICIT));
        }
        String fromEnvironment = env.apply("TAPSTATE_CONTEXT");
        if (present(fromEnvironment)) {
            return Optional.of(named(current, fromEnvironment, ResolvedContext.Source.ENVIRONMENT));
        }

        Path canonical = canonicalIfPresent(workspaceRoot);
        if (canonical == null) {
            return Optional.empty();
        }
        String bound = current.workspaceBindings().get(canonical.toString());
        return present(bound)
                ? Optional.of(named(current, bound, ResolvedContext.Source.WORKSPACE_BINDING))
                : Optional.empty();
    }

    private static ResolvedContext.Named named(
            ContextConfig config, String name, ResolvedContext.Source source) {
        ContextDefinition definition = config.contexts().get(name);
        if (definition == null) {
            throw new TapstateException(CliError.CONTEXT_NOT_FOUND, Map.of("name", name), null);
        }
        return new ResolvedContext.Named(name, definition, source);
    }

    private static Path canonicalIfPresent(Path workspaceRoot) {
        if (workspaceRoot == null) {
            return null;
        }
        try {
            return workspaceRoot.toRealPath();
        } catch (IOException absentOrUnusable) {
            return null;
        }
    }

    private static boolean present(String value) {
        return value != null && !value.isBlank();
    }
}
