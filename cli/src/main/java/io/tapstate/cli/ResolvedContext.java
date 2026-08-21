package io.tapstate.cli;

import java.util.Objects;

/** The single non-secret transport target chosen for an online command. */
sealed interface ResolvedContext {

    /** A process-only target from {@code --connect}; it has no durable identity or auth reference. */
    record Temporary(String seedExpression) implements ResolvedContext {
        public Temporary {
            if (seedExpression == null || seedExpression.isBlank()) {
                throw new IllegalArgumentException("temporary seed expression must not be blank");
            }
        }
    }

    /** A durable named context selected explicitly, through the environment, or by exact binding. */
    record Named(String name, ContextDefinition definition, Source source) implements ResolvedContext {
        public Named {
            ContextConfig.requireName(name);
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(source, "source");
        }
    }

    enum Source {
        EXPLICIT,
        ENVIRONMENT,
        WORKSPACE_BINDING
    }
}
