package io.tapstate.core.lifecycle;

import java.util.List;
import java.util.Objects;

/**
 * How wide one pipeline node runs in one execution, and why that wide.
 *
 * <p>{@code requested} and {@code origin} say what was asked and whether the author asked it; {@code scope}
 * says which of the engine's two shapes the node runs in; {@code memberCount}, {@code computedLocal} and
 * {@code effective} are the answer for the members taking part in this execution. A node the engine runs as
 * one processor for the whole cluster carries no per-member count at all: it is not a per-member count of
 * one, which on three members would be three processors.
 *
 * <p>{@code reasons} are stable ids a read face reports as they are: {@code requested-one}, a singleton
 * reason, {@code rounded-up} / {@code rounded-down} where the members could not represent the target
 * exactly, and {@code budget:<limit>} for each limit that ruled out a candidate.
 */
public record NodeParallelism(
        String node,
        int requested,
        Origin origin,
        Scope scope,
        int memberCount,
        Integer computedLocal,
        int effective,
        List<String> reasons) {

    /** Whether the target was written by the author or taken from the node's kind. */
    public enum Origin {
        EXPLICIT("explicit"),
        NODE_DEFAULT("node-default");

        private final String id;

        Origin(String id) {
            this.id = id;
        }

        /** The spelling a read face reports. */
        public String id() {
            return id;
        }
    }

    /** Which shape the engine runs the node in. */
    public enum Scope {
        /** One processor for the whole cluster, placed on one member. */
        TOTAL_ONE("total-one"),
        /** The same number of processors on every member taking part. */
        NATIVE("native");

        private final String id;

        Scope(String id) {
            this.id = id;
        }

        /** The spelling a read face reports. */
        public String id() {
            return id;
        }
    }

    /** The reason reported when the target is one. */
    public static final String REQUESTED_ONE = "requested-one";
    /** The reason reported when the members could only represent a width above the target. */
    public static final String ROUNDED_UP = "rounded-up";
    /** The reason reported when the members could only represent a width below the target. */
    public static final String ROUNDED_DOWN = "rounded-down";
    /** The prefix of the reason reported for each limit that ruled a candidate out. */
    public static final String BUDGET_PREFIX = "budget:";

    public NodeParallelism {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(scope, "scope");
        reasons = List.copyOf(reasons);
        if (memberCount < 1) {
            throw new IllegalArgumentException("a run takes part on at least one member, got " + memberCount);
        }
        if (scope == Scope.TOTAL_ONE && (computedLocal != null || effective != 1)) {
            throw new IllegalArgumentException("a total-one node runs one processor and has no per-member count");
        }
        if (scope == Scope.NATIVE && (computedLocal == null || effective != computedLocal * memberCount)) {
            throw new IllegalArgumentException("a native node runs its per-member count on every member");
        }
    }
}
