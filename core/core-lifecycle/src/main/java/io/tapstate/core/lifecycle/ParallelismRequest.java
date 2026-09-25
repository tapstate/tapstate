package io.tapstate.core.lifecycle;

import java.util.Objects;

/**
 * What one pipeline node asks of how wide it runs, and what about the node bounds the answer.
 *
 * <p>{@code written} is the author's cluster-wide target, or null where the author left it to the node's
 * kind. {@code singleton} is set where the node can run as one processor only, and says why; whether an
 * explicit wider target is then refused or quietly held to one is the reason's to say, not the caller's.
 *
 * @param node             the pipeline node's id, as a run names it
 * @param kind             what sort of node it is, which picks its default target
 * @param written          the target the author wrote, or null
 * @param singleton        why the node must run as one processor, or null where it may run wider
 * @param sharedConnector  true only for a sink whose connector artifact is certified to share one instance
 *                         per member; every other node opens one per processor, or none at all
 * @param maxRecords       the batch row limit the node runs with, which bounds what each processor buffers
 * @param blockingVertices how many vertices of this node hold a thread each for the life of a run; zero for
 *                         a node whose processors all share the engine's cooperative threads
 */
public record ParallelismRequest(
        String node,
        Kind kind,
        Integer written,
        Singleton singleton,
        boolean sharedConnector,
        int maxRecords,
        int blockingVertices) {

    /** The kinds of node whose defaults differ. */
    public enum Kind {
        /** A source table's reader. */
        SOURCE(1),
        /** Any step between the sources and the outputs: stateless, union, nest or join. */
        TRANSFORM(1),
        /** A view or a serve.sync element. */
        SINK(4);

        private final int defaultTarget;

        Kind(int defaultTarget) {
            this.defaultTarget = defaultTarget;
        }

        /** The target a node of this kind runs at when its author wrote none. */
        public int defaultTarget() {
            return defaultTarget;
        }
    }

    /** Why a node can run as one processor only, and whether a wider explicit target is refused for it. */
    public enum Singleton {
        /**
         * A source's reader. No connector has declared a read split into parts that never overlap and each
         * keep their own progress, so a wider target is held to one and reported, not refused: more readers
         * would only make the read slower to reason about, never wrong in a way one reader is not.
         */
        SOURCE_READS_NOT_SPLIT("source-reads-not-split", false),
        /**
         * A sink every row of which lands in one target table that has no key. With no key to route by, two
         * writers would apply one row's changes in an order nobody decides, so a wider target is refused.
         */
        SINGLE_TARGET_KEYLESS("single-target-keyless", true),
        /**
         * A step whose input carries no key it can be routed by. The same reason as a keyless sink: two
         * processors would take one row's changes in an order nobody decides.
         */
        KEY_NOT_DERIVABLE("key-not-derivable", true);

        private final String id;
        private final boolean refusesExplicit;

        Singleton(String id, boolean refusesExplicit) {
            this.id = id;
            this.refusesExplicit = refusesExplicit;
        }

        /** The stable reason id a read face reports. */
        public String id() {
            return id;
        }

        /** Whether an explicit target above one is refused rather than held to one. */
        public boolean refusesExplicit() {
            return refusesExplicit;
        }
    }

    public ParallelismRequest {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(kind, "kind");
        if (written != null && written < 1) {
            throw new IllegalArgumentException("a written target is at least one, got " + written);
        }
        if (maxRecords < 1) {
            throw new IllegalArgumentException("maxRecords must be at least one, got " + maxRecords);
        }
        if (blockingVertices < 0) {
            throw new IllegalArgumentException("blockingVertices cannot be negative, got " + blockingVertices);
        }
        if (sharedConnector && kind != Kind.SINK) {
            throw new IllegalArgumentException("only a sink opens a connector it could share");
        }
    }

    /** The target in force: the written one, or the kind's default. */
    public int target() {
        return written != null ? written : kind.defaultTarget();
    }

    /** Whether the target was written by the author rather than taken from the node's kind. */
    public boolean explicit() {
        return written != null;
    }
}
