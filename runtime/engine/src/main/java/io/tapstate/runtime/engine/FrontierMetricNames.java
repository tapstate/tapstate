package io.tapstate.runtime.engine;

/**
 * What a sink's per-chain frontier readings are called among a run's statistics, and how to read one back.
 * Naming and parsing sit together because they are one contract with two ends: a reader parsing one spelling
 * while the sink writes another reports every chain as unmeasured rather than failing.
 *
 * <p>Each reading is left by the sink processor that took it, so the engine files it under that processor as
 * well as under the chain: read per processor, it says which writer's frontier is behind; read across a
 * vertex, the widest reading is how far the sink as a whole trails.
 */
public final class FrontierMetricNames {

    /**
     * What a chain's distance reading is named, before the chain: how far the bound combined for the chain runs
     * ahead of the position its frontier reached, in the chain's own positions.
     */
    public static final String GAP_PREFIX = "frontierGap.";

    /**
     * What a chain's pinned-for reading is named, before the chain: how long its durable position has been
     * where it is. Milliseconds are in the name because a run's statistics are bare numbers, and a duration
     * whose unit lives only in a document gets read in seconds by whoever wires the first threshold to it.
     */
    public static final String STALL_PREFIX = "frontierStalledMillis.";

    private FrontierMetricNames() {
    }

    /** The name the distance reading for {@code chain} is left under. */
    public static String gapNameOf(String chain) {
        return GAP_PREFIX + chain;
    }

    /** The name the pinned-for reading for {@code chain} is left under. */
    public static String stallNameOf(String chain) {
        return STALL_PREFIX + chain;
    }

    /** The chain a distance reading named {@code metric} concerns, or {@code null} when it is not one. */
    public static String chainOf(String metric) {
        return metric.startsWith(GAP_PREFIX) ? metric.substring(GAP_PREFIX.length()) : null;
    }

    /** The chain a pinned-for reading named {@code metric} concerns, or {@code null} when it is not one. */
    public static String stalledChainOf(String metric) {
        return metric.startsWith(STALL_PREFIX) ? metric.substring(STALL_PREFIX.length()) : null;
    }
}
