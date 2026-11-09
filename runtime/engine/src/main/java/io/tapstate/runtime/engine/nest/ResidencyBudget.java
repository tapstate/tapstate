package io.tapstate.runtime.engine.nest;

/**
 * How a nest step's memory, in bytes, is shared out between the state namespaces its vertices keep resident.
 *
 * <p>A step writes one byte budget, or none and gets the default. The budget is split evenly between the
 * step's namespaces: each level of a cascade and each lookup keeps its own share, so one hot namespace cannot
 * push another's values out and turn every read of it into a store read.
 */
public final class ResidencyBudget {

    /** The share of the member's heap one step keeps resident when it writes no budget of its own. */
    public static final long DEFAULT_HEAP_DIVISOR = 8L;

    /** The least a namespace is given, however small the step's budget or however many namespaces it has. */
    public static final long SMALLEST_NAMESPACE_BYTES = 64L * 1024L;

    private ResidencyBudget() {
    }

    /** The budget a step that writes none keeps resident, on a member whose heap may grow to {@code maxHeap}. */
    public static long defaultStepBytes(long maxHeap) {
        if (maxHeap <= 0) {
            throw new IllegalArgumentException("a heap must be positive: " + maxHeap);
        }
        return Math.max(SMALLEST_NAMESPACE_BYTES, maxHeap / DEFAULT_HEAP_DIVISOR);
    }

    /** The budget each of a step's {@code namespaces} keeps resident, out of {@code stepBytes} for the step. */
    public static long perNamespace(long stepBytes, int namespaces) {
        if (stepBytes <= 0) {
            throw new IllegalArgumentException("a step's memory must be positive: " + stepBytes);
        }
        if (namespaces <= 0) {
            throw new IllegalArgumentException("a step keeps at least one namespace: " + namespaces);
        }
        return Math.max(SMALLEST_NAMESPACE_BYTES, stepBytes / namespaces);
    }
}
