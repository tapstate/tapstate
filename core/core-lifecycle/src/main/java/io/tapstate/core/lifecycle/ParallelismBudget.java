package io.tapstate.core.lifecycle;

/**
 * The limits a node's per-member width is held to before it is chosen. Every one of them can be counted
 * before anything is opened, which is the point: a limit only knowable once connectors are open would let
 * the same configuration pass on one connector version and fail on the next.
 *
 * <p>The connector limit counts instances, not connections. Each instance a sink opens carries a pool of its
 * own whose ceiling is the connector's business and changes with its configuration, so the instance count is
 * the one number here that bounds connections and can be known in advance; the ceilings themselves are
 * reported beside it rather than enforced.
 *
 * @param maxLocalParallelism            processors of one node on one member
 * @param maxConnectorInstancesPerMember connector instances one sink opens on one member
 * @param maxBufferedRecordsPerMember    rows one node's processors may hold at once on one member
 * @param maxBlockingProcessorsPerMember processors of one node, on one member, that each hold a thread for
 *                                       the life of a run
 */
public record ParallelismBudget(
        int maxLocalParallelism,
        int maxConnectorInstancesPerMember,
        long maxBufferedRecordsPerMember,
        int maxBlockingProcessorsPerMember) {

    /** The budget a deployment runs with unless it configures another. */
    public static final ParallelismBudget DEFAULTS = new ParallelismBudget(16, 8, 262_144L, 128);

    /** The configuration key of each limit, as a refusal and a read face name it. */
    public static final String MAX_LOCAL_PARALLELISM = "max-local-parallelism";
    public static final String MAX_CONNECTOR_INSTANCES_PER_MEMBER = "max-connector-instances-per-member";
    public static final String MAX_BUFFERED_RECORDS_PER_MEMBER = "max-buffered-records-per-member";
    public static final String MAX_BLOCKING_PROCESSORS_PER_MEMBER = "max-blocking-processors-per-member";

    public ParallelismBudget {
        if (maxLocalParallelism < 1 || maxConnectorInstancesPerMember < 1 || maxBufferedRecordsPerMember < 1
                || maxBlockingProcessorsPerMember < 1) {
            throw new IllegalArgumentException("every parallelism budget is at least one: " + this);
        }
    }

    /**
     * The first limit a node would break by running {@code local} processors on each member, or null where
     * it breaks none. A sink buffers twice its batch - the one forming and the one in flight - where every
     * other node buffers one.
     */
    String firstBrokenBy(ParallelismRequest request, int local) {
        if (local > maxLocalParallelism) {
            return MAX_LOCAL_PARALLELISM;
        }
        if (request.kind() == ParallelismRequest.Kind.SINK) {
            int instances = request.sharedConnector() ? 1 : local;
            if (instances > maxConnectorInstancesPerMember) {
                return MAX_CONNECTOR_INSTANCES_PER_MEMBER;
            }
        }
        long buffers = request.kind() == ParallelismRequest.Kind.SINK ? 2L : 1L;
        if ((long) local * request.maxRecords() * buffers > maxBufferedRecordsPerMember) {
            return MAX_BUFFERED_RECORDS_PER_MEMBER;
        }
        if ((long) request.blockingVertices() * local > maxBlockingProcessorsPerMember) {
            return MAX_BLOCKING_PROCESSORS_PER_MEMBER;
        }
        return null;
    }
}
