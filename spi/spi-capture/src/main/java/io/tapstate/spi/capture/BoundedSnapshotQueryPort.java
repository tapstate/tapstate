package io.tapstate.spi.capture;

/**
 * Reads a bounded, finite set of source rows for pipeline preview. Implementations must preserve exact
 * tuple semantics, enforce the request limit, and report whether every requested match was read. A
 * caller must not execute a preview DAG when {@link BoundedSnapshotQueryResult#complete()} is false.
 *
 * <p>This port is deliberately separate from {@link CapturePort}: preview does not open a CDC tail,
 * persist a source position, or participate in a pipeline lifecycle.
 */
@FunctionalInterface
public interface BoundedSnapshotQueryPort {

    /** Stable connector identity for raw-sample cache keys; implementations should include artifact version. */
    default String cacheIdentity(BoundedSnapshotQueryRequest request) {
        return request.connectorId();
    }

    /** Executes one bounded source query. */
    BoundedSnapshotQueryResult query(BoundedSnapshotQueryRequest request);

    /** Executes one bounded source query that can be stopped when its preview request is cancelled. */
    default BoundedSnapshotQueryResult query(
            BoundedSnapshotQueryRequest request, BoundedQueryCancellation cancellation) {
        cancellation.throwIfCancelled();
        return query(request);
    }
}
