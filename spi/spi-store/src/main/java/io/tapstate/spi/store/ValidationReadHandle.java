package io.tapstate.spi.store;

import java.util.concurrent.CompletionStage;

/** An independent cursor whose adapter-owned query and decode worker lifetimes are observable. */
public interface ValidationReadHandle extends ValidationRowCursor {
    /**
     * Completes only after all submitted query/decode bodies have exited or cannot start. Cancellation
     * of an executing future is not sufficient. Exceptional completion still means those workers
     * exited and retains their failure; it never means successful EOF or a confirmed observation.
     * Borrowed rows, result queues and connector-owned asynchronous resources retain their separate
     * ownership/admission rules. This stage is read-only; completion callbacks must not block workers.
     */
    CompletionStage<Void> workersExited();
}
