package io.tapstate.app;

import java.util.concurrent.*;

/** Shared bounded owner renewal/deadline threads; a blocked renewal never owns the deadline thread. */
final class ValidationOwnerTimers implements AutoCloseable {
    final ScheduledThreadPoolExecutor renewals = timer("validation-owner-renewal", 2);
    final ScheduledThreadPoolExecutor deadlines = timer("validation-owner-deadline", 1);
    private static ScheduledThreadPoolExecutor timer(String name, int workers) {
        var executor = new ScheduledThreadPoolExecutor(workers, task -> { var thread = new Thread(task, name); thread.setDaemon(true); return thread; });
        executor.setRemoveOnCancelPolicy(true); executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false); return executor;
    }
    @Override public void close() { renewals.shutdown(); deadlines.shutdown(); }
}
