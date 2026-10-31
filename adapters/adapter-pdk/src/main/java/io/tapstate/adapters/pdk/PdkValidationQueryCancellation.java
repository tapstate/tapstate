package io.tapstate.adapters.pdk;

/** Orders host query interruption against the query thread entering connector cleanup. */
final class PdkValidationQueryCancellation {
    private boolean cleanup;

    synchronized void cancel(PdkValidationActivityTask task) {
        task.cancel(!cleanup);
    }

    /** Called by the query thread; its caller restores the prior interrupt after actual cleanup. */
    synchronized boolean enterCleanup() {
        cleanup = true;
        return Thread.interrupted();
    }
}
