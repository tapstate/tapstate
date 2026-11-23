package io.tapstate.app;

import io.tapstate.core.common.FailureFact;
import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.RetryWindow;
import java.time.Duration;

final class CaptureReadRetry {
    record Pending(FailureFact failure, Duration delay) {}

    private final RetryWindow window = new RetryWindow();
    private FailureFact retained;
    private boolean pending;

    synchronized void interrupted(FailureFact failure) {
        window.failed();
        retained = failure;
        pending = true;
    }

    synchronized boolean waiting() {
        return pending;
    }

    synchronized boolean inProgress() {
        return retained != null;
    }

    synchronized Pending decision(RetryPolicy policy) {
        return pending ? new Pending(retained, window.delay(policy).orElse(null)) : null;
    }

    synchronized void attempting() {
        pending = false;
    }

    synchronized void recovered() {
        window.recovered();
        retained = null;
        pending = false;
    }
}
