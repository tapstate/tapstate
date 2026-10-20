package io.tapstate.app;

import java.net.SocketTimeoutException;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** A fixed category from at most one scoped immediate cause, without exception text. */
final class CloudArtifactReadFailureClassifier {
    private final boolean enabled;
    private final AtomicBoolean classified = new AtomicBoolean();

    CloudArtifactReadFailureClassifier(boolean enabled) {
        this.enabled = enabled;
    }

    Optional<Category> classify(Scope scope, Supplier<? extends Throwable> immediateCause) {
        if (!enabled || scope == null || !scope.matches()
                || !classified.compareAndSet(false, true)) return Optional.empty();
        try {
            Throwable cause = immediateCause == null ? null : immediateCause.get();
            return Optional.of(cause instanceof SocketTimeoutException
                    || cause instanceof InterruptedByTimeoutException
                    ? Category.READ_TIMEOUT : Category.UNCLASSIFIED);
        } catch (RuntimeException unavailable) {
            return Optional.of(Category.UNCLASSIFIED);
        }
    }

    record Scope(String connector, boolean seed, String operation, String command,
                 String step, String exceptionType, int driverCode) {
        boolean matches() {
            return "mysql".equals(connector) && !seed && "artifact".equals(operation)
                    && "find".equals(command) && "download".equals(step)
                    && "com.mongodb.MongoSocketReadException".equals(exceptionType)
                    && driverCode == -2;
        }
    }

    enum Category { READ_TIMEOUT, UNCLASSIFIED }
}
