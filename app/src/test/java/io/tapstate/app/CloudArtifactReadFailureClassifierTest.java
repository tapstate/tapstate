package io.tapstate.app;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class CloudArtifactReadFailureClassifierTest {
    private static final CloudArtifactReadFailureClassifier.Scope SELECTED = scope(
            "mysql", false, "artifact", "find", "download",
            "com.mongodb.MongoSocketReadException", -2);

    @Test void recognizesDirectSocketTimeout() {
        category(new SocketTimeoutException(), "READ_TIMEOUT");
    }

    @Test void recognizesDirectTlsTimeout() {
        category(new InterruptedByTimeoutException(), "READ_TIMEOUT");
    }

    @Test void recognizesTimeoutSubtypeWithoutReadingIt() {
        category(new SocketTimeoutException() {}, "READ_TIMEOUT");
    }

    @Test void unknownCausesStayUnclassifiedWithoutTraversalOrText() {
        category(new IOException(), "UNCLASSIFIED");
        category(null, "UNCLASSIFIED");
        category(new OpaqueCause(new SocketTimeoutException()), "UNCLASSIFIED");
        category(new OpaqueCause(null), "UNCLASSIFIED");
    }

    @Test void disabledNeverReadsCause() {
        AtomicInteger reads = new AtomicInteger();
        var classifier = new CloudArtifactReadFailureClassifier(false);
        assertThat(classifier.classify(SELECTED, () -> {
            reads.incrementAndGet(); return new SocketTimeoutException();
        })).isEmpty();
        assertThat(reads).hasValue(0);
    }

    @Test void otherScopesNeitherReadNorConsumeTheOneAttempt() {
        List<CloudArtifactReadFailureClassifier.Scope> rejected = new ArrayList<>();
        rejected.add(null);
        rejected.add(scope("oracle", false, "artifact", "find", "download", SELECTED.exceptionType(), -2));
        rejected.add(scope("mysql", true, "artifact", "find", "download", SELECTED.exceptionType(), -2));
        rejected.add(scope("mysql", false, "registry-list", "find", "download", SELECTED.exceptionType(), -2));
        rejected.add(scope("mysql", false, "artifact", "getMore", "download", SELECTED.exceptionType(), -2));
        rejected.add(scope("mysql", false, "artifact", "find", "metadata-find", SELECTED.exceptionType(), -2));
        rejected.add(scope("mysql", false, "artifact", "find", "unclassified", SELECTED.exceptionType(), -2));
        rejected.add(scope("mysql", false, "artifact", "find", "download", "com.mongodb.MongoSocketReadTimeoutException", -2));
        rejected.add(scope("mysql", false, "artifact", "find", "download", SELECTED.exceptionType(), 8000));
        rejected.add(scope(null, false, null, null, null, null, -2));
        for (var other : rejected) {
            AtomicInteger reads = new AtomicInteger();
            var classifier = new CloudArtifactReadFailureClassifier(true);
            assertThat(classifier.classify(other, () -> {
                reads.incrementAndGet(); return new SocketTimeoutException();
            })).isEmpty();
            assertThat(reads).hasValue(0);
            assertThat(classifier.classify(SELECTED, SocketTimeoutException::new).orElseThrow().name())
                    .isEqualTo("READ_TIMEOUT");
        }
    }

    @Test void unreadableGetterStaysUnclassifiedAndConsumesOnce() {
        AtomicInteger reads = new AtomicInteger();
        var classifier = new CloudArtifactReadFailureClassifier(true);
        assertThat(classifier.classify(SELECTED, () -> {
            reads.incrementAndGet(); throw new IllegalStateException();
        }).orElseThrow().name()).isEqualTo("UNCLASSIFIED");
        assertThat(classifier.classify(SELECTED, () -> {
            reads.incrementAndGet(); return new SocketTimeoutException();
        })).isEmpty();
        assertThat(reads).hasValue(1);
    }

    @Test void absentGetterStaysUnclassified() {
        assertThat(new CloudArtifactReadFailureClassifier(true).classify(SELECTED, null)
                .orElseThrow().name()).isEqualTo("UNCLASSIFIED");
    }

    @Test void concurrentCallbacksReadOnlyOncePerWitness() throws Exception {
        var classifier = new CloudArtifactReadFailureClassifier(true);
        AtomicInteger reads = new AtomicInteger();
        try (var pool = Executors.newFixedThreadPool(8)) {
            List<Callable<Optional<CloudArtifactReadFailureClassifier.Category>>> calls = new ArrayList<>();
            for (int i = 0; i < 32; i++) calls.add(() -> classifier.classify(SELECTED, () -> {
                reads.incrementAndGet(); return new InterruptedByTimeoutException();
            }));
            int categories = 0;
            for (var future : pool.invokeAll(calls)) if (future.get().isPresent()) categories++;
            assertThat(categories).isEqualTo(1);
            assertThat(reads).hasValue(1);
        }
    }

    private static void category(Throwable cause, String expected) {
        AtomicInteger reads = new AtomicInteger();
        var classifier = new CloudArtifactReadFailureClassifier(true);
        assertThat(classifier.classify(SELECTED, () -> {
            reads.incrementAndGet(); return cause;
        }).orElseThrow().name()).isEqualTo(expected);
        assertThat(reads).hasValue(1);
    }

    private static CloudArtifactReadFailureClassifier.Scope scope(String connector, boolean seed,
            String operation, String command, String step, String type, int code) {
        return new CloudArtifactReadFailureClassifier.Scope(connector, seed, operation, command, step, type, code);
    }

    private static final class OpaqueCause extends IOException {
        private OpaqueCause(Throwable nested) { super(null, nested); }
        @Override public synchronized Throwable getCause() { throw new AssertionError("No cause traversal"); }
        @Override public String getMessage() { throw new AssertionError("No message access"); }
        @Override public String getLocalizedMessage() { throw new AssertionError("No localized message access"); }
        @Override public String toString() { throw new AssertionError("No serialization"); }
        @Override public StackTraceElement[] getStackTrace() { throw new AssertionError("No stack access"); }
    }
}
