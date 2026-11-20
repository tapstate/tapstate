package io.tapstate.core.logging;

import io.tapstate.core.common.FailureFact;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class FailureLogSnapshotTest {
    @Test
    void unavailableProgrammerFailureRetainsActualFramesWithoutReadingItsMessage() {
        var failure = new AssertionError() {
            @Override public String getMessage() { throw new AssertionError("message must not be read"); }
            @Override public String toString() { throw new AssertionError("toString must not be read"); }
        };
        failure.setStackTrace(new StackTraceElement[] {new StackTraceElement("example.Worker", "run", "Worker.java", 42)});
        var result = FailureLogSnapshot.of(FailureFact.unavailable(failure));
        assertThat((String) result.get("stackTrace")).contains(failure.getClass().getName(), "example.Worker.run(Worker.java:42)");
        assertThat(result).containsEntry("truncated", true);
    }

    @Test
    void alreadyCapturedDetailsAreNotReplacedByLaterThrowableChanges() {
        var fact = new FailureFact(new IllegalStateException("private"), Map.of("stackTrace", "captured safe frame"));
        assertThat(FailureLogSnapshot.of(fact)).isEqualTo(fact.diagnostic());
    }

    @Test
    void inaccessibleStackDoesNotReplaceTheOperationFailure() {
        var failure = new IllegalStateException() {
            @Override public StackTraceElement[] getStackTrace() { throw new IllegalStateException("private"); }
        };
        assertThat(FailureLogSnapshot.of(FailureFact.unavailable(failure)))
                .containsEntry("truncated", true);
    }

    @Test
    void inaccessibleCauseDoesNotReplaceTheOperationFailure() {
        var failure = new IllegalStateException() {
            @Override public synchronized Throwable getCause() { throw new LinkageError("private"); }
        };
        var result = FailureLogSnapshot.of(FailureFact.unavailable(failure));
        assertThat(result).containsEntry("truncated", true);
        assertThat((String) result.get("stackTrace")).contains(failure.getClass().getName()).doesNotContain("private");
    }

    @Test
    void cyclicCausesStayWithinTheStackBudget() {
        var first = new IllegalStateException("private one");
        var second = new IllegalArgumentException("private two", first);
        first.initCause(second);
        first.setStackTrace(new StackTraceElement[0]);
        second.setStackTrace(new StackTraceElement[0]);
        String stack = (String) FailureLogSnapshot.of(FailureFact.unavailable(first)).get("stackTrace");
        assertThat(stack.lines().count()).isLessThanOrEqualTo(32);
        assertThat(stack).doesNotContain("private");
    }
}
