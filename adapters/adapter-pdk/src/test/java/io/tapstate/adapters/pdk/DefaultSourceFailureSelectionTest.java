package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DefaultSourceFailureSelectionTest {
    private static TapstateException controlled() {
        return new TapstateException(ConnectorError.TEST_FAILED, Map.of("connector", "fixture", "detail", "failed check"), null);
    }

    @Test void cleanupAloneRemainsTheOriginalDefect() {
        var cleanup = new IllegalStateException("close defect");
        assertThat(PdkConnectionTester.selectDefaultFailure(null, cleanup)).isSameAs(cleanup);
        assertThat(cleanup.getSuppressed()).isEmpty();
    }

    @Test void aCleanupDefectCannotBeMaskedByAControlledConnectionFailure() {
        var original = controlled(); var cleanup = new NullPointerException("close defect");
        assertThat(PdkConnectionTester.selectDefaultFailure(original, cleanup)).isSameAs(cleanup);
        assertThat(cleanup.getSuppressed()).containsExactly(original);
    }

    @Test void anOriginalProgrammingDefectKeepsItsIdentityAndTheCleanupDiagnostic() {
        var original = new LinkageError("original defect"); var cleanup = new IllegalStateException("close defect");
        assertThat(PdkConnectionTester.selectDefaultFailure(original, cleanup)).isSameAs(original);
        assertThat(original.getSuppressed()).containsExactly(cleanup);
    }

    @Test void aControlledCleanupCannotReplaceAnOriginalProgrammingDefect() {
        var original = new NullPointerException("original defect"); var cleanup = controlled();
        assertThat(PdkConnectionTester.selectDefaultFailure(original, cleanup)).isSameAs(original);
        assertThat(original.getSuppressed()).containsExactly(cleanup);
    }

    @Test void twoControlledFailuresPreserveTheOriginalConnectionFailure() {
        var original = controlled(); var cleanup = controlled();
        assertThat(PdkConnectionTester.selectDefaultFailure(original, cleanup)).isSameAs(original);
        assertThat(original.getSuppressed()).containsExactly(cleanup);
    }

    @Test void theSameFailureIsNeverSuppressedByItself() {
        var original = new IllegalStateException("shared defect");
        assertThat(PdkConnectionTester.selectDefaultFailure(original, original)).isSameAs(original);
        assertThat(original.getSuppressed()).isEmpty();
    }
}
