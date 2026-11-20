package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PdkSinkWriterFailureDetailsTest {

    @Test
    void aWriteFailureKeepsTheRawMessageAndOriginalStackAsTechnicalFacts() {
        var cause = new IllegalStateException("fail_writes raw-driver-marker");
        var originalStack = cause.getStackTrace();
        var failure = PdkSinkWriter.writeFailed("fixture", cause);
        assertThat(failure.code()).isEqualTo(ConnectorError.WRITE_FAILED);
        assertThat(failure.args()).containsEntry("connector", "fixture")
                .containsEntry("detail", "fail_writes raw-driver-marker");
        assertThat(failure.getCause()).isSameAs(cause);
        assertThat(failure.getCause().getStackTrace()).containsExactly(originalStack);
    }

    @Test
    void aFailureWithoutAMessageKeepsItsTypeAndOriginalCause() {
        RuntimeException original = new RuntimeException();

        TapstateException failure = PdkSinkWriter.writeFailed("demo", original);

        assertThat(failure.code()).isEqualTo(ConnectorError.WRITE_FAILED);
        assertThat(failure.args()).containsEntry("connector", "demo")
                .containsEntry("detail", "RuntimeException");
        assertThat(failure.getCause()).isSameAs(original);
    }
}
