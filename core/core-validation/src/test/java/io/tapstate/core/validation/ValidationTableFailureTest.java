package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class ValidationTableFailureTest {
    @ParameterizedTest
    @ValueSource(strings = {"validation.read-failed", "TS0300015"})
    void retainsTheOriginalDiagnosticIdentity(String code) {
        var failure = new ValidationTableFailure("orders", ValidationTableFailure.Side.TARGET, code, "Read failed");
        assertThat(failure.code()).isEqualTo(code);
        assertThat(failure.message()).isEqualTo("Read failed");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TS0000015", "TS0300000", " TS0300015", "TS0300015x", "TS030001", "TS03000155"})
    void malformedProductCodesCannotBecomeStoredFacts(String code) {
        assertThatThrownBy(() -> new ValidationTableFailure("orders", ValidationTableFailure.Side.TARGET, code, "Read failed"))
                .isInstanceOf(TapstateException.class);
    }
}
