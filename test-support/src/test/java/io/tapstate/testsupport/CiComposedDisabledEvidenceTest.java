package io.tapstate.testsupport;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@CiDisabled
class CiComposedDisabledEvidenceTest {
    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void composedDisabled(String value) {
        throw new AssertionError("a composed annotation must prevent provider execution");
    }
}
