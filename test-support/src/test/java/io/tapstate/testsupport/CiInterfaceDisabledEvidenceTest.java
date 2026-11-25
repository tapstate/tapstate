package io.tapstate.testsupport;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CiInterfaceDisabledEvidenceTest implements CiDisabledInterface {
    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void interfaceDisabled(String value) {
        throw new AssertionError("a disabled interface must prevent provider execution");
    }
}
