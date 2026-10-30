package io.tapstate.testsupport;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Disabled("source-bound container skip control")
class CiDisabledContainerEvidenceTest {
    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void disabledByClass(String value) {
        throw new AssertionError("a disabled container must not invoke providers or tests");
    }

    @Test
    void ordinaryDisabledByClass() {
        throw new AssertionError("a disabled container must not execute");
    }

    @Nested
    class NestedContainer {
        @Test
        void ordinaryNestedDisabled() {
            throw new AssertionError("a disabled nested container must not execute");
        }

        @ParameterizedTest
        @ValueSource(strings = {"nested"})
        void disabledByEnclosingClass(String value) {
            throw new AssertionError("a disabled enclosing container must not execute nested tests");
        }
    }
}
