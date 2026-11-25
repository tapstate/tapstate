package io.tapstate.testsupport;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CiInheritedDisabledEvidenceTest extends CiInheritedDisabledBase {
    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void inheritedComposedDisabled(String value) {
        throw new AssertionError("an inherited composed annotation must prevent provider execution");
    }
}
