package io.tapstate.testsupport;

import org.junit.jupiter.api.RepeatedTest;

// This annotation is reflection metadata; the holder is outside test-class discovery.
final class RepeatedInvocationMetadata {
    @RepeatedTest(3)
    void repeatedFixture() {
    }
}
