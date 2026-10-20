package io.tapstate.testsupport;

import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class CiInjectedEvidenceTest {
    @ParameterizedTest
    @ValueSource(strings = {"case"})
    void injectedInfo(String value, TestInfo info) {
        assertThat(value).isEqualTo("case");
        assertThat(info.getTestMethod()).hasValueSatisfying(method ->
                assertThat(method.getName()).isEqualTo("injectedInfo"));
    }

    @RepeatedTest(2)
    void injectedRepetition(RepetitionInfo info) {
        assertThat(info.getTotalRepetitions()).isEqualTo(2);
        assertThat(info.getCurrentRepetition()).isBetween(1, 2);
    }

    @ParameterizedTest
    @MethodSource("textBlocks")
    void escapedTextBlock(String value) {
        assertThat(value).isEqualTo("C:\\temp\\n\\r\\\"\\\\\n");
    }

    static Stream<String> textBlocks() {
        return Stream.of("""
                C:\\temp\\n\\r\\\"\\\\
                """);
    }

    @ParameterizedTest
    @MethodSource("indentedTextBlocks")
    void closingDelimiterPreservesSignificantIndent(String value) {
        assertThat(value).isEqualTo("    content\n");
    }

    static Stream<String> indentedTextBlocks() {
        return Stream.of("""
            content
        """);
    }

    @Disabled("source-bound skip control")
    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void disabledParameterized(String value) {
        throw new AssertionError("a disabled provider must never execute");
    }

    @RepeatedTest(2)
    @Disabled
    void disabledRepeated(RepetitionInfo info) {
        throw new AssertionError("a disabled repetition must never execute");
    }

    @Disabled("source-bound varargs skip control")
    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    void disabledVarargs(String... values) {
        throw new AssertionError("disabled varargs must not invoke their provider");
    }
}
