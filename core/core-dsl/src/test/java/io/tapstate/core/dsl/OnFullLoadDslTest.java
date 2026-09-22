package io.tapstate.core.dsl;

import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OnFullLoadDslTest {
    private static final String PREFIX = """
            version: tapstate/v1
            kind: serve
            id: target
            sync:
              - id: output
                source: warehouse
            """;

    @ParameterizedTest
    @ValueSource(strings = {"clear", "fail"})
    void preservesExplicitFullLoadPolicyThroughCanonicalRoundTrip(String policy) {
        String declared = PREFIX + "    on_full_load: " + policy + "\n";
        DslParser parser = new DslParser();
        CanonicalWriter writer = new CanonicalWriter();
        String canonical = writer.write(parser.parse(declared));
        assertThat(canonical).isEqualTo(declared);
        assertThat(writer.write(parser.parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void explicitAppendAndOmissionHaveTheSameCanonicalForm() {
        CanonicalWriter writer = new CanonicalWriter();
        DslParser parser = new DslParser();
        assertThat(writer.write(parser.parse(PREFIX + "    on_full_load: append\n")))
                .isEqualTo(writer.write(parser.parse(PREFIX)));
    }

    @Test
    void refusesUnknownPolicyWithoutTreatingItAsAppend() {
        assertThatThrownBy(() -> new DslParser().parse(PREFIX + "    on_full_load: truncate\n"))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.path()).isEqualTo("sync[0].on_full_load");
                    assertThat(error.code()).isEqualTo(DslError.ILLEGAL_VALUE);
                });
    }
}
