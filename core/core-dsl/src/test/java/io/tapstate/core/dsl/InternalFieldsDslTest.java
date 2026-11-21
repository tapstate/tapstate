package io.tapstate.core.dsl;

import io.tapstate.core.model.InternalFields;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalFieldsDslTest {
    private static final String PREFIX = """
            version: tapstate/v1
            kind: serve
            id: target
            sync:
              - id: output
                source: warehouse
            """;

    @Test
    void aTargetThatOptsOutKeepsThatChoiceThroughCanonicalRoundTrip() {
        String declared = PREFIX + "    internal_fields: strip\n";
        DslParser parser = new DslParser();
        CanonicalWriter writer = new CanonicalWriter();

        ServeResource parsed = (ServeResource) parser.parse(declared);
        assertThat(parsed.sync().get(0).internalFields()).isEqualTo(InternalFields.STRIP);
        String canonical = writer.write(parsed);
        assertThat(canonical).isEqualTo(declared);
        assertThat(writer.write(parser.parse(canonical))).isEqualTo(canonical);
    }

    @Test
    void explicitDeliverAndOmissionHaveTheSameCanonicalForm() {
        CanonicalWriter writer = new CanonicalWriter();
        DslParser parser = new DslParser();
        assertThat(writer.write(parser.parse(PREFIX + "    internal_fields: deliver\n")))
                .isEqualTo(writer.write(parser.parse(PREFIX)));
    }

    @Test
    void refusesAnUnknownValueWithoutTreatingItAsDeliver() {
        assertThatThrownBy(() -> new DslParser().parse(PREFIX + "    internal_fields: hide\n"))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.path()).isEqualTo("sync[0].internal_fields");
                    assertThat(error.code()).isEqualTo(DslError.ILLEGAL_VALUE);
                });
    }
}
