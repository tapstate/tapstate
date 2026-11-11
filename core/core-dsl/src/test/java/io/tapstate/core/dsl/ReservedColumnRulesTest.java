package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.ReservedField;
import io.tapstate.core.common.ReservedFieldError;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.Resource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReservedColumnRulesTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: cosmos_main
            connector: mysql
            """;

    private static DiscoveredTable events(Map<String, String> renamed) {
        return new DiscoveredTable("events",
                Map.of("id", TapstateType.INT64, ReservedField.DEFAULT_SOURCE_RENAME, TapstateType.STRING),
                List.of("id"), null, renamed);
    }

    @Test
    @DisplayName("a renamed source column is reported once, naming the source, table, column and new name")
    void aRenamedColumnIsReported() {
        Resource source = new DslParser().parse(SOURCE);

        List<Advisory> findings = ReservedColumnRules.review(List.of(source), Map.of("cosmos_main",
                List.of(events(Map.of(ReservedField.DEFAULT_SOURCE_RENAME, ReservedField.NAME)))));

        assertThat(findings).singleElement().satisfies(finding -> {
            assertThat(finding.code()).isEqualTo(ReservedFieldError.RENAMED);
            assertThat(finding.params()).containsOnlyKeys(ReservedFieldError.RENAMED.placeholders())
                    .containsEntry("source", "cosmos_main").containsEntry("table", "events")
                    .containsEntry("column", ReservedField.NAME)
                    .containsEntry("renamed", ReservedField.DEFAULT_SOURCE_RENAME);
        });
    }

    @Test
    @DisplayName("a column that merely has the new name, with nothing renamed, is not reported")
    void aColumnThatOnlyLooksRenamedIsNotReported() {
        Resource source = new DslParser().parse(SOURCE);

        assertThat(ReservedColumnRules.review(List.of(source),
                Map.of("cosmos_main", List.of(events(Map.of()))))).isEmpty();
    }

    @Test
    @DisplayName("an expression reading the reserved field is refused when it is compiled")
    void anExpressionReadingTheReservedFieldIsRefused() {
        String step = """
                version: tapstate/v1
                kind: transform
                id: peek
                type: map
                fields: { seen: "=after.%s" }
                """.formatted(ReservedField.NAME);

        assertThatThrownBy(() -> new DslParser().parse(step))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.ILLEGAL_EXPRESSION);
                    assertThat(error.path()).isEqualTo("fields.seen");
                });
    }

    @Test
    @DisplayName("a rename rule reading the reserved field is refused too")
    void aRenameRuleReadingTheReservedFieldIsRefused() {
        String step = """
                version: tapstate/v1
                kind: transform
                id: peek
                type: map
                fields: { seen: "$%s" }
                """.formatted(ReservedField.NAME);

        assertThatThrownBy(() -> new DslParser().parse(step))
                .isInstanceOfSatisfying(DslException.class, error ->
                        assertThat(error.code()).isEqualTo(DslError.ILLEGAL_EXPRESSION));
    }
}
