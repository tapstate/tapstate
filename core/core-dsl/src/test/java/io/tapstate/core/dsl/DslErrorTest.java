package io.tapstate.core.dsl;

import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DslErrorTest {

    @Test
    void everyCodeIsInTheDslDomainAndErrorSeverity() {
        for (DslError e : DslError.values()) {
            assertThat(e.code()).startsWith("dsl.");
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }

    @Test
    void carriesTheCorpusVocabularyCodes() {
        assertThat(DslError.values()).extracting(DslError::code).containsExactlyInAnyOrder(
                "dsl.unsupported-version",
                "dsl.unknown-field",
                "dsl.forbidden-field",
                "dsl.missing-reference",
                "dsl.ambiguous-reference",
                "dsl.mode-mismatch",
                "dsl.illegal-value",
                "dsl.illegal-expression",
                "dsl.composition",
                "dsl.duplicate-id",
                "dsl.unsupported-mode",
                "dsl.config-type-mismatch",
                "dsl.config-required",
                "dsl.invalid-config-value",
                // pre-semantic syntax error — no corpus witness (a syntax error cannot be well-formed)
                "dsl.malformed-yaml",
                // pre-semantic interpolation errors — no corpus witness either, since they are raised on
                // raw text before the parse and turn on the environment, not on the document
                "dsl.undefined-variable",
                "dsl.malformed-interpolation",
                // post-semantic row-expression type errors — the artifact is well-formed and complete,
                // but the verdict turns on a source's discovered column types, which it does not carry
                "dsl.row-expression-needs-discovery",
                "dsl.row-expression-type-unsupported",
                "dsl.row-expression-type-unknown",
                // post-semantic too - whether a table declares a key is a property of the table,
                // carried only by a discovered model and never by the document naming it
                "dsl.upsert-needs-key");
    }

    @Test
    void declaresThePlaceholderContractPerCode() {
        assertThat(DslError.UNKNOWN_FIELD.placeholders()).containsExactlyInAnyOrder("field", "path");
        assertThat(DslError.FORBIDDEN_FIELD.placeholders()).containsExactlyInAnyOrder("field", "path");
        assertThat(DslError.MISSING_REFERENCE.placeholders()).containsExactlyInAnyOrder("ref", "path");
        assertThat(DslError.AMBIGUOUS_REFERENCE.placeholders()).containsExactlyInAnyOrder("ref", "path");
        assertThat(DslError.MODE_MISMATCH.placeholders()).containsExactlyInAnyOrder("field", "mode", "path");
        assertThat(DslError.ILLEGAL_VALUE.placeholders()).containsExactlyInAnyOrder("value", "expected", "path");
        assertThat(DslError.ILLEGAL_EXPRESSION.placeholders()).containsExactlyInAnyOrder("expr", "detail", "path");
        assertThat(DslError.COMPOSITION.placeholders()).containsExactlyInAnyOrder("detail", "path");
        assertThat(DslError.DUPLICATE_ID.placeholders()).containsExactlyInAnyOrder("id", "path");
        assertThat(DslError.UNSUPPORTED_MODE.placeholders())
                .containsExactlyInAnyOrder("connector", "mode", "allowed", "path");
        assertThat(DslError.CONFIG_TYPE_MISMATCH.placeholders())
                .containsExactlyInAnyOrder("connector", "field", "expected", "path");
        assertThat(DslError.CONFIG_REQUIRED.placeholders())
                .containsExactlyInAnyOrder("connector", "field", "path");
        assertThat(DslError.INVALID_CONFIG_VALUE.placeholders())
                .containsExactlyInAnyOrder("connector", "field", "value", "allowed", "path");
        // malformed-yaml is pre-semantic: it carries only the parser detail, no field path
        assertThat(DslError.MALFORMED_YAML.placeholders()).containsExactlyInAnyOrder("detail");
        // the interpolation codes are pre-semantic too: the offending variable / reference, no field path
        assertThat(DslError.UNDEFINED_VARIABLE.placeholders()).containsExactlyInAnyOrder("name");
        assertThat(DslError.MALFORMED_INTERPOLATION.placeholders()).containsExactlyInAnyOrder("ref");
        // the row-expression type codes name the column the verdict is about, so the author is told
        // which one to change rather than only that the expression was refused - and the table it was
        // read from, since an expression reading several is judged against each of them in turn
        assertThat(DslError.ROW_EXPRESSION_NEEDS_DISCOVERY.placeholders())
                .containsExactlyInAnyOrder("expr", "source", "path");
        assertThat(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED.placeholders())
                .containsExactlyInAnyOrder("expr", "column", "type", "table", "path");
        assertThat(DslError.ROW_EXPRESSION_TYPE_UNKNOWN.placeholders())
                .containsExactlyInAnyOrder("expr", "column", "table", "path");
        assertThat(DslError.UPSERT_NEEDS_KEY.placeholders())
                .containsExactlyInAnyOrder("table", "source", "path");
    }

    @Test
    void resolvesByCorpusVocabularySymbol() {
        assertThat(DslError.ofSymbol("unknown-field")).isSameAs(DslError.UNKNOWN_FIELD);
        assertThat(DslError.ofSymbol("mode-mismatch")).isSameAs(DslError.MODE_MISMATCH);
    }

    @Test
    void symbolLookupRejectsUnknownVocabulary() {
        assertThatThrownBy(() -> DslError.ofSymbol("no-such-rule"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
