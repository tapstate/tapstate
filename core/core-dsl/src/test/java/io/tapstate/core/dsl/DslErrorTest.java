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
            assertThat(e.code()).matches("TS001(?!0000)[0-9]{4}");
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }

    @Test
    void carriesTheCorpusVocabularyCodes() {
        assertThat(DslError.values()).extracting(DslError::code).containsExactlyInAnyOrder(
                "TS0010001",
                "TS0010002",
                "TS0010003",
                "TS0010004",
                "TS0010005",
                "TS0010006",
                "TS0010007",
                "TS0010008",
                "TS0010009",
                "TS0010010",
                "TS0010011",
                "TS0010012",
                "TS0010013",
                "TS0010014",
                "TS0010015",
                "TS0010016",
                // pre-semantic syntax error — no corpus witness (a syntax error cannot be well-formed)
                "TS0010017",
                // pre-semantic interpolation errors — no corpus witness either, since they are raised on
                // raw text before the parse and turn on the environment, not on the document
                "TS0010018",
                "TS0010019",
                // post-semantic row-expression type errors — the artifact is well-formed and complete,
                // but the verdict turns on a source's discovered column types, which it does not carry
                "TS0010020",
                "TS0010021",
                "TS0010022",
                // post-semantic too - whether a table declares a key is a property of the table,
                // carried only by a discovered model and never by the document naming it
                "TS0010023",
                // the unwind gate, and the other way round from the one above: what an expansion
                // says about its own rows, and which sync those rows reach, are both written in the
                // document, so both are witnessed by an ordinary corpus case
                "TS0010024",
                "TS0010025",
                // a step output field taking the name kept for product-added row data
                "TS0010037",
                // Source columns are available during assembly, not offline corpus validation.
                "TS0010027",
                // the write-target gate: which connectors may be written to is a property of the
                // deployment applied to, not of the document, so it is raised on the apply path
                "TS0010026",
                // the join SQL gate: both are raised while reading the artifact, so both are
                // witnessed by an ordinary corpus case
                "TS0010028",
                "TS0010029",
                // a join reading another step or a pattern: the wiring is written in the document, so
                // an ordinary corpus case witnesses each
                "TS0010030",
                "TS0010031", "TS0010034",
                "TS0010035", "TS0010036");

    }

    @Test
    void declaresThePlaceholderContractPerCode() {
        assertThat(DslError.UNKNOWN_FIELD.placeholders()).containsExactlyInAnyOrder("field", "path");
        assertThat(DslError.FORBIDDEN_FIELD.placeholders()).containsExactlyInAnyOrder("field", "path");
        assertThat(DslError.MISSING_FIELD.placeholders()).containsExactlyInAnyOrder("field", "path");
        assertThat(DslError.MISSING_REFERENCE.placeholders()).containsExactlyInAnyOrder("ref", "path");
        assertThat(DslError.AMBIGUOUS_REFERENCE.placeholders()).containsExactlyInAnyOrder("ref", "path");
        assertThat(DslError.MODE_MISMATCH.placeholders()).containsExactlyInAnyOrder("field", "mode", "path");
        // the document the offending element is written in is named too: a path alone does not say
        // which of several pipelines in a batch, nor which serve definition carries the element
        assertThat(DslError.UNSUPPORTED_TARGET_CONNECTOR.placeholders())
                .containsExactlyInAnyOrder("connector", "source", "resource", "supported", "path");
        // the pipeline is what made the field required, so it is named alongside the source that lacks it
        assertThat(DslError.MODE_REQUIRED_FOR_READ.placeholders())
                .containsExactlyInAnyOrder("source", "pipeline", "path");
        assertThat(DslError.ILLEGAL_VALUE.placeholders()).containsExactlyInAnyOrder("value", "expected", "path");
        assertThat(DslError.ILLEGAL_EXPRESSION.placeholders()).containsExactlyInAnyOrder("expr", "detail", "path");
        assertThat(DslError.JOIN_SQL_NOT_PARSABLE.placeholders())
                .containsExactlyInAnyOrder("detail", "path");
        assertThat(DslError.JOIN_SQL_UNSUPPORTED.placeholders())
                .containsExactlyInAnyOrder("shape", "line", "column", "path");
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
        // the step is the whole diagnosis: which expansion cannot tell its own rows apart
        assertThat(DslError.UNWIND_NEEDS_AN_ELEMENT_KEY.placeholders())
                .containsExactlyInAnyOrder("step", "path");
        // both ends are named, because either one is a legitimate thing to change
        assertThat(DslError.UNWIND_NEEDS_AN_UPSERT_TARGET.placeholders())
                .containsExactlyInAnyOrder("step", "sync", "path");
    }

    @Test
    void resolvesByCorpusVocabularySymbol() {
        for (DslError code : DslError.values()) {
            assertThat(code.symbol()).matches("[a-z]+(-[a-z]+)*");
            assertThat(DslError.ofSymbol(code.symbol())).isSameAs(code);
        }
        assertThat(DslError.ofSymbol("unknown-field")).isSameAs(DslError.UNKNOWN_FIELD);
        assertThat(DslError.ofSymbol("mode-mismatch")).isSameAs(DslError.MODE_MISMATCH);
    }

    @Test
    void symbolLookupRejectsUnknownVocabulary() {
        assertThatThrownBy(() -> DslError.ofSymbol("no-such-rule"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
