package io.tapstate.messages;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.dsl.DslError;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The error-code message catalog: the presentation-layer renderer that turns a coded exception into
 * a user-facing diagnostic via the bundled {@code messages/en.yml} catalog, substituting named
 * placeholders from the exception's arguments and falling back to the bare canonical code.
 */
class MessageCatalogTest {

    private static final MessageCatalog EN = MessageCatalog.bundled();

    @Test
    void rendersNamedPlaceholdersFromTheArgs() {
        MessageCatalog.Rendered r = EN.render(DslError.UNKNOWN_FIELD,
                Map.of("field", "srcc", "path", "source"));
        assertThat(r.message()).contains("srcc").contains("source").doesNotContain("{");
    }

    @Test
    void carriesASolutionHint() {
        MessageCatalog.Rendered r = EN.render(DslError.UNKNOWN_FIELD,
                Map.of("field", "srcc", "path", "source"));
        assertThat(r.solution()).isNotNull().isNotEmpty();
    }

    @Test
    void theRefusalAboutADottedNameCarriesTheSpellingThatAddressesIt() {
        // The one refusal that names a field the reader cannot address the obvious way, and so the one
        // whose hint has to do more than point elsewhere: saying it "can still be filtered on" and
        // stopping is a dead end, because the spelling that does it appears nowhere a reader would look.
        // Both forms, because which one they need depends on whether they are typing it or sending it.
        // Named by its literal code: this module cannot see the enum that declares it, and the golden
        // in arch-tests pins the string, so it cannot drift here unnoticed.
        MessageCatalog.Rendered r = EN.render("data-browser.unorderable-field",
                Map.of("field", "price\\.usd"));

        assertThat(r.solution())
                .as("the spelling as it is typed into the shell")
                .contains("price\\.usd");
        assertThat(r.solution())
                .as("and as it survives JSON, where a lone backslash is not an escape the format allows")
                .contains("\"price\\\\.usd\"");
    }

    @Test
    void theRefusalAboutADottedNameAlsoSaysWhatReadingItCostsInstead() {
        // The refusal stops an order, and then offers a filter as the way to reach the field. That offer
        // is the whole reason this hint has to name a cost: ordering is refused, so its cost is obvious,
        // while filtering succeeds and charges quietly. An index is written the same way a path is, so a
        // field whose name holds a dot cannot be indexed at all, and every read that matches on it walks
        // the collection. A hint that sends a reader down that path as though it were the free one is
        // wrong in the direction that never gets reported - the answers are right, only slow.
        MessageCatalog.Rendered r = EN.render("data-browser.unorderable-field",
                Map.of("field", "price\\.usd"));

        assertThat(r.solution())
                .as("that a filter on this field reads the whole collection")
                .contains("every row");
        assertThat(r.solution())
                .as("and why - so the cost reads as a property of the name, not as this read being unlucky")
                .contains("index");
    }

    @Test
    void schemaReadContentionNamesTheConnectionAndExplainsWhenToRetry() {
        MessageCatalog.Rendered rendered = EN.render("io.schema-read-contention",
                Map.of("connectionId", "orders-db"));

        assertThat(rendered.message()).contains("orders-db", "changed", "read attempt")
                .doesNotContain("{");
        assertThat(rendered.solution()).contains("re-discovery", "finish", "retry");
    }

    @Test
    void unknownCodeFallsBackToTheBareCanonicalCode() {
        TapstateErrorCode absent = new TapstateErrorCode() {
            @Override
            public String code() {
                return "cli.not-in-catalog-test";
            }

            @Override
            public Severity severity() {
                return Severity.ERROR;
            }

            @Override
            public Set<String> placeholders() {
                return Set.of();
            }
        };
        MessageCatalog.Rendered r = EN.render(absent, Map.of());
        assertThat(r.message()).isEqualTo("cli.not-in-catalog-test");
        assertThat(r.solution()).isNull();
    }

    @Test
    void everyDslCodeRendersFullyWithNoLeftoverPlaceholders() {
        for (DslError code : DslError.values()) {
            Map<String, Object> args = new HashMap<>();
            for (String placeholder : code.placeholders()) {
                args.put(placeholder, "X");
            }
            MessageCatalog.Rendered r = EN.render(code, args);
            assertThat(r.message())
                    .as("message for %s must be present in the catalog and fully substituted", code.code())
                    .isNotEqualTo(code.code())
                    .doesNotContain("{")
                    .doesNotContain("}");
        }
    }

    @Test
    void readerKeepsColonsInsideQuotedValues() {
        Map<String, MessageCatalog.Entry> parsed = MessageCatalog.parse("x.y:\n  message: \"a: b {z}\"\n");
        assertThat(parsed.get("x.y").message()).isEqualTo("a: b {z}");
        assertThat(parsed.get("x.y").solution()).isNull();
    }
}
