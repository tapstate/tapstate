package io.tapstate.core.dsl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Configuration interpolation (grammar §9): {@code ${NAME}} reads a variable, {@code ${var:NAME:default}}
 * reads one with a fallback, and both are substituted once, when the document is loaded.
 *
 * <p>Interpolation runs on the raw text, before anything is parsed. That is what fixes the location
 * of the whole mechanism: the loading side substitutes from its own environment and sends values, so
 * the variables read are the author's own. It is also why these codes are pre-semantic — there is no
 * field path yet, only a line and a column, the same shape {@code TS0010017} carries.
 *
 * <p>The refusals matter as much as the substitutions. A reference that cannot be resolved must fail
 * loudly: passing {@code ${MONGO_URI}} through as a literal string hands a connector a host that does
 * not exist, and the failure then surfaces far from its cause.
 */
class InterpolatorTest {

    private static final UnaryOperator<String> NOTHING_SET = name -> null;

    private static UnaryOperator<String> env(String name, String value) {
        return Map.of(name, value)::get;
    }

    @Test
    @DisplayName("a bare reference is replaced with the variable's value")
    void replacesABareReferenceWithTheVariablesValue() {
        String out = Interpolator.interpolate(
                "uri: ${MONGO_URI}\n", env("MONGO_URI", "mongodb://127.0.0.1:27017/demo"));

        assertThat(out).isEqualTo("uri: mongodb://127.0.0.1:27017/demo\n");
    }

    @Test
    @DisplayName("every reference in the document is replaced, not just the first")
    void replacesEveryReferenceInTheDocument() {
        UnaryOperator<String> lookup = Map.of("HOST", "db.example.com", "PORT", "3306")::get;

        String out = Interpolator.interpolate("host: ${HOST}\nport: ${PORT}\n", lookup);

        assertThat(out).isEqualTo("host: db.example.com\nport: 3306\n");
    }

    @Test
    @DisplayName("a document with no references comes back byte for byte")
    void leavesADocumentWithNoReferencesByteForByteAlone() {
        String yaml = "version: tapstate/v1\nkind: source\nid: src\n";

        assertThat(Interpolator.interpolate(yaml, NOTHING_SET)).isEqualTo(yaml);
    }

    @Test
    @DisplayName("a dollar that opens no reference is left alone (passwords hold them)")
    void leavesADollarThatOpensNoReferenceAlone() {
        String yaml = "password: pa$$w0rd$\n";

        assertThat(Interpolator.interpolate(yaml, NOTHING_SET)).isEqualTo(yaml);
    }

    @Test
    @DisplayName("an undefined variable is refused, never passed through as a literal")
    void refusesAnUndefinedVariableRatherThanPassingTheLiteralThrough() {
        DslException ex = catchThrowableOfType(
                () -> Interpolator.interpolate("uri: ${MONGO_URI}\n", NOTHING_SET), DslException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.code().code()).isEqualTo("TS0010018");
        assertThat(ex.args()).containsEntry("name", "MONGO_URI");
    }

    @Test
    @DisplayName("the default is used when the variable is unset")
    void fallsBackToTheDefaultWhenTheVariableIsUnset() {
        String out = Interpolator.interpolate("port: ${var:PORT:3306}\n", NOTHING_SET);

        assertThat(out).isEqualTo("port: 3306\n");
    }

    @Test
    @DisplayName("a set variable wins over the default")
    void prefersTheSetVariableOverTheDefault() {
        String out = Interpolator.interpolate("port: ${var:PORT:3306}\n", env("PORT", "5432"));

        assertThat(out).isEqualTo("port: 5432\n");
    }

    @Test
    @DisplayName("colons inside a default are part of the default, not separators")
    void keepsColonsInsideTheDefaultValue() {
        String out = Interpolator.interpolate(
                "uri: ${var:MONGO_URI:mongodb://127.0.0.1:27017/demo}\n", NOTHING_SET);

        assertThat(out).isEqualTo("uri: mongodb://127.0.0.1:27017/demo\n");
    }

    @Test
    @DisplayName("an empty default is a default, and resolves to the empty string")
    void acceptsAnEmptyDefault() {
        assertThat(Interpolator.interpolate("note: '${var:NOTE:}'\n", NOTHING_SET))
                .isEqualTo("note: ''\n");
    }

    @Test
    @DisplayName("an unknown prefix is refused rather than read as a variable named 'secret:TOKEN'")
    void refusesAnUnknownInterpolationPrefix() {
        DslException ex = catchThrowableOfType(
                () -> Interpolator.interpolate("token: ${secret:TOKEN}\n", NOTHING_SET), DslException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.code().code()).isEqualTo("TS0010019");
        assertThat(ex.args().get("ref").toString()).contains("secret:TOKEN");
    }

    @Test
    @DisplayName("the var form with no default is refused (a bare reference is the form without one)")
    void refusesTheVarFormWithNoDefault() {
        DslException ex = catchThrowableOfType(
                () -> Interpolator.interpolate("port: ${var:PORT}\n", env("PORT", "3306")), DslException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.code().code()).isEqualTo("TS0010019");
    }

    @Test
    @DisplayName("an unclosed reference is refused")
    void refusesAnUnclosedReference() {
        DslException ex = catchThrowableOfType(
                () -> Interpolator.interpolate("uri: ${MONGO_URI\nkind: source\n", NOTHING_SET),
                DslException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.code().code()).isEqualTo("TS0010019");
        // the reference reported stops at the line's end — it does not swallow the rest of the document
        assertThat(ex.args().get("ref").toString()).doesNotContain("kind: source");
    }

    @Test
    @DisplayName("a name that is not a variable name at all is refused")
    void refusesANameThatIsNotAVariableName() {
        DslException ex = catchThrowableOfType(
                () -> Interpolator.interpolate("uri: ${not a name}\n", NOTHING_SET), DslException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.code().code()).isEqualTo("TS0010019");
    }

    @Test
    @DisplayName("a failure is located at the line and column the reference opens on")
    void locatesAFailureAtItsLineAndColumn() {
        DslException ex = catchThrowableOfType(
                () -> Interpolator.interpolate("kind: source\nconfig:\n  uri: ${MONGO_URI}\n", NOTHING_SET),
                DslException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.line()).isEqualTo(3);
        assertThat(ex.column()).isEqualTo(8);
    }

    @Test
    @DisplayName("pre-semantic: the args carry only the code's own placeholders, no empty path leaks in")
    void carriesNoPathBecauseNothingIsParsedYet() {
        DslException undefined = catchThrowableOfType(
                () -> Interpolator.interpolate("uri: ${MONGO_URI}\n", NOTHING_SET), DslException.class);
        DslException malformed = catchThrowableOfType(
                () -> Interpolator.interpolate("uri: ${secret:X}\n", NOTHING_SET), DslException.class);

        assertThat(undefined.args()).containsOnlyKeys("name");
        assertThat(malformed.args()).containsOnlyKeys("ref");
    }
}
