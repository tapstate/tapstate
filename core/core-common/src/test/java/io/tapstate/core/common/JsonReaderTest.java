package io.tapstate.core.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class JsonReaderTest {

    @Test
    void parsesScalars() {
        assertThat(JsonReader.parse("\"hello\"")).isEqualTo("hello");
        assertThat(JsonReader.parse("true")).isEqualTo(Boolean.TRUE);
        assertThat(JsonReader.parse("false")).isEqualTo(Boolean.FALSE);
        assertThat(JsonReader.parse("null")).isNull();
    }

    @Test
    void parsesIntegersAsLongAndDecimalsAsDouble() {
        assertThat(JsonReader.parse("3306")).isEqualTo(3306L);
        assertThat(JsonReader.parse("-12")).isEqualTo(-12L);
        assertThat(JsonReader.parse("1.5")).isEqualTo(1.5d);
        assertThat(JsonReader.parse("-2.5e3")).isEqualTo(-2500.0d);
    }

    @Test
    void parsesIntegersThatOverflowLongAsBigInteger() {
        assertThat(JsonReader.parse("18446744073709551615"))
                .isEqualTo(new java.math.BigInteger("18446744073709551615"));
    }

    @Test
    void parsesDecimalsBeyondDoublesRangeExactlyRatherThanAsAnInfinity() {
        // A connector's spec states a column's bounds as literals, and a 128-bit decimal's are far past
        // what a double holds. Parsed as one they become an infinity, whose text form is the word
        // "Infinity" - not a number at all, and rejected by whatever reads the bound back, which drops
        // the whole type rather than the bound. Same call this already makes for an integer past long.
        assertThat(JsonReader.parse("1E+6145")).isEqualTo(new java.math.BigDecimal("1E+6145"));
        assertThat(JsonReader.parse("-1E+6145")).isEqualTo(new java.math.BigDecimal("-1E+6145"));
        // What a double does hold is still a double: this widens nothing that already worked.
        assertThat(JsonReader.parse("1.5")).isInstanceOf(Double.class);
    }

    @Test
    void parsesDecimalsBelowDoublesRangeExactlyRatherThanAsZero() {
        // The mirror of the case above, and the quieter half: a type states both of its bounds, so a
        // literal that reaches one reaches the other. Under what a double holds the parse answers zero,
        // which unlike an infinity is well-formed and reads as a bound somebody meant.
        assertThat(JsonReader.parse("1E-6143")).isEqualTo(new java.math.BigDecimal("1E-6143"));
        assertThat(JsonReader.parse("-1E-6143")).isEqualTo(new java.math.BigDecimal("-1E-6143"));
        // A literal that is itself zero is not that, and stays the double it always was.
        assertThat(JsonReader.parse("0.0")).isInstanceOf(Double.class);
        assertThat(JsonReader.parse("-0.0")).isInstanceOf(Double.class);
    }

    @Test
    void parsesStringEscapesAndUnicode() {
        assertThat(JsonReader.parse("\"a\\\"b\\\\c\\n\"")).isEqualTo("a\"b\\c\n");
        assertThat(JsonReader.parse("\"\\u20ac\"")).isEqualTo("€");
        assertThat(JsonReader.parse("\"€\"")).isEqualTo("€");
        assertThat(JsonReader.parse("\"a\\/b\"")).isEqualTo("a/b");
    }

    @Test
    void parsesEmptyAndNestedContainers() {
        assertThat(JsonReader.parse("{}")).isEqualTo(Map.of());
        assertThat(JsonReader.parse("[]")).isEqualTo(List.of());
        assertThat(JsonReader.parse("[\"a\", \"b\"]")).isEqualTo(List.of("a", "b"));
        assertThat(JsonReader.parse("{ \"k\": { \"n\": [true, null] } }")).isInstanceOf(Map.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void preservesObjectKeyOrder() {
        Map<String, Object> tree = (Map<String, Object>) JsonReader.parse("{\"b\":1,\"a\":2,\"c\":3}");
        assertThat(tree.keySet()).containsExactly("b", "a", "c");
    }

    @Test
    void toleratesInsignificantWhitespace() {
        assertThat(JsonReader.parse("  {  \"a\" :\n \"x\" }  ")).isEqualTo(Map.of("a", "x"));
    }

    @Test
    void rejectsMalformedInput() {
        assertThatThrownBy(() -> JsonReader.parse("{\"a\":}")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonReader.parse("[1,2")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonReader.parse("{\"a\":1} trailing"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsPathologicallyDeepNestingWithoutAStackOverflow() {
        String deep = "[".repeat(5000) + "]".repeat(5000);
        assertThatThrownBy(() -> JsonReader.parse(deep)).isInstanceOf(IllegalArgumentException.class);
    }
}
