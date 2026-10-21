package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationJsonTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(8192, 100, 12);

    @Test void largeIntegersDecimalsAndExponentValuesRetainTheirExactValues() {
        var parsed = (List<?>) parse("[9007199254740992,9007199254740993,18446744073709551615,1.2300,1.25e-7]");
        assertThat(parsed.get(0)).isEqualTo(new BigInteger("9007199254740992"));
        assertThat(parsed.get(1)).isEqualTo(new BigInteger("9007199254740993"));
        assertThat(parsed.get(2)).isEqualTo(new BigInteger("18446744073709551615"));
        assertThat(parsed.get(3)).isEqualTo(new BigDecimal("1.2300"));
        assertThat(parsed.get(4)).isEqualTo(new BigDecimal("1.25e-7"));
        assertThat(parsed).noneMatch(value -> value instanceof Float || value instanceof Double);
    }

    @Test void structuralComparisonIgnoresObjectOrderButPreservesArrayOrderAndNumericDifferences() {
        assertThat(compare("{\"a\":1.00,\"b\":[true,null,\"001\"]}",
                "{\"b\":[true,null,\"001\"],\"a\":1}")).isEmpty();
        assertThat(compare("[1,2,2]", "[2,1,2]")).isPresent();
        assertThat(compare("[1,2,2]", "[1,2]")).isPresent();
        assertThat(compare("9007199254740992", "9007199254740993")).isPresent();
        assertThat(compare("{\"a\":null}", "{}")).get().extracting(ValidationValueComparison.Difference::kind)
                .isEqualTo(ValidationValueComparison.Kind.MISSING_TARGET);
    }

    @Test void nullEmptyShapesAndStringsDoNotBecomeOneAnother() {
        assertThat(parse("null")).isNull();
        assertThat(parse("\" 001 \"")).isEqualTo(" 001 ");
        assertThat(parse("\"true\"")).isEqualTo("true");
        assertThat(compare("\"001\"", "1")).isPresent();
        assertThat(compare("\"\"", "null")).isPresent();
        assertThat(compare("[]", "{}")).isPresent();
    }

    @Test void duplicateKeysIncludingNullAndEscapedNamesAreRejectedAtEveryDepth() {
        for (String text : List.of("{\"a\":1,\"a\":2}", "{\"a\":null,\"a\":null}",
                "{\"a\":1,\"\\u0061\":2}", "{\"child\":[{\"key\":1,\"key\":1}]}")) {
            assertCode(text, LIMITS, ValidationError.CODEC_FAILED);
        }
    }

    @Test void nonJsonExtensionsTrailingValuesAndMalformedTextAreNotAccepted() {
        for (String text : List.of("", " ", "[1,]", "{a:1}", "{'a':1}", "// comment\n1", "01",
                "NaN", "Infinity", "-Infinity", "+1", ".1", "1.", "[1", "{\"a\":", "{} {}", "null true")) {
            assertCode(text, LIMITS, ValidationError.CODEC_FAILED);
        }
    }

    @Test void unpairedSurrogatesAreRejectedAndAValidPairIsPreserved() {
        assertCode("\"\\ud800\"", LIMITS, ValidationError.CODEC_FAILED);
        assertCode("{\"\\udc00\":1}", LIMITS, ValidationError.CODEC_FAILED);
        assertThat(parse("\"\\ud83d\\ude00\"")).isEqualTo(new String(Character.toChars(0x1F600)));
    }

    @Test void inputDepthNodeTreeByteAndNumericExpansionLimitsAreEnforced() {
        assertCode("\"" + "x".repeat(100) + "\"", new ValidationValues.Limits(128, 100, 12),
                ValidationError.VALUE_LIMIT_EXCEEDED);
        assertCode("[[[0]]]", new ValidationValues.Limits(8192, 100, 2), ValidationError.VALUE_LIMIT_EXCEEDED);
        assertCode("[null,null,null]", new ValidationValues.Limits(8192, 3, 12), ValidationError.VALUE_LIMIT_EXCEEDED);
        assertCode("{\"a\":null,\"b\":null}", new ValidationValues.Limits(128, 100, 12),
                ValidationError.VALUE_LIMIT_EXCEEDED);
        assertCode("1e1000000", LIMITS, ValidationError.VALUE_LIMIT_EXCEEDED);
        assertCode("1e-1000000", LIMITS, ValidationError.VALUE_LIMIT_EXCEEDED);
        assertCode("1".repeat(4097), new ValidationValues.Limits(16384, 100, 12),
                ValidationError.VALUE_LIMIT_EXCEEDED);
    }

    @Test void parsedContainersAreImmutableAndCanEnterOwnedPortableRows() {
        var root = (Map<?, ?>) parse("{\"a\":[1,null]}");
        var array = (List<?>) root.get("a");
        assertThatThrownBy(root::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(array::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(ValidationValues.ownRow(Map.of("json", root), LIMITS).fields()).containsKey("json");
    }

    private static Object parse(String text) { return PdkValidationJson.parse(text, "fixture", "json", LIMITS); }

    private static java.util.Optional<ValidationValueComparison.Difference> compare(String left, String right) {
        // Explicit JSON typing is a caller decision; plain text does not invoke the parser.
        return ValidationValueComparison.compare(row(parse(left)), row(parse(right)));
    }

    private static ValidationValues.Row row(Object value) {
        return ValidationValues.ownRow(Map.of("json", new ValidationValues.Typed("json", "native-json", "binding", value)), LIMITS);
    }

    private static void assertCode(String text, ValidationValues.Limits limits, ValidationError expected) {
        assertThatThrownBy(() -> PdkValidationJson.parse(text, "fixture", "json", limits))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(expected));
    }
}
