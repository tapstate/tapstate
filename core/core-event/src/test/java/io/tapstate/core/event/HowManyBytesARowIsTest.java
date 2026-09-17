package io.tapstate.core.event;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.DynamicTest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * What a row's payload costs, rule by rule. The table below is the definition: each row names a value,
 * the kind it is charged as, and what it costs. Changing any figure here is changing the product's own
 * meaning of a byte, which is why they are written out rather than computed.
 *
 * <p>The table is also held to the type namespace: every kind in it but one has a case here, and the
 * one that has none is named with its reason. A kind added to the namespace arrives with no width
 * decided, and both this and the switch that charges it go red - the switch at compile time, having no
 * default arm to fall into.
 *
 * <p>Text cases are written as escapes on purpose. This repository is English-only, so the characters
 * whose widths are being pinned cannot appear in it literally; the escape is the same character to the
 * compiler and keeps the source to one alphabet.
 */
class HowManyBytesARowIsTest {

    /** One rule of the definition: a value, the kind it is charged as, and what it costs. */
    private record Charge(String what, Object value, TapstateType kind, long bytes) {
    }

    private static List<Charge> theDefinition() {
        List<Charge> charges = new ArrayList<>();
        charges.add(new Charge("ascii text", "abc", TapstateType.STRING, 3));
        charges.add(new Charge("two-byte text", "é", TapstateType.STRING, 2));
        charges.add(new Charge("three-byte text", "中", TapstateType.STRING, 3));
        charges.add(new Charge("a surrogate pair", "😀", TapstateType.STRING, 4));
        charges.add(new Charge("an unpaired surrogate", "\uD83D", TapstateType.STRING, 3));
        charges.add(new Charge("empty text", "", TapstateType.STRING, 0));
        charges.add(new Charge("an exact decimal", new BigDecimal("12.3400"), TapstateType.DECIMAL, 7));
        charges.add(new Charge("a negative exact decimal", new BigDecimal("-0.5"), TapstateType.DECIMAL, 4));
        charges.add(new Charge("a whole number too wide for the namespace",
                new BigInteger("123456789012345678901234567890"), TapstateType.DECIMAL, 30));
        charges.add(new Charge("a small whole number", 7L, TapstateType.INT64, 8));
        charges.add(new Charge("the widest whole number", Long.MAX_VALUE, TapstateType.INT64, 8));
        charges.add(new Charge("an int", 7, TapstateType.INT64, 8));
        charges.add(new Charge("a short", (short) 7, TapstateType.INT64, 8));
        charges.add(new Charge("a byte", (byte) 7, TapstateType.INT64, 8));
        charges.add(new Charge("a double", 1.5d, TapstateType.DOUBLE, 8));
        charges.add(new Charge("a float", 1.5f, TapstateType.DOUBLE, 8));
        charges.add(new Charge("a boolean", true, TapstateType.BOOLEAN, 1));
        charges.add(new Charge("a tagged byte string",
                new Bytes((byte) 4, new byte[] {1, 2, 3}), TapstateType.BINARY, 3));
        charges.add(new Charge("a bare byte string", new byte[] {1, 2, 3, 4}, TapstateType.BINARY, 4));
        charges.add(new Charge("a nested row", Map.of("ab", 1L), TapstateType.MAP, 10));
        charges.add(new Charge("an array", List.of(1L, 2L), TapstateType.ARRAY, 16));
        charges.add(new Charge("a date", LocalDate.of(2026, 9, 17), TapstateType.DATE, 8));
        charges.add(new Charge("a time", LocalTime.of(1, 2), TapstateType.TIME, 8));
        charges.add(new Charge("a time with an offset",
                OffsetTime.of(LocalTime.of(1, 2), ZoneOffset.UTC), TapstateType.TIME, 8));
        charges.add(new Charge("a year", Year.of(2026), TapstateType.YEAR, 8));
        charges.add(new Charge("a legacy timestamp", new Date(0L), TapstateType.DATETIME, 8));
        charges.add(new Charge("an instant", Instant.EPOCH, TapstateType.DATETIME, 8));
        charges.add(new Charge("a temporal no rule names on its own",
                YearMonth.of(2026, 9), TapstateType.DATETIME, 8));
        charges.add(new Charge("a value no rule names", new Unmodelled("xyz"), TapstateType.UNKNOWN, 3));
        return charges;
    }

    /**
     * The only kind the classifier never answers. A document reaches a row as text or as a nested row
     * and is charged as whichever it arrives as; the namespace names it because a column can be
     * declared that way, and a declared type is not what is classified here.
     */
    private static final TapstateType NOT_A_VALUE_SHAPE = TapstateType.JSON;

    @TestFactory
    Stream<DynamicTest> eachRuleChargesWhatTheDefinitionSays() {
        return theDefinition().stream().map(charge -> DynamicTest.dynamicTest(
                charge.what() + " is " + charge.kind() + " at " + charge.bytes() + " bytes",
                () -> {
                    assertThat(PayloadBytes.kindOf(charge.value())).isEqualTo(charge.kind());
                    assertThat(PayloadBytes.ofValue(charge.value())).isEqualTo(charge.bytes());
                }));
    }

    @Test
    void everyKindInTheNamespaceHasARuleOrIsNamedAsHavingNoValueShape() {
        Set<TapstateType> charged = EnumSet.noneOf(TapstateType.class);
        theDefinition().forEach(charge -> charged.add(charge.kind()));
        Set<TapstateType> expected = EnumSet.allOf(TapstateType.class);
        expected.remove(NOT_A_VALUE_SHAPE);
        assertThat(charged).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void aFieldCostsItsNameAndItsValue() {
        assertThat(PayloadBytes.ofRow(Map.of("ab", "cde"))).isEqualTo(2 + 3);
    }

    @Test
    void aFieldWithNoValueStillCostsItsName() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ab", null);
        assertThat(PayloadBytes.ofRow(row)).isEqualTo(2);
    }

    @Test
    void aRowThatIsAbsentOrEmptyCostsNothing() {
        assertThat(PayloadBytes.ofRow(null)).isZero();
        assertThat(PayloadBytes.ofRow(Map.of())).isZero();
    }

    @Test
    void aContainerCostsItsContentsAndNothingForHoldingThem() {
        assertThat(PayloadBytes.ofValue(List.of())).isZero();
        assertThat(PayloadBytes.ofValue(Map.of())).isZero();
        assertThat(PayloadBytes.ofValue(List.of(Map.of("a", "bc"), Map.of("d", "ef"))))
                .isEqualTo((1 + 2) + (1 + 2));
    }

    @Test
    void anInsertCostsItsAfterImage() {
        assertThat(PayloadBytes.of(Envelope.insert(1L, "s", Map.of("ab", "cde"), null)))
                .isEqualTo(2 + 3);
    }

    @Test
    void aDeleteCostsItsBeforeImage() {
        assertThat(PayloadBytes.of(Envelope.delete(1L, "s", Map.of("ab", "cde"), null)))
                .isEqualTo(2 + 3);
    }

    @Test
    void anUpdateCostsBothHalvesOfTheRow() {
        Envelope update = Envelope.update(1L, "s", Map.of("ab", "old"), Map.of("ab", "newer"), null);
        assertThat(PayloadBytes.of(update)).isEqualTo((2 + 3) + (2 + 5));
    }

    @Test
    void aSchemaChangeCarriesNoRowAndCostsNothing() {
        assertThat(PayloadBytes.of(Envelope.ddl(1L, "s", Map.of("columns", "a long schema document"))))
                .isZero();
    }

    @Test
    void whatTheEnvelopeSaysAboutTheRowIsNotChargedAsTheRow() {
        Envelope bare = Envelope.insert(1L, "s", Map.of("ab", "cde"), null);
        long cost = PayloadBytes.of(bare);
        assertThat(PayloadBytes.of(bare.withSrcPos("a position token of some length"))).isEqualTo(cost);
        assertThat(PayloadBytes.of(bare.withRemoved(Set.of("a-field-that-went-away")))).isEqualTo(cost);
        assertThat(PayloadBytes.of(Envelope.insert(
                1L, "s", Map.of("ab", "cde"), Map.of("ab", "a schema entry")))).isEqualTo(cost);
    }

    @Test
    void anEventThatOnlySaysAFieldIsGoneCostsNothing() {
        Envelope gone = new Envelope(Op.UPDATE, 1L, "s", null, null, null, Map.of(),
                Set.of("a-field-that-went-away"));
        assertThat(PayloadBytes.of(gone)).isZero();
    }

    @Test
    void aCarriedValueCostsWhatTheValueInsideCosts() {
        assertThat(PayloadBytes.ofValue(new ConvertedValue("cde", "varchar(64)")))
                .isEqualTo(PayloadBytes.ofValue("cde"));
        assertThat(PayloadBytes.kindOf(new ConvertedValue("cde", "varchar(64)")))
                .isEqualTo(TapstateType.STRING);
    }

    @Test
    void howLongTheSourceSpellsItsOwnTypeChangesNothing() {
        assertThat(PayloadBytes.ofValue(new ConvertedValue(1L, "t")))
                .isEqualTo(PayloadBytes.ofValue(new ConvertedValue(
                        1L, "a driver type with a very considerably longer declared name")));
    }

    @Test
    void aCarrierInsideACarrierIsSeenThroughAsWell() {
        assertThat(PayloadBytes.ofValue(
                new ConvertedValue(new ConvertedValue("cde", "inner"), "outer")))
                .isEqualTo(PayloadBytes.ofValue("cde"));
    }

    @Test
    void carriersInsideContainersAreSeenThroughToo() {
        assertThat(PayloadBytes.ofValue(Map.of("ab", new ConvertedValue("cde", "varchar(64)"))))
                .isEqualTo(PayloadBytes.ofValue(Map.of("ab", "cde")));
    }

    @Test
    void aValueNoRuleNamesIsChargedButNeverAtNothing() {
        assertThat(PayloadBytes.ofValue(new Unmodelled("xyz"))).isEqualTo(3);
        assertThat(PayloadBytes.ofValue(new Unmodelled(""))).isEqualTo(PayloadBytes.UNWRITABLE_BYTES);
    }

    @Test
    void aValueThatCannotWriteItselfDownDoesNotStopThePipeline() {
        assertThat(PayloadBytes.ofValue(new Unwritable())).isEqualTo(PayloadBytes.UNWRITABLE_BYTES);
        assertThat(PayloadBytes.ofValue(new Nameless())).isEqualTo(PayloadBytes.UNWRITABLE_BYTES);
    }

    @Test
    void theWidthOfAWholeNumberIsDeclaredRatherThanMeasured() {
        assertThat(PayloadBytes.ofValue(1L))
                .isEqualTo(PayloadBytes.ofValue(Long.MIN_VALUE))
                .isEqualTo(PayloadBytes.FIXED_WIDTH_BYTES);
    }

    @Test
    void anExactNumbersDigitsAreTheWidthBecauseTheyAreThePayload() {
        assertThat(PayloadBytes.ofValue(new BigDecimal("1.0000000000000000000000000")))
                .isGreaterThan(PayloadBytes.FIXED_WIDTH_BYTES);
    }

    @Test
    void anAbsentValueHasNoKindToAskFor() {
        assertThatNullPointerException().isThrownBy(() -> PayloadBytes.kindOf(null));
    }

    @Test
    void thereIsNoPayloadWithoutAnEvent() {
        assertThatNullPointerException().isThrownBy(() -> PayloadBytes.of(null));
    }

    /** A value the definition has no rule for, standing in for a driver's own object. */
    private record Unmodelled(String written) {
        @Override
        public String toString() {
            return written.isEmpty() ? null : written;
        }
    }

    /** A value whose written form throws, which a byte count must survive. */
    private static final class Unwritable {
        @Override
        public String toString() {
            throw new IllegalStateException("this value refuses to be written down");
        }
    }

    /** A value whose written form is absent, which is not the same as a value of no size. */
    private static final class Nameless {
        @Override
        public String toString() {
            return null;
        }
    }
}
