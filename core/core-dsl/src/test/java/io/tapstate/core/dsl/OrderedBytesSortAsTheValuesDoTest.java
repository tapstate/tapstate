package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.dsl.aggregate.OrderedBytes;
import io.tapstate.core.dsl.aggregate.Values;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** The bytes a value is stored under sort, unsigned and left to right, exactly as the library orders the value. */
class OrderedBytesSortAsTheValuesDoTest {

    private static final List<Object> NUMBERS = List.of(
            Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, new BigDecimal("-1e40"), Long.MIN_VALUE, -1000L,
            new BigDecimal("-999.99"), -10L, -9.5, new BigDecimal("-9.49"), -1L, -0.5, new BigDecimal("-0.05"),
            -Double.MIN_VALUE, 0L, Double.MIN_VALUE, new BigDecimal("0.0049"), 0.005, new BigDecimal("0.05"),
            0.5f, 1L, new BigDecimal("1.0000000000000000000000000000000000001"), 2, new BigDecimal("2.5"),
            9L, 10L, new BigDecimal("10.01"), 99L, 100L, new BigInteger("9223372036854775808"),
            new BigDecimal("1e40"), Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN);

    @Test
    void numbersSortByValueWhateverTheyAreHeldAs() {
        inOrder(NUMBERS);
        Random random = new Random(11);
        agrees(random, 4000, r -> switch (r.nextInt(4)) {
            case 0 -> (Object) r.nextLong();
            case 1 -> (Object) (long) r.nextInt(-50, 50);
            case 2 -> (Object) BigDecimal.valueOf(r.nextLong(-100_000, 100_000), r.nextInt(-3, 8));
            default -> (Object) ((r.nextDouble() - 0.5) * Math.pow(10, r.nextInt(-30, 30)));
        });
    }

    @Test
    void numbersEqualInValueAreTheSameBytes() {
        assertThat(OrderedBytes.of(new BigDecimal("2.50"))).isEqualTo(OrderedBytes.of(new BigDecimal("2.5")));
        assertThat(OrderedBytes.of(2L)).isEqualTo(OrderedBytes.of(new BigDecimal("2.000")));
        assertThat(OrderedBytes.of(0.5)).isEqualTo(OrderedBytes.of(new BigDecimal("0.5")));
        assertThat(OrderedBytes.of(-0.0)).isEqualTo(OrderedBytes.of(0L));
        assertThat(OrderedBytes.of(300L)).isEqualTo(OrderedBytes.of(new BigDecimal("3E+2")));
    }

    @Test
    void textSortsByCodePoint() {
        String astral = new String(Character.toChars(0x1F600));
        inOrder(List.of("", "\u0000", "\u0000a", "\u0001", "A", "Z", "a", "a\u0000", "a\u0000b", "a\u0001", "aa",
                "b", "é", "߿", "ࠀ", "\ud800", "￿", astral, astral + "a"));
        Random random = new Random(12);
        int[] points = {0, 1, 'a', 'b', 0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xd800, 0xdfff, 0xe000, 0xffff, 0x10000, 0x10ffff};
        agrees(random, 4000, r -> {
            StringBuilder text = new StringBuilder();
            for (int i = r.nextInt(4); i > 0; i--) {
                text.appendCodePoint(points[r.nextInt(points.length)]);
            }
            return text.toString();
        });
    }

    @Test
    void temporalsSortByInstantOrByTheirOwnOrder() {
        Random random = new Random(13);
        agrees(random, 2000, r -> r.nextBoolean()
                ? (Object) Instant.ofEpochSecond(r.nextLong(-5_000_000_000L, 5_000_000_000L), r.nextInt(1_000_000_000))
                : (Object) OffsetDateTime.ofInstant(Instant.ofEpochSecond(r.nextLong(-5_000_000_000L, 5_000_000_000L)),
                        ZoneOffset.ofHours(r.nextInt(-12, 13))));
        agrees(random, 1000, r -> LocalDate.ofEpochDay(r.nextLong(-800_000, 800_000)));
        agrees(random, 1000, r -> LocalDateTime.ofEpochSecond(r.nextLong(-5_000_000_000L, 5_000_000_000L),
                r.nextInt(1_000_000_000), ZoneOffset.UTC));
        agrees(random, 1000, r -> LocalTime.ofNanoOfDay(r.nextLong(86_400_000_000_000L)));
        agrees(random, 500, r -> Year.of(r.nextInt(-5000, 5000)));
        inOrder(List.of(false, true));
    }

    @Test
    void aListSortsLeftToRightAndNoValueBeginsAnother() {
        Random random = new Random(14);
        List<Function<Random, Object>> columns = List.of(
                r -> List.of("", "a", "a\u0000", "ab", "b").get(r.nextInt(5)),
                r -> (Object) BigDecimal.valueOf(r.nextLong(-200, 200), r.nextInt(0, 3)),
                r -> Instant.ofEpochSecond(r.nextInt(5)));
        for (int i = 0; i < 4000; i++) {
            List<Object> a = new ArrayList<>();
            List<Object> b = new ArrayList<>();
            for (Function<Random, Object> column : columns) {
                a.add(column.apply(random));
                b.add(column.apply(random));
            }
            assertThat(Integer.signum(Arrays.compareUnsigned(OrderedBytes.ofAll(a), OrderedBytes.ofAll(b))))
                    .as("%s against %s", a, b).isEqualTo(Integer.signum(Values.compareAll(a, b)));
        }
    }

    @Test
    void aKeySortsAsTheLibraryBreaksTiesWithANullFirst() {
        Random random = new Random(15);
        for (int i = 0; i < 4000; i++) {
            List<Object> a = new ArrayList<>();
            List<Object> b = new ArrayList<>();
            for (List<Object> key : List.of(a, b)) {
                key.add(random.nextInt(5) == 0 ? null : (Object) (long) random.nextInt(4));
                key.add(random.nextInt(5) == 0 ? null : List.of("", "x", "xy").get(random.nextInt(3)));
            }
            assertThat(Integer.signum(Arrays.compareUnsigned(OrderedBytes.ofKey(a), OrderedBytes.ofKey(b))))
                    .as("%s against %s", a, b).isEqualTo(Integer.signum(Values.compareKeys(a, b)));
        }
    }

    @Test
    void aNullIsRefusedOutsideAKey() {
        assertThatThrownBy(() -> OrderedBytes.of(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OrderedBytes.ofAll(Arrays.asList(1L, null))).isInstanceOf(IllegalArgumentException.class);
    }

    /** Every pair of a list written in ascending order sorts that way, both by the library and as bytes. */
    private static void inOrder(List<?> ascending) {
        for (int i = 0; i < ascending.size(); i++) {
            for (int j = 0; j < ascending.size(); j++) {
                Object a = ascending.get(i);
                Object b = ascending.get(j);
                assertThat(Integer.signum(Values.compare(a, b))).as("library: %s against %s", a, b)
                        .isEqualTo(Integer.signum(Integer.compare(i, j)));
                assertThat(Integer.signum(Arrays.compareUnsigned(OrderedBytes.of(a), OrderedBytes.of(b))))
                        .as("bytes: %s against %s", a, b).isEqualTo(Integer.signum(Integer.compare(i, j)));
            }
        }
    }

    private static void agrees(Random random, int pairs, Function<Random, Object> next) {
        for (int i = 0; i < pairs; i++) {
            Object a = next.apply(random);
            Object b = random.nextInt(8) == 0 ? a : next.apply(random);
            assertThat(Integer.signum(Arrays.compareUnsigned(OrderedBytes.of(a), OrderedBytes.of(b))))
                    .as("%s against %s", a, b).isEqualTo(Integer.signum(Values.compare(a, b)));
        }
    }
}
