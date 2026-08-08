package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Two keys that are not equal are never filed under the same name.
 *
 * <p>This is the one property the naming cannot be wrong about. A collision is not a failure that shows
 * up as a failure: the second key finds the first one's state, reads it as its own, and writes back over
 * it. Both documents then look built, and both are built out of the wrong rows.
 *
 * <p>The pairs below are the ones a rendering of values alone gets wrong. A whole number and a decimal
 * of the same value render alike; so do a decimal and a big decimal; and a string of digits is only kept
 * apart from the number by its quotes. Each pair here is two keys the engine can genuinely be handed for
 * the same field - a column read as a whole number in one connector and a decimal in another - so none
 * of them is hypothetical.
 */
class NestStateKeyNamesTellApartWhatIsNotEqualTest {

    @Test
    void valuesThatRenderAlikeAreStillNamedApart() {
        List<Object> keys = List.of(
                List.of("1"),
                List.of(1L),
                List.of(1),
                List.of(1.0d),
                List.of(1.0f),
                List.of(new BigDecimal("1.0")),
                List.of(java.math.BigInteger.ONE),
                List.of(true));

        List<String> names = new ArrayList<>();
        for (Object key : keys) {
            names.add(NestStateKeys.nameOf(key));
        }

        assertThat(names)
                .describedAs("each of these is a different key, so each must be a different name")
                .doesNotHaveDuplicates()
                .hasSameSizeAs(keys);
    }

    @Test
    void anAbsentValueIsNotTheWordForIt() {
        assertThat(NestStateKeys.nameOf(Arrays.asList((Object) null)))
                .isNotEqualTo(NestStateKeys.nameOf(List.of("null")));
    }

    @Test
    void aKeyOfSeveralValuesIsNotConfusedWithADifferentGroupingOfThem() {
        assertThat(NestStateKeys.nameOf(List.of("a", "bc")))
                .describedAs("the boundary between two values is part of the name, not something a "
                        + "reader has to guess back")
                .isNotEqualTo(NestStateKeys.nameOf(List.of("ab", "c")));
    }

    @Test
    void thesameKeyIsAlwaysTheSameName() {
        assertThat(NestStateKeys.nameOf(List.of("C1", 7L)))
                .isEqualTo(NestStateKeys.nameOf(List.of("C1", 7L)));
    }

    /**
     * A value the naming has no letter for is the engine's own defect - it chose the fields the vertex is
     * partitioned by - so it crashes rather than being given a name that another kind might also take.
     */
    @Test
    void aValueWithNoNameCrashesRatherThanBorrowingOne() {
        assertThatThrownBy(() -> NestStateKeys.nameOf(List.of(new java.util.Date())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no name in the state layer");
    }
}
