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
     * The name a bucket of the identities pointing at a row is filed under, written out rather than
     * derived.
     *
     * <p>That key says which row places it, so that every bucket of one row is held where that row is.
     * What it is <i>named</i> by is a separate question and did not move with it: the values it carries,
     * flat, exactly as when the key was a plain list. Written here as the literal string because what this
     * protects is entries that already exist - anything that changes the name leaves every bucket ever
     * written unreachable, and nothing reports it. The rows themselves still render and every document
     * still looks right; what is lost is the record of who points at them, which surfaces much later as an
     * edit to a row that reaches no document at all.
     *
     * <p>Derived instead of written out, this would agree with whatever the naming happens to do, which is
     * the one thing it must not do.
     */
    @Test
    void aBucketOfReferrersKeepsTheNameItsEntriesWereWrittenUnder() {
        assertThat(NestStateKeys.nameOf(NestLookup.bucketKey(List.of("C7"), 0)))
                .isEqualTo("[\"C7\",0]~si");
        assertThat(NestStateKeys.nameOf(NestLookup.bucketKey(List.of("C7", 42L), 7)))
                .describedAs("a composite identity keeps its values in the order it carries them, with "
                        + "the bucket last - which is what makes the name injective")
                .isEqualTo("[\"C7\",42,7]~sli");
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
