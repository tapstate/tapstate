package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The key a row joins by.
 *
 * <p>A join is the one consumer of a row value with no way to report that it went wrong. Two sides
 * whose keys never match produce no error, no warning and no empty result that looks unusual — the
 * job runs, the rows arrive, and the document on the other side simply never fills in. So the one
 * thing worth pinning here is that a key is built from what a value <em>is</em>, whatever box it
 * arrived in.
 */
class NestKeysTest {

    @Test
    void aKeyBuiltFromACarriedValueMatchesOneBuiltFromThePlainValue() {
        Map<String, Object> carried = Map.of("_id", new ConvertedValue("64f0c0de", "OBJECT_ID"));
        Map<String, Object> plain = Map.of("_id", "64f0c0de");

        // The two sides of a join reach this from different places: one side's rows came through a
        // connector that converts its own key type, the other side's did not have to. Comparing the
        // carriers instead would compare identities - false for every row, on every pair.
        assertThat(NestKeys.valuesOf(carried, List.of("_id")))
                .isEqualTo(NestKeys.valuesOf(plain, List.of("_id")));
    }

    @Test
    void twoRowsKeyedOnTheSameBytesProduceTheSameKeyAndTheSameHash() {
        Map<String, Object> left = Map.of("uid", new ConvertedValue(new Bytes((byte) 4, new byte[]{1, 2, 3}), "BINARY"));
        Map<String, Object> right = Map.of("uid", new ConvertedValue(new Bytes((byte) 4, new byte[]{1, 2, 3}), "BINARY"));

        // Two arrays holding the same bytes are two objects. Keyed on what a binary column arrives in
        // unchanged, the two sides of a join compare by identity and never match, and nothing says so.
        assertThat(NestKeys.valuesOf(left, List.of("uid")))
                .isEqualTo(NestKeys.valuesOf(right, List.of("uid")));
        // The hash is the other half and the one a single-member test would miss: it picks the member a
        // key routes to, so an identity hash sends the two sides of one join to two places.
        assertThat(NestKeys.valuesOf(left, List.of("uid")).hashCode())
                .isEqualTo(NestKeys.valuesOf(right, List.of("uid")).hashCode());
    }

    @Test
    void aKeyOnBytesStillTellsDifferentBytesApart() {
        Map<String, Object> row = Map.of("uid", new ConvertedValue(new Bytes((byte) 4, new byte[]{1, 2, 3}), "BINARY"));

        // Which is the half that makes the case above mean something: equal for the same bytes is free
        // if everything is equal to everything.
        assertThat(NestKeys.valuesOf(row, List.of("uid")))
                .isNotEqualTo(NestKeys.valuesOf(
                        Map.of("uid", new ConvertedValue(new Bytes((byte) 4, new byte[]{1, 2, 4}), "BINARY")),
                        List.of("uid")))
                // ...and a tag is part of what a binary column is, not decoration on it.
                .isNotEqualTo(NestKeys.valuesOf(
                        Map.of("uid", new ConvertedValue(new Bytes((byte) 0, new byte[]{1, 2, 3}), "BINARY")),
                        List.of("uid")));
    }

    @Test
    void twoRowsCarryingTheSameValueProduceTheSameKeyEvenFromDifferentColumns() {
        Map<String, Object> left = Map.of("ref", new ConvertedValue("64f0c0de", "OBJECT_ID"));
        Map<String, Object> right = Map.of("ref", new ConvertedValue("64f0c0de", "STRING(24)"));

        // Both sides converted, which is the ordinary case when a document store joins to itself - and
        // the two sides need not agree on what their schemas call the column. The key is the value, so
        // it must not be. The two declared names differ on purpose: with them equal this case would
        // pass on an implementation that never unwrapped, because a carrier compares by its parts.
        assertThat(NestKeys.valuesOf(left, List.of("ref")))
                .isEqualTo(NestKeys.valuesOf(right, List.of("ref")));
    }

    @Test
    void anOrdinaryRowStillKeysOnItsOwnValues() {
        Map<String, Object> row = Map.of("region", "eu", "tier", 2L);

        assertThat(NestKeys.valuesOf(row, List.of("region", "tier")))
                .containsExactly("eu", 2L);
    }

    @Test
    void twoSpellingsOfOneExactDecimalAreOneKey() {
        Map<String, Object> written = Map.of("amount", new BigDecimal("10.50"));
        Map<String, Object> theSameNumber = Map.of("amount", new BigDecimal("10.5"));

        // A document store keeps the scale each value was written with, so one column hands over both
        // of these for one number. Compared with the scale they arrive in they are two keys - one
        // parent's elements split in half, filed under two names in the state layer, and no error
        // anywhere: the job runs, the rows arrive, and the document simply never fills in.
        assertThat(NestKeys.valuesOf(written, List.of("amount")))
                .isEqualTo(NestKeys.valuesOf(theSameNumber, List.of("amount")));
        // The hash decides which member a key routes to, so an unnormalized one sends the two
        // spellings to two places before anything compares them at all.
        assertThat(NestKeys.valuesOf(written, List.of("amount")).hashCode())
                .isEqualTo(NestKeys.valuesOf(theSameNumber, List.of("amount")).hashCode());
    }

    @Test
    void aWholeNumberWrittenAsAnExactDecimalKeepsItsPlainName() {
        Map<String, Object> row = Map.of("amount", new BigDecimal("100.00"));

        // Dropping the trailing zeros alone leaves 1E+2, which is the same number and a name nobody
        // reading the state layer would connect to the column it came from.
        assertThat(NestStateKeys.nameOf(NestKeys.valuesOf(row, List.of("amount"))))
                .isEqualTo("[100]~m");
    }

    @Test
    void aKeyOnAnExactDecimalStillTellsDifferentNumbersApart() {
        Map<String, Object> row = Map.of("amount", new BigDecimal("10.50"));

        // Which is what makes the case above mean anything: equal for one number is free if every
        // number is equal to every other. The second pair is the digit the narrowing this normalization
        // rides on used to lose, so it also pins that the key sees the whole value.
        assertThat(NestKeys.valuesOf(row, List.of("amount")))
                .isNotEqualTo(NestKeys.valuesOf(Map.of("amount", new BigDecimal("10.51")), List.of("amount")));
        assertThat(NestKeys.valuesOf(
                        Map.of("amount", new BigDecimal("1234567890.123456789012345678901234")),
                        List.of("amount")))
                .isNotEqualTo(NestKeys.valuesOf(
                        Map.of("amount", new BigDecimal("1234567890.123456789012345678901235")),
                        List.of("amount")));
    }

    @Test
    void anExactDecimalAndAWholeNumberAreStillTwoKeys() {
        Map<String, Object> decimal = Map.of("amount", new BigDecimal("1.0"));
        Map<String, Object> whole = Map.of("amount", 1L);

        // Normalizing the scale is not coercion between kinds. The state layer names a decimal and a
        // whole number with different letters on purpose, and merging them here would file two keys it
        // tells apart under one name.
        assertThat(NestKeys.valuesOf(decimal, List.of("amount")))
                .isNotEqualTo(NestKeys.valuesOf(whole, List.of("amount")));
    }
}
