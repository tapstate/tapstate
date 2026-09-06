package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ConvertedValue;
import org.junit.jupiter.api.Test;

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
}
