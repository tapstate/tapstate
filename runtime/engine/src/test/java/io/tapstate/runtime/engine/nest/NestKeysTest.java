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

    /**
     * Stands in for what a connector's conversion actually produces: an object with no equality of its
     * own, so two of them holding the same value are two different things. A stand-in that <em>had</em>
     * equality would give the carrier around it equality too, and every case below would pass whether
     * or not anything unwrapped.
     */
    private static final class DriverKey {
        private final String hex;

        private DriverKey(String hex) {
            this.hex = hex;
        }

        @Override
        public String toString() {
            return hex;
        }
    }

    @Test
    void aKeyBuiltFromACarriedValueMatchesOneBuiltFromThePlainValue() {
        Map<String, Object> carried = Map.of("_id", new ConvertedValue("64f0c0de", new DriverKey("64f0c0de")));
        Map<String, Object> plain = Map.of("_id", "64f0c0de");

        // The two sides of a join reach this from different places: one side's rows came through a
        // connector that converts its own key type, the other side's did not have to. Comparing the
        // carriers instead would compare identities - false for every row, on every pair.
        assertThat(NestKeys.valuesOf(carried, List.of("_id")))
                .isEqualTo(NestKeys.valuesOf(plain, List.of("_id")));
    }

    @Test
    void twoRowsCarryingTheSameValueProduceTheSameKey() {
        Map<String, Object> left = Map.of("_id", new ConvertedValue("64f0c0de", new DriverKey("64f0c0de")));
        Map<String, Object> right = Map.of("_id", new ConvertedValue("64f0c0de", new DriverKey("64f0c0de")));

        // Both sides converted, which is the ordinary case when a document store joins to itself. Two
        // rows read at different moments hold two conversions of the one key, and what a conversion
        // produces does not compare by its contents - so the carriers around them do not either.
        assertThat(NestKeys.valuesOf(left, List.of("_id")))
                .isEqualTo(NestKeys.valuesOf(right, List.of("_id")));
    }

    @Test
    void anOrdinaryRowStillKeysOnItsOwnValues() {
        Map<String, Object> row = Map.of("region", "eu", "tier", 2L);

        assertThat(NestKeys.valuesOf(row, List.of("region", "tier")))
                .containsExactly("eu", 2L);
    }
}
