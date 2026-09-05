package io.tapstate.core.sql;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Both encoding rules here fail silently when they are wrong: what comes out is rows that look
 * ordinary. Every case therefore carries a control -- the same shape without the feature under
 * test -- because an encoder that simply never repeats itself satisfies every inequality below.
 */
class JoinKeyTest {

    @Test
    @DisplayName("the boundary between two columns cannot be moved")
    void columnBoundaryCannotBeMoved() {
        assertThat(JoinKey.of(List.of("a", "b")))
                .as("control: equal column values are one key, or the inequalities below prove nothing")
                .isEqualTo(JoinKey.of(List.of("a", "b")));

        assertThat(JoinKey.of(List.of("ab", "c")))
                .as("plain concatenation puts these two on one key")
                .isNotEqualTo(JoinKey.of(List.of("a", "bc")));
        assertThat(JoinKey.of(List.of("a|b", "c")))
                .as("concatenation with a separator puts these two on one key")
                .isNotEqualTo(JoinKey.of(List.of("a", "b|c")));
    }

    @Test
    @DisplayName("a null in any column poisons the whole key, which then equals nothing but itself")
    void nullPoisonsTheWholeKey() {
        JoinKey poisoned = JoinKey.of(Arrays.asList("a", null));
        JoinKey anotherRowsNull = JoinKey.of(Arrays.asList("a", null));

        assertThat(poisoned.matchable()).isFalse();
        assertThat(poisoned)
                .as("still equal to itself, so a carrier may hold it in a map like any other key")
                .isEqualTo(poisoned);
        assertThat(poisoned)
                .as("the other side's null must not match this one -- SQL never matches null to null")
                .isNotEqualTo(anotherRowsNull);
        assertThat(poisoned)
                .as("nor may a null collapse onto the empty value")
                .isNotEqualTo(JoinKey.of(List.of("a", "")));
        assertThat(JoinKey.of(Arrays.asList((Object) null)))
                .as("a single column that is null is the same rule, not a special case")
                .isNotEqualTo(JoinKey.of(Arrays.asList((Object) null)));

        assertThat(JoinKey.of(List.of("a", "b")))
                .as("control: the same two columns without the null do match")
                .isEqualTo(JoinKey.of(List.of("a", "b")));
    }

    @Test
    @DisplayName("a hash table built from one row's null is not answered by another row's null")
    void poisonedKeysDoNotMeetInAHashTable() {
        Map<JoinKey, String> build = new HashMap<>();
        build.put(JoinKey.of(Arrays.asList("x", null)), "dimension row with a null key");
        build.put(JoinKey.of(List.of("x", "y")), "dimension row with a real key");

        assertThat(build.get(JoinKey.of(Arrays.asList("x", null))))
                .as("this is the naive implementation's extra row, and it looks like a real match")
                .isNull();
        assertThat(build.get(JoinKey.of(List.of("x", "y"))))
                .as("control: the same table does answer a key with no null in it")
                .isEqualTo("dimension row with a real key");
    }

    @Test
    @DisplayName("an exact number's trailing zeros do not split one value across two keys")
    void exactNumbersAreCanonical() {
        assertThat(JoinKey.of(List.of(new BigDecimal("1.0"))))
                .as("a database compares these equal, so a join on them must match")
                .isEqualTo(JoinKey.of(List.of(new BigDecimal("1.00"))));
        assertThat(JoinKey.of(List.of(new BigDecimal("1.0"))))
                .as("control: it is still the value that decides, not the type")
                .isNotEqualTo(JoinKey.of(List.of(new BigDecimal("1.1"))));
    }

    @Test
    @DisplayName("two byte strings holding the same bytes are one key")
    void byteStringsCompareByContent() {
        assertThat(JoinKey.of(List.of(new byte[]{1, 2})))
                .as("comparing byte strings by identity loses every match, silently")
                .isEqualTo(JoinKey.of(List.of(new byte[]{1, 2})));
        assertThat(JoinKey.of(List.of(new byte[]{1, 2})))
                .as("control: different bytes are still different keys")
                .isNotEqualTo(JoinKey.of(List.of(new byte[]{1, 3})));
    }
}
