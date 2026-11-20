package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Two values of one temporal column are ordered by the order those values carry, so a filter can keep
 * rows by the order of their own dates and times. Ordering them needs no zone, unlike
 * converting either side to an instant; a pair with no order in common still fails the evaluation
 * rather than being guessed at, and the orderings CEL already had keep working beside them.
 */
class SameTypeTemporalOrderingTest {

    private static final LocalDateTime OLDER = LocalDateTime.of(2020, 1, 1, 0, 0);
    private static final LocalDateTime NEWER = LocalDateTime.of(2026, 11, 16, 9, 0);

    @Test
    @DisplayName("a row whose two timestamps are in order passes, and one whose are not is dropped")
    void aRowIsKeptByComparingTwoOfItsTimestamps() {
        TransformPort filter = StatelessTransforms.filter("after.shipped >= after.ordered");
        Envelope inOrder = Envelope.insert(1L, "orders", Map.of("id", 1, "ordered", OLDER, "shipped", NEWER), null);
        Envelope reversed = Envelope.insert(2L, "orders", Map.of("id", 2, "ordered", NEWER, "shipped", OLDER), null);

        assertThat(filter.transform(inOrder)).containsExactly(inOrder);
        assertThat(filter.transform(reversed)).isEmpty();
    }

    @Test
    @DisplayName("an update's new timestamp can be compared with its old one")
    void anUpdatesTimestampsCompareAcrossItsTwoImages() {
        LinkedHashMap<String, FieldRule> fields = new LinkedHashMap<>();
        fields.put("id", FieldRule.rename("id"));
        fields.put("moved_back", FieldRule.computed("after.updated < before.updated"));
        TransformPort map = StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(fields)));
        Envelope stale = Envelope.update(1L, "players", Map.of("id", 7, "updated", NEWER),
                Map.of("id", 7, "updated", OLDER), null);

        assertThat(map.transform(stale)).singleElement()
                .satisfies(out -> assertThat(out.after()).containsEntry("moved_back", true));
    }

    @ParameterizedTest(name = "{0} {1} {2} is {3}")
    @CsvSource({
            "2020-01-01, <, 2026-11-16, true", "2026-11-16, <, 2020-01-01, false", "2026-11-16, <, 2026-11-16, false",
            "2020-01-01, <=, 2026-11-16, true", "2026-11-16, <=, 2026-11-16, true", "2026-11-16, <=, 2020-01-01, false",
            "2026-11-16, >, 2020-01-01, true", "2020-01-01, >, 2026-11-16, false", "2026-11-16, >, 2026-11-16, false",
            "2026-11-16, >=, 2020-01-01, true", "2026-11-16, >=, 2026-11-16, true", "2020-01-01, >=, 2026-11-16, false"})
    void everyOrderingOperatorReadsBothOperandsInTheirOwnOrder(String left, String operator, String right,
            boolean expected) {
        TransformPort filter = StatelessTransforms.filter("after.a " + operator + " after.b");
        Envelope row = Envelope.insert(1L, "t",
                Map.of("a", LocalDate.parse(left), "b", LocalDate.parse(right)), null);

        assertThat(filter.transform(row)).hasSize(expected ? 1 : 0);
    }

    @Test
    @DisplayName("two columns of a built-in kind compare with each other, not only with a literal")
    void twoColumnsOfABuiltInKindCompare() {
        TransformPort filter = StatelessTransforms.filter("after.a < after.b && after.s < after.t && after.d < after.e");
        Envelope row = Envelope.insert(1L, "t",
                Map.of("a", 3L, "b", 4L, "s", "a", "t", "b", "d", 1.5d, "e", 2.5d), null);
        Envelope reversed = Envelope.insert(2L, "t",
                Map.of("a", 4L, "b", 3L, "s", "a", "t", "b", "d", 1.5d, "e", 2.5d), null);

        assertThat(filter.transform(row)).containsExactly(row);
        assertThat(filter.transform(reversed)).isEmpty();
    }

    @Test
    @DisplayName("the orderings CEL already had still answer as before")
    void builtInOrderingsAreUnchanged() {
        TransformPort filter = StatelessTransforms.filter("after.n < 10 && after.s > 'a' && after.d <= 2.5");
        Envelope row = Envelope.insert(1L, "t", Map.of("n", 3L, "s", "b", "d", 2.5d), null);

        assertThat(filter.transform(row)).containsExactly(row);
    }

    @Test
    @DisplayName("two values with no order in common fail the evaluation instead of being guessed at")
    void valuesWithNoCommonOrderFail() {
        TransformPort filter = StatelessTransforms.filter("after.a < after.b");
        Envelope row = Envelope.insert(1L, "t", Map.of("a", LocalDate.of(2020, 1, 1), "b", OLDER), null);

        assertThatThrownBy(() -> filter.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> assertThat(((TapstateException) failure).code().code())
                        .isEqualTo("transform.expression-failed"));
    }
}
