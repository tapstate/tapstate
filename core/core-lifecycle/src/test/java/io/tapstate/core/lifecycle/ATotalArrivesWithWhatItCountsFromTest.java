package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Every reading a run hands out that carries a running total also carries the moment it began counting,
 * and each of them refuses to be built without one.
 *
 * <p><strong>Why this is one case over all of them rather than one case each.</strong> The rule is written
 * out three times, once per reading, and three copies of a rule are three chances for the next one to be
 * written without it — silently, because a total with no start is a perfectly readable object that only
 * misleads later. Listing the readings here means a fourth is added either to this list or against a case
 * that names the gap.
 *
 * <p>What the start buys is the difference between two observations that otherwise look identical: a run
 * restarted onto a fresh account, whose totals begin again from nothing, and a live account whose total
 * went backwards. The first is ordinary and the second is a fault, and a consumer handed only the numbers
 * cannot tell them apart.
 */
class ATotalArrivesWithWhatItCountsFromTest {

    private static final Instant SINCE = Instant.parse("2026-09-17T11:00:00Z");

    /** One entry per reading that carries a total: what it looks like with rows, and with none. */
    private record Reading(String name, Supplier<Object> rowsWithNoStart, Supplier<Object> empty) {
    }

    private static List<Reading> readings() {
        return List.of(
                new Reading("CaptureReading",
                        () -> new CaptureReading(Map.of("orders", Map.of("i", 3L)), null),
                        () -> new CaptureReading(Map.of(), null)),
                new Reading("DeliveryReading",
                        () -> new DeliveryReading(Map.of("orders", Map.of("i", 3L)), Map.of(), null),
                        () -> new DeliveryReading(Map.of(), Map.of(), null)),
                new Reading("SnapshotReading",
                        () -> new SnapshotReading(Map.of("orders", new TableSnapshot(3L, null, null)), null),
                        () -> new SnapshotReading(Map.of(), null)));
    }

    @Test
    @DisplayName("a reading that counted rows cannot be built without what it counted them from")
    void rowsWithoutAStartAreRefusedByEveryReadingThatCarriesATotal() {
        List<Reading> all = readings();
        // Asserted before the loop: a list that had quietly emptied would pass every check below.
        assertThat(all).hasSize(3);
        for (Reading reading : all) {
            assertThatIllegalArgumentException()
                    .as("%s must refuse rows with no start", reading.name())
                    .isThrownBy(() -> reading.rowsWithNoStart().get())
                    .withMessageContaining("says what it counted them from");
        }
    }

    @Test
    @DisplayName("a reading that counted nothing needs no start, because nothing accumulated")
    void anEmptyReadingIsAllowedToHaveNoStart() {
        for (Reading reading : readings()) {
            assertThatCode(() -> reading.empty().get())
                    .as("%s with nothing counted is a legitimate reading", reading.name())
                    .doesNotThrowAnyException();
        }
    }
}
