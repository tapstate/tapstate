package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class AStageReadingNamesOnlyStagesOfTheGraphTest {

    private static HistogramValue oneUnit() {
        List<Long> buckets = new ArrayList<>();
        for (int index = 0; index < HistogramBounds.PROCESS_DURATION.buckets(); index++) {
            buckets.add(index == 2 ? 1L : 0L);
        }
        return HistogramBounds.PROCESS_DURATION.value(1L, 0.0003, buckets);
    }

    @Test
    @DisplayName("a reading keyed by a word that is not a stage is refused")
    void aWordThatIsNotAStageIsRefused() {
        // The stage set is closed: a reading keyed by a word outside it would be a series the data added,
        // under a name nothing else in the product knows.
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new StageReading(Map.of("write", oneUnit()), Instant.EPOCH))
                .withMessageContaining("not a stage");
    }

    @Test
    @DisplayName("a reading with distributions in it says what it accumulated them from")
    void distributionsWithoutAStartAreRefused() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new StageReading(Map.of("sink", oneUnit()), null))
                .withMessageContaining("accumulated them from");
    }

    @Test
    @DisplayName("an empty reading needs no start and reports nothing")
    void anEmptyReadingIsAllowed() {
        assertThat(StageReading.NONE.isEmpty()).isTrue();
        assertThat(StageReading.NONE.start()).isEmpty();
        assertThat(new StageReading(Map.of("transform", oneUnit()), Instant.EPOCH).isEmpty()).isFalse();
    }
}
