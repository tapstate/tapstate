package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationOwnerRankingTest {
    @Test void publishedRankingVectorIsStableAcrossImplementations() {
        assertThat(ValidationOwnerRanking.rank("cluster", "pipeline",
                List.of("node-a", "node-b", "node-c", "node-d")))
                .containsExactly("node-c", "node-b", "node-a", "node-d");
    }

    @Test void differentDiscoveryOrdersChooseTheSameCandidateOrder() {
        var candidates = List.of("node-a", "node-b", "node-c", "node-d");
        var reversed = new ArrayList<>(candidates);
        Collections.reverse(reversed);
        assertThat(ValidationOwnerRanking.rank("cluster", "pipeline", candidates))
                .isEqualTo(ValidationOwnerRanking.rank("cluster", "pipeline", reversed))
                .containsExactlyInAnyOrderElementsOf(candidates);
    }

    @Test void removingACandidatePreservesTheRelativeOrderOfSurvivors() {
        var ranked = ValidationOwnerRanking.rank("cluster", "pipeline", List.of("a", "b", "c", "d"));
        var survivors = ranked.stream().filter(node -> !node.equals(ranked.get(1))).toList();
        assertThat(ValidationOwnerRanking.rank("cluster", "pipeline", survivors)).isEqualTo(survivors);
    }

    @Test void emptyCandidatesDoNotInventAnOwnerAndDuplicateIdentitiesAreRejected() {
        assertThat(ValidationOwnerRanking.rank("cluster", "pipeline", List.of())).isEmpty();
        assertThatThrownBy(() -> ValidationOwnerRanking.rank("cluster", "pipeline", List.of("a", "a")))
                .isInstanceOf(TapstateException.class);
    }

    @Test void malformedUnicodeCannotCollapseDistinctNodeIdsDuringUtf8Hashing() {
        assertThatThrownBy(() -> ValidationOwnerRanking.rank("cluster", "pipeline", List.of("node-\ud800")))
                .isInstanceOf(TapstateException.class);
    }
}
