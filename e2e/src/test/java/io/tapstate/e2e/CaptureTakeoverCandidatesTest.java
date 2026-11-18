package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CaptureTakeoverCandidatesTest {
    @ParameterizedTest
    @ValueSource(strings = {"node-a", "node-b"})
    void retainsEveryHolderCandidateUntilTwoRemoteConsumersAreFound(String holder) {
        String remote = holder.equals("node-a") ? "node-b" : "node-a";
        var candidates = new CaptureTakeoverCandidates(holder, remote, 12);
        assertThat(candidates.add("first", holder, "first claim")).isFalse();
        assertThat(candidates.add("local", holder, "local initial claim")).isFalse();
        assertThat(candidates.ready()).isFalse();
        assertThat(candidates.add("joiner", remote, "joiner claim")).isTrue();
        assertThat(candidates.add("another-local", holder, "another local claim")).isFalse();
        assertThat(candidates.ready()).isFalse();
        assertThat(candidates.add("remaining", remote, "remaining claim")).isTrue();
        assertThat(candidates.ready()).isTrue();
        assertThat(candidates.onTheHolder()).containsExactly("first", "local", "another-local");
        assertThat(candidates.firstJoiner()).isEqualTo("joiner");
        assertThat(candidates.remainingPipeline()).isEqualTo("remaining");
    }

    @Test
    void exhaustionFailsWithEveryCandidateClaimInsteadOfSkippingTheWitness() {
        var candidates = new CaptureTakeoverCandidates("node-a", "node-b", 12);
        for (int n = 1; n <= 12; n++) {
            candidates.add("pipe-" + n, n == 2 ? "node-b" : "node-a", "issuer/claim/controller-" + n);
        }
        assertThatThrownBy(candidates::requireReady).isInstanceOf(AssertionError.class)
                .hasMessageContaining("fewer than two pipelines of 12")
                .hasMessageContaining("issuer/claim/controller-1")
                .hasMessageContaining("issuer/claim/controller-12");
        assertThat(candidates.onTheHolder()).hasSize(11).doesNotContain("pipe-2");
    }

    @Test
    void missingOrForeignControllersAreFailuresRatherThanLocalCandidates() {
        var candidates = new CaptureTakeoverCandidates("node-a", "node-b", 12);
        assertThatThrownBy(() -> candidates.add("missing", null, "no claim"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unexpected controller");
        assertThatThrownBy(() -> candidates.add("foreign", "node-c", "foreign claim"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unexpected controller");
        assertThat(candidates.onTheHolder()).isEmpty();
    }
}
