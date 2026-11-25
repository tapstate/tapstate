package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertQuotaBucketTest {
    private static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    private static final Instant MINUTE = Instant.parse("2026-10-27T00:00:00Z");

    @Test void oneScopeHeadPreservesTheActorAndGlobalLimitsWithoutDuplicatedTotals() {
        var bucket = AlertTestQuota.Bucket.empty(SCOPE, MINUTE);
        var original = bucket;
        for (int i = 0; i < 3; i++) { bucket = bucket.accepted("actor-a"); }
        assertThat(bucket.window("actor-a").actorAccepted()).isEqualTo(3);
        var fullActor = bucket;
        assertThatThrownBy(() -> fullActor.accepted("actor-a")).isInstanceOf(TapstateException.class);
        for (int i = 0; i < 7; i++) { bucket = bucket.accepted("actor-" + i); }
        assertThat(bucket.scopeAccepted()).isEqualTo(10);
        assertThat(bucket.window("new-actor").scopeAccepted()).isEqualTo(10);
        var fullScope = bucket;
        assertThatThrownBy(() -> fullScope.accepted("new-actor")).isInstanceOf(TapstateException.class);
        assertThat(original.actors()).isEmpty();
        assertThatThrownBy(() -> fullScope.actors().put("changed", 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void aSuccessorConsumesExactlyOneAndCannotRefundOrReplaceAnotherActor() {
        var before = new AlertTestQuota.Bucket(SCOPE, MINUTE, Map.of("a", 2, "b", 1));
        assertThat(before.accepted("a").succeeds(before)).isTrue();
        assertThat(before.accepted("c").succeeds(before)).isTrue();
        assertThat(before.succeeds(before)).isFalse();
        assertThat(new AlertTestQuota.Bucket(SCOPE, MINUTE, Map.of("a", 1, "b", 3)).succeeds(before)).isFalse();
        assertThat(new AlertTestQuota.Bucket(SCOPE, MINUTE, Map.of("a", 2, "c", 2)).succeeds(before)).isFalse();
        assertThat(new AlertTestQuota.Bucket(SCOPE, MINUTE.plusSeconds(60), Map.of("a", 3, "b", 1)).succeeds(before)).isFalse();
    }

    @Test void minuteAndCleanupAreFixedAcrossRestartsAndNanosecondBoundaryInputs() {
        var bucket = AlertTestQuota.Bucket.empty(SCOPE, MINUTE.plusSeconds(59).plusNanos(999999999));
        assertThat(bucket.minute()).isEqualTo(MINUTE);
        assertThat(bucket.cleanupAfter()).isEqualTo(MINUTE.plusSeconds(3660));
        assertThat(AlertTestQuota.Bucket.empty(SCOPE, MINUTE.minusNanos(1)).minute()).isEqualTo(MINUTE.minusSeconds(60));
        assertThatThrownBy(() -> new AlertTestQuota.Bucket(SCOPE, MINUTE.plusNanos(1), Map.of())).isInstanceOf(TapstateException.class);
    }

    @Test void malformedOrOverBudgetCountsNeverBecomeAnEmptyWindow() {
        for (var counts : java.util.List.of(Map.of("a", 0), Map.of("a", -1), Map.of("a", 4),
                Map.of("a", 3, "b", 3, "c", 3, "d", 2), Map.of("bad\nactor", 1))) {
            assertThatThrownBy(() -> new AlertTestQuota.Bucket(SCOPE, MINUTE, counts)).isInstanceOf(TapstateException.class);
        }
    }
}
