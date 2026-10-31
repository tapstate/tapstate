package io.tapstate.core.alerting;

import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.DeliveryEvidence.Outcome.*;
import static io.tapstate.core.alerting.FrozenGroupPolicy.Action.*;
import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FrozenGroupPolicyTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org", "cluster");
    static FrozenGroupPolicy.Member member(String id) {
        return new FrozenGroupPolicy.Member(new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", id, "resource-1"), "pipeline.failed", "rule"), 1);
    }
    static FrozenGroupPolicy.Batch batch() {
        var round = DeliveryKey.NotificationRound.forGroup(SCOPE, "group", "group-1", 1, "route", "route-1", DeliveryKey.Kind.FAULT, 1);
        return new FrozenGroupPolicy.Batch(new DeliveryKey(round, "webhook", "receiver-1", "target-1"), 7, List.of(member("a"), member("b")));
    }
    @Test void partialInvalidationStopsTheOldBatchAndPreservesAnImmutableSinglePendingFact() {
        var batch = batch(); var decision = FrozenGroupPolicy.reconcile(batch, Set.of(member("b")), List.of(), T);
        assertThat(decision.action()).isEqualTo(STOP_PENDING); assertThat(decision.stopOriginalPermits()).isTrue();
        assertThat(decision.pendingMembers()).containsExactly(member("b"));
        assertThat(decision.previousOutcome()).isEqualTo(NOT_ATTEMPTED);
        assertThat(batch.members()).containsExactly(member("a"), member("b"));
        assertThat(FrozenGroupPolicy.reconcile(batch, Set.of(member("b")), List.of(), T.plusSeconds(10)).key()).isEqualTo(decision.key());
        assertThatThrownBy(() -> decision.pendingMembers().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void oldInflightAttemptsWaitOnlyToTheirOwnCutoffsAndKeepUnknownContextForSurvivors() {
        var attempts = List.of(new DeliveryEvidence.Previous(NOT_ACCEPTED, false, T.plusSeconds(10)),
                new DeliveryEvidence.Previous(NOT_ATTEMPTED, false, T.plusSeconds(30)));
        var first = FrozenGroupPolicy.reconcile(batch(), Set.of(member("b")), attempts, T);
        assertThat(first.action()).isEqualTo(STOP_WAIT); assertThat(first.recheckAt()).isEqualTo(T.plusSeconds(10));
        var second = FrozenGroupPolicy.reconcile(batch(), Set.of(member("b")), attempts, T.plusSeconds(20));
        assertThat(second.action()).isEqualTo(STOP_WAIT); assertThat(second.recheckAt()).isEqualTo(T.plusSeconds(30));
        var expired = FrozenGroupPolicy.reconcile(batch(), Set.of(member("b")), attempts, T.plusSeconds(100));
        assertThat(expired.action()).isEqualTo(STOP_PENDING); assertThat(expired.previousOutcome()).isEqualTo(UNKNOWN);
        assertThat(expired.pendingMembers()).containsExactly(member("b")); assertThat(expired.recheckAt()).isNull();
    }
    @Test void acceptedOldContentCoversSurvivorsAndAllInvalidMembersNeverCreateAnEmptySuccessor() {
        var accepted = List.of(new DeliveryEvidence.Previous(ACCEPTED, false, null));
        var covered = FrozenGroupPolicy.reconcile(batch(), Set.of(member("b")), accepted, T);
        assertThat(covered.action()).isEqualTo(STOP_COVERED); assertThat(covered.pendingMembers()).isEmpty();
        var none = FrozenGroupPolicy.reconcile(batch(), Set.of(), List.of(new DeliveryEvidence.Previous(UNKNOWN, false, T.plusSeconds(30))), T);
        assertThat(none.action()).isEqualTo(STOP_NO_SUCCESSOR); assertThat(none.pendingMembers()).isEmpty();
        assertThat(none.recheckAt()).isEqualTo(T.plusSeconds(30));
        assertThat(FrozenGroupPolicy.reconcile(batch(), Set.of(member("a"), member("b")), accepted, T).action()).isEqualTo(KEEP_ORIGINAL);
    }
    @Test void acceptedEvidenceStillWaitsForEveryOriginalInflightCutoffBeforeSuppressingTheSuccessor() {
        var attempts = List.of(new DeliveryEvidence.Previous(ACCEPTED, false, null),
                new DeliveryEvidence.Previous(NOT_ATTEMPTED, false, T.plusSeconds(30)));
        var waiting = FrozenGroupPolicy.reconcile(batch(), Set.of(member("b")), attempts, T.plusSeconds(29));
        assertThat(waiting.action()).isEqualTo(STOP_WAIT);
        assertThat(waiting.recheckAt()).isEqualTo(T.plusSeconds(30));
        assertThat(waiting.pendingMembers()).containsExactly(member("b"));
        var settled = FrozenGroupPolicy.reconcile(batch(), Set.of(member("b")), attempts, T.plusSeconds(30));
        assertThat(settled.action()).isEqualTo(STOP_COVERED);
        assertThat(settled.previousOutcome()).isEqualTo(ACCEPTED);
        assertThat(settled.pendingMembers()).isEmpty();
        assertThat(settled.key()).isEqualTo(waiting.key());
    }
    @Test void keysRemainTargetSpecificAndInvalidMembershipCannotMasqueradeAsTheFrozenBatch() {
        var original = batch(); var decision = FrozenGroupPolicy.reconcile(original, Set.of(member("b")), List.of(), T);
        var otherTarget = new FrozenGroupPolicy.Batch(new DeliveryKey(original.deliveryKey().notification(), "webhook", "receiver-1", "target-2"), 7, original.members());
        assertThat(FrozenGroupPolicy.reconcile(otherTarget, Set.of(member("b")), List.of(), T).key()).isNotEqualTo(decision.key());
        assertThatThrownBy(() -> FrozenGroupPolicy.reconcile(original, Set.of(member("new-joiner")), List.of(), T)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new FrozenGroupPolicy.Batch(original.deliveryKey(), 7, List.of(member("a"), member("a")))).isInstanceOf(TapstateException.class);
        var foreign = new FrozenGroupPolicy.Member(new AlarmKey(new AlarmKey.Scope("other", "cluster"), member("a").key().resource(), "pipeline.failed", "rule"), 1);
        assertThatThrownBy(() -> new FrozenGroupPolicy.Batch(original.deliveryKey(), 7, List.of(foreign))).isInstanceOf(TapstateException.class);
        var many = new ArrayList<FrozenGroupPolicy.Member>();
        for (int i = 0; i < 101; i++) { many.add(member("p" + i)); }
        assertThatThrownBy(() -> new FrozenGroupPolicy.Batch(original.deliveryKey(), 7, many)).isInstanceOf(TapstateException.class);
    }
}
