package io.tapstate.core.alerting;

import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.AlertChannelPolicy.Deployment.*;
import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class AlertTestCommandTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlertChannelPolicy.Capability CHANNEL = new AlertChannelPolicy.Capability("webhook", 1, AlertChannelPolicy.Family.OP_EXTERNAL);
    static final AlertTestCommand.ClientId ID = new AlertTestCommand.ClientId(T, "00112233445566778899aabbccddeeff");
    static final AlertTestCommand.Key KEY = new AlertTestCommand.Key(new AlarmKey.Scope("org", "cluster"), "principal-1", ID);
    static final AlertTestCommand.Candidate CANDIDATE = new AlertTestCommand.Candidate("candidate-1", "c".repeat(64));
    static final AlertTestCommand.ExistingReceiver RECEIVER = new AlertTestCommand.ExistingReceiver("receiver-1", 1);
    static final AlertTestCommand.Parameters PARAMETERS = parameters(CANDIDATE, RECEIVER, "target", "binding-1", null, "a".repeat(64));
    static AlertTestCommand.Parameters parameters(AlertTestCommand.Candidate candidate, AlertTestCommand.ExistingReceiver receiver,
            String target, String binding, AlertTestCommand.Route route, String payload) {
        return new AlertTestCommand.Parameters("webhook", candidate, receiver, target, binding, route, payload);
    }
    static AlertTestCommand.Admission admit(AlertTestCommand.Key key, AlertTestCommand.Parameters parameters,
            AlertTestCommand.Accepted existing, Instant now) {
        return AlertTestCommand.admit(ON_PREM, CHANNEL, key, parameters, existing, now);
    }
    static AlertTestQuota.Decision quota(AlertTestCommand.Admission admission, Instant now, AlertTestQuota.Window window,
            AlertTestQuota.ReceiverBusy receiver) {
        return AlertTestQuota.evaluate(admission, now, window, receiver, null);
    }
    @Test void clientIdIsCanonicalAndCannotMoveItsCreationTimeWithoutChangingIdentity() {
        assertThat(ID.value()).isEqualTo("act1.1792540800.00112233445566778899aabbccddeeff");
        assertThat(AlertTestCommand.ClientId.parse(ID.value())).isEqualTo(ID);
        for (String invalid : new String[]{"act1.01792540800.00112233445566778899aabbccddeeff", "act2.1792540800.00112233445566778899aabbccddeeff",
                "act1.1792540800.00112233445566778899AABBCCDDEEFF", "act1.-1.00112233445566778899aabbccddeeff", ID.value() + ".x"}) {
            assertThatThrownBy(() -> AlertTestCommand.ClientId.parse(invalid)).isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> new AlertTestCommand.ClientId(T.plusNanos(1), ID.nonce())).isInstanceOf(TapstateException.class);
        assertThat(new AlertTestCommand.Key(KEY.scope(), KEY.actorPrincipal(), new AlertTestCommand.ClientId(T.plusSeconds(1), ID.nonce())).testId())
                .isNotEqualTo(KEY.testId());
    }
    @Test void firstAdmissionHasExactAgeAndFutureBoundariesAndExistingCommandsReplayBeforeExpiryChecks() {
        assertThat(admit(KEY, PARAMETERS, null, T.minusSeconds(30)).replay()).isFalse();
        assertThatThrownBy(() -> admit(KEY, PARAMETERS, null, T.minusSeconds(30).minusNanos(1)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(AlertError.COMMAND_NOT_YET_VALID));
        var admission = admit(KEY, PARAMETERS, null, T.plusSeconds(299));
        assertThat(admission.command().deadline()).isEqualTo(T.plusSeconds(599));
        assertThat(admit(KEY, PARAMETERS, admission.command(), T.plusSeconds(86400)).command()).isSameAs(admission.command());
        assertThatThrownBy(() -> admit(KEY, PARAMETERS, null, T.plusSeconds(300)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(AlertError.COMMAND_EXPIRED));
        assertThatThrownBy(() -> admit(KEY, PARAMETERS, null, T.plusSeconds(86400))).isInstanceOf(TapstateException.class);
    }
    @Test void allFrozenParameterChangesConflictEvenAfterTheFirstAdmissionWindow() {
        var original = admit(KEY, PARAMETERS, null, T).command();
        for (var changed : List.of(
                parameters(CANDIDATE, new AlertTestCommand.ExistingReceiver("receiver-2", 1), "target", "binding-1", null, "a".repeat(64)),
                parameters(CANDIDATE, new AlertTestCommand.ExistingReceiver("receiver-1", 2), "target", "binding-1", null, "a".repeat(64)),
                parameters(CANDIDATE, null, "target", "binding-1", null, "a".repeat(64)),
                parameters(new AlertTestCommand.Candidate("candidate-2", CANDIDATE.digest()), RECEIVER, "target", "binding-1", null, "a".repeat(64)),
                parameters(new AlertTestCommand.Candidate(CANDIDATE.id(), "d".repeat(64)), RECEIVER, "target", "binding-1", null, "a".repeat(64)),
                parameters(CANDIDATE, RECEIVER, "other", "binding-1", null, "a".repeat(64)),
                parameters(CANDIDATE, RECEIVER, "target", "binding-2", null, "a".repeat(64)),
                parameters(CANDIDATE, RECEIVER, "target", "binding-1", new AlertTestCommand.Route("route-1", 1, 1), "a".repeat(64)),
                parameters(CANDIDATE, RECEIVER, "target", "binding-1", null, "b".repeat(64)))) {
            assertThatThrownBy(() -> admit(KEY, changed, original, T.plusSeconds(86400)))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(AlertError.COMMAND_CONFLICT));
        }
        var routed = parameters(CANDIDATE, RECEIVER, "target", "binding-1", new AlertTestCommand.Route("route-1", 1, 1), "a".repeat(64));
        var routedCommand = admit(KEY, routed, null, T).command();
        var changedMessage = parameters(CANDIDATE, RECEIVER, "target", "binding-1", new AlertTestCommand.Route("route-1", 1, 2), "a".repeat(64));
        assertThatThrownBy(() -> admit(KEY, changedMessage, routedCommand, T)).isInstanceOf(TapstateException.class);
    }
    @Test void newUnsavedReceiverHasNoFakeSavedIdentityAndReplayKeepsItsCandidate() {
        var unsaved = parameters(CANDIDATE, null, "target", "binding-1", null, "a".repeat(64));
        var original = admit(KEY, unsaved, null, T);
        assertThat(original.command().parameters().receiver()).isNull();
        assertThat(original.command().parameters().candidate()).isEqualTo(CANDIDATE);
        assertThat(quota(original, T, AlertTestQuota.empty(KEY, T), null).allowed()).isTrue();
        assertThat(admit(KEY, unsaved, original.command(), T.plusSeconds(600)).command()).isSameAs(original.command());
        assertThatThrownBy(() -> quota(original, T, AlertTestQuota.empty(KEY, T),
                new AlertTestQuota.ReceiverBusy(KEY.scope(), "receiver-1", T.plusSeconds(30))))
                .isInstanceOf(TapstateException.class);
    }
    @Test void candidateAndSavedBaselinesRejectIncompleteValues() {
        for (String invalid : new String[]{"", "a".repeat(63), "a".repeat(65), "A".repeat(64)}) {
            assertThatThrownBy(() -> new AlertTestCommand.Candidate("candidate", invalid)).isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> new AlertTestCommand.ExistingReceiver("receiver", 0)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> parameters(null, null, "target", "binding", null, "a".repeat(64))).isInstanceOf(TapstateException.class);
    }
    @Test void fullTrustedScopeActorAndClientIdentitySeparateTestIdsAndCloudCannotAdmitTests() {
        var otherActor = new AlertTestCommand.Key(KEY.scope(), "principal-2", ID);
        var otherScope = new AlertTestCommand.Key(new AlarmKey.Scope("other", "cluster"), KEY.actorPrincipal(), ID);
        assertThat(KEY.testId()).isNotEqualTo(otherActor.testId()).isNotEqualTo(otherScope.testId()).startsWith("test-");
        for (var deployment : new AlertChannelPolicy.Deployment[]{CLOUD_MANAGED, UNRESOLVED}) {
            assertThatThrownBy(() -> AlertTestCommand.admit(deployment, CHANNEL, KEY, PARAMETERS, null, T)).isInstanceOf(TapstateException.class);
        }
        var cloud = new AlertChannelPolicy.Capability("cloud-sdk", 1, AlertChannelPolicy.Family.CLOUD_SDK);
        assertThatThrownBy(() -> AlertTestCommand.admit(ON_PREM, cloud, KEY, PARAMETERS, null, T)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> admit(KEY, PARAMETERS, admit(otherActor, PARAMETERS, null, T).command(), T)).isInstanceOf(IllegalStateException.class);
    }
    @Test void quotaConsumesActorAndScopeTogetherAndReplayNeverConsumesOrWaits() {
        var admission = admit(KEY, PARAMETERS, null, T);
        var window = AlertTestQuota.empty(KEY, T);
        for (int i = 1; i <= 3; i++) {
            var result = quota(admission, T, window, null);
            assertThat(result.allowed()).isTrue(); window = result.candidate();
            assertThat(window.actorAccepted()).isEqualTo(i); assertThat(window.scopeAccepted()).isEqualTo(i);
        }
        var blocked = quota(admission, T, window, null);
        assertThat(blocked.allowed()).isFalse(); assertThat(blocked.candidate()).isSameAs(window);
        assertThat(blocked.refusal()).isEqualTo(AlertTestQuota.Refusal.ACTOR_LIMIT);
        assertThat(blocked.retryAt()).isEqualTo(T.plusSeconds(60));
        var replay = admit(KEY, PARAMETERS, admission.command(), T);
        var replayResult = AlertTestQuota.evaluate(replay, T, window, new AlertTestQuota.ReceiverBusy(KEY.scope(), "receiver-1", T.plusSeconds(300)),
                new AlertTestQuota.TargetBusy(KEY.scope(), "webhook", "target", T.plusSeconds(300)));
        assertThat(replayResult.allowed()).isTrue(); assertThat(replayResult.candidate()).isSameAs(window);
        var fullScope = new AlertTestQuota.Window(KEY.scope(), KEY.actorPrincipal(), T, 1, 10);
        var scopeBlocked = quota(admission, T, fullScope, null);
        assertThat(scopeBlocked.refusal()).isEqualTo(AlertTestQuota.Refusal.SCOPE_LIMIT);
        assertThat(scopeBlocked.candidate()).isSameAs(fullScope);
    }
    @Test void receiverOccupancyMustSettleAndDoesNotSlideItsDeadlineWhenRechecked() {
        var original = admit(KEY, PARAMETERS, null, T);
        var busy = new AlertTestQuota.ReceiverBusy(KEY.scope(), "receiver-1", T.plusSeconds(20));
        var first = quota(original, T, AlertTestQuota.empty(KEY, T), busy);
        assertThat(first.allowed()).isFalse(); assertThat(first.retryAt()).isEqualTo(T.plusSeconds(20));
        var later = admit(KEY, PARAMETERS, null, T.plusSeconds(21));
        var overdue = quota(later, T.plusSeconds(21), AlertTestQuota.empty(KEY, T.plusSeconds(21)), busy);
        assertThat(overdue.allowed()).isFalse(); assertThat(overdue.retryAt()).isEqualTo(busy.originalDeadline());
        assertThat(quota(later, T.plusSeconds(21), overdue.candidate(), null).allowed()).isTrue();
        var changedBaseline = parameters(CANDIDATE, new AlertTestCommand.ExistingReceiver("receiver-1", 2), "target", "binding-1", null, "a".repeat(64));
        assertThat(quota(admit(KEY, changedBaseline, null, T), T, AlertTestQuota.empty(KEY, T), busy).allowed()).isFalse();
    }
    @Test void changingCandidateOrCommandCannotEvadeTheOriginalCanonicalTargetOccupancy() {
        var nextKey = new AlertTestCommand.Key(KEY.scope(), KEY.actorPrincipal(), new AlertTestCommand.ClientId(T, "f".repeat(32)));
        var unsaved = parameters(new AlertTestCommand.Candidate("candidate-2", "d".repeat(64)), null, "target", "binding-1", null, "a".repeat(64));
        var admission = admit(nextKey, unsaved, null, T);
        var window = AlertTestQuota.empty(nextKey, T);
        var target = new AlertTestQuota.TargetBusy(KEY.scope(), "webhook", "target", T.plusSeconds(20));
        var blocked = AlertTestQuota.evaluate(admission, T, window, null, target);
        assertThat(blocked.allowed()).isFalse(); assertThat(blocked.refusal()).isEqualTo(AlertTestQuota.Refusal.TARGET_BUSY);
        assertThat(blocked.candidate()).isSameAs(window); assertThat(blocked.retryAt()).isEqualTo(T.plusSeconds(20));
        var later = admit(nextKey, unsaved, null, T.plusSeconds(21));
        var overdue = AlertTestQuota.evaluate(later, T.plusSeconds(21), window, null, target);
        assertThat(overdue.allowed()).isFalse(); assertThat(overdue.retryAt()).isEqualTo(target.originalDeadline());
        assertThat(AlertTestQuota.evaluate(later, T.plusSeconds(21), window, null, null).allowed()).isTrue();
        var saved = admit(KEY, PARAMETERS, null, T);
        var combined = AlertTestQuota.evaluate(saved, T, AlertTestQuota.empty(KEY, T),
                new AlertTestQuota.ReceiverBusy(KEY.scope(), "receiver-1", T.plusSeconds(40)), target);
        assertThat(combined.allowed()).isFalse(); assertThat(combined.retryAt()).isEqualTo(T.plusSeconds(40));
    }
    @Test void minuteBoundariesAndQuotaKeysDoNotBorrowAnotherWindowOrIdentity() {
        assertThat(AlertTestQuota.minute(T.minusNanos(1))).isEqualTo(T.minusSeconds(60));
        var admission = admit(KEY, PARAMETERS, null, T.plusSeconds(60));
        var next = AlertTestQuota.empty(KEY, T.plusSeconds(60));
        assertThat(quota(admission, T.plusSeconds(60), next, null).allowed()).isTrue();
        assertThatThrownBy(() -> quota(admission, T.plusSeconds(60), AlertTestQuota.empty(KEY, T), null)).isInstanceOf(TapstateException.class);
        var wrongScope = new AlertTestQuota.Window(new AlarmKey.Scope("other", "cluster"), KEY.actorPrincipal(), next.minute(), 0, 0);
        assertThatThrownBy(() -> quota(admission, T.plusSeconds(60), wrongScope, null)).isInstanceOf(TapstateException.class);
        var wrongActor = new AlertTestQuota.Window(KEY.scope(), "other", next.minute(), 0, 0);
        assertThatThrownBy(() -> quota(admission, T.plusSeconds(60), wrongActor, null)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> quota(admission, T.plusSeconds(60), next,
                new AlertTestQuota.ReceiverBusy(KEY.scope(), "receiver-2", T.plusSeconds(300)))).isInstanceOf(TapstateException.class);
        for (var target : List.of(new AlertTestQuota.TargetBusy(new AlarmKey.Scope("other", "cluster"), "webhook", "target", T.plusSeconds(300)),
                new AlertTestQuota.TargetBusy(KEY.scope(), "email", "target", T.plusSeconds(300)),
                new AlertTestQuota.TargetBusy(KEY.scope(), "webhook", "other-target", T.plusSeconds(300)))) {
            assertThatThrownBy(() -> AlertTestQuota.evaluate(admission, T.plusSeconds(60), next, null, target)).isInstanceOf(TapstateException.class);
        }
    }
}
