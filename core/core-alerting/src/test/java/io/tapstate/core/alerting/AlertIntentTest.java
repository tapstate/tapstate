package io.tapstate.core.alerting;

import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import io.tapstate.core.alerting.AlertIntent.*;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertIntentTest {
    private static final Instant C = Instant.parse("2026-10-27T00:00:00.123456789Z");
    private static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    private static final String UUID = "11111111-1111-4111-8111-111111111111";
    private static final Payload PAYLOAD = new Payload("frozen-message".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    @Test void completeBusinessKeyDefinesStableIdentityAndMutablePresentationDoesNotEnterIt() {
        Frozen frozen = frozen(false);
        assertThat(frozen.identity().deliveryId()).startsWith("delivery-").hasSize(73);
        var key = ((Business) frozen.identity()).key();
        assertThat(new Business(key).deliveryId()).isEqualTo(frozen.identity().deliveryId());
        var changed = new DeliveryKey(key.notification(), key.channelType(), key.receiverIncarnation(), "different@example.test");
        assertThat(new Business(changed).deliveryId()).isNotEqualTo(frozen.identity().deliveryId());
        assertThat(initial(frozen).state().summary()).isEqualTo(Outcome.NOT_ATTEMPTED);
    }

    @Test void payloadOwnershipAndTypedFrozenTargetCannotBeMutatedOrSubstituted() {
        byte[] bytes = "frozen-message".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Payload value = new Payload(bytes); bytes[0] = 'X'; byte[] copy = value.bytes(); copy[0] = 'Y';
        assertThat(value).isEqualTo(PAYLOAD);
        assertThat(value.toString()).doesNotContain("frozen-message");
        Frozen frozen = frozen(false);
        var target = (ReceiverTarget) frozen.target();
        var foreign = new AlertConfiguration.Identity(AlertConfiguration.Kind.RECEIVER, new AlarmKey.Scope("other", "cluster-a"), "receiver-a", UUID);
        assertThatThrownBy(() -> new Frozen(frozen.identity(), frozen.channel(), frozen.route(),
                new ReceiverTarget(foreign, 1, target.transport(), null), frozen.content(), frozen.members(), frozen.retry(),
                frozen.retention(), C, frozen.deadline())).isInstanceOf(TapstateException.class);
    }

    @Test void anExactStoredPermitAndLateAcceptedEvidenceKeepTheirOriginalDeadlineAndFixedEnd() {
        Frozen frozen = frozen(false);
        DeliveryAttempt attempt = permit(frozen);
        var pending = record(frozen, List.of(Attempt.pending(attempt)), Outcome.UNKNOWN, null, false, null);
        assertThat(pending.state().attempts().getFirst().permit().deadline()).isEqualTo(C.plusSeconds(60));
        DeliveryAttempt ended = attempt.settleDeadline(C.plusSeconds(90));
        DeliveryAttempt restored = DeliveryAttempt.restore(ended.permit(), ended.outcome(), ended.endedAt());
        assertThat(restored.endedAt()).isEqualTo(C.plusSeconds(60));
        Instant acceptedAt = C.plusSeconds(120);
        var accepted = restored.receipt(restored.permit(), Outcome.ACCEPTED, acceptedAt).attempt();
        var stored = new Attempt(accepted.permit(), accepted.outcome(), accepted.endedAt(), acceptedAt,
                new Protocol(Phase.FINAL_RESPONSE, Action.STOP, 250, "smtp", null), new Receipt("channel-protocol", "provider-id", null));
        var result = record(frozen, List.of(stored), Outcome.ACCEPTED, acceptedAt, true, accepted.endedAt());
        assertThat(result.state().attempts().getFirst().endedAt()).isEqualTo(C.plusSeconds(60));
        assertThat(result.state().retained().acceptedAt()).isEqualTo(acceptedAt);
        assertThat(result.frozen().deadline()).isEqualTo(frozen.deadline());
    }

    @Test void aManagedAcceptedResultRequiresItsOriginalSdkEventReceiptAndOneAttempt() {
        Frozen frozen = frozen(true); DeliveryAttempt original = permit(frozen);
        Instant acceptedAt = C.plusSeconds(1);
        DeliveryAttempt accepted = original.receipt(original.permit(), Outcome.ACCEPTED, acceptedAt).attempt();
        var withoutReceipt = new Attempt(accepted.permit(), Outcome.ACCEPTED, accepted.endedAt(), acceptedAt,
                new Protocol(Phase.FINAL_RESPONSE, Action.STOP, 200, "sdk-event-center", null), null);
        assertThatThrownBy(() -> record(frozen, List.of(withoutReceipt), Outcome.ACCEPTED, acceptedAt, true, acceptedAt))
                .isInstanceOf(TapstateException.class);
        var received = new Attempt(accepted.permit(), Outcome.ACCEPTED, accepted.endedAt(), acceptedAt,
                withoutReceipt.protocol(), new Receipt("sdk-event-center", "event-a", "request-a"));
        assertThat(record(frozen, List.of(received), Outcome.ACCEPTED, acceptedAt, true, acceptedAt).state().summary()).isEqualTo(Outcome.ACCEPTED);
        var retry = new ChannelRetryPolicy(2, 0, List.of(Duration.ofSeconds(1)), Duration.ofDays(1), Duration.ofSeconds(30));
        assertThatThrownBy(() -> new Frozen(frozen.identity(), frozen.channel(), frozen.route(), frozen.target(), frozen.content(),
                frozen.members(), retry, frozen.retention(), C, frozen.deadline())).isInstanceOf(TapstateException.class);
    }

    @Test void acceptedEvidenceCannotRestoreAnIntentWithOpenPermissionForAnotherAttempt() {
        Frozen frozen = frozen(false); DeliveryAttempt original = permit(frozen);
        Instant at = C.plusSeconds(1);
        var accepted = original.receipt(original.permit(), Outcome.ACCEPTED, at).attempt();
        var stored = new Attempt(accepted.permit(), Outcome.ACCEPTED, accepted.endedAt(), at,
                new Protocol(Phase.FINAL_RESPONSE, Action.STOP, 250, "smtp", null), null);
        assertThatThrownBy(() -> record(frozen, List.of(stored), Outcome.ACCEPTED, at, false, null))
                .isInstanceOf(TapstateException.class);
    }

    @Test void aFixedStopCannotPrecedeTheOriginalAttemptSettlementCutoff() {
        Frozen frozen = frozen(false);
        DeliveryAttempt settled = permit(frozen).settleDeadline(C.plusSeconds(90));
        var attempt = new Attempt(settled.permit(), settled.outcome(), settled.endedAt(), null, null, null);
        var retained = initial(frozen).state().retained().value(frozen).stop(C, settled.permit().deadline());
        assertThatThrownBy(() -> new AlertIntent.Record(frozen, new State(new AlertOwnership(1, "owner-a", C.plusSeconds(120)),
                0, true, StopReason.CANCELLED, null, 1, List.of(attempt), Outcome.UNKNOWN, Retained.capture(retained),
                PAYLOAD, null, null, null, null))).isInstanceOf(TapstateException.class);
    }

    @Test void lateAcceptedEvidenceStopsNewPermitsButKeepsAnAlreadyGrantedRequestAndEarlierStopReason() {
        Frozen frozen = frozen(false); DeliveryAttempt first = permit(frozen);
        Instant acceptedAt = C.plusSeconds(120);
        var accepted = first.settleDeadline(C.plusSeconds(60)).receipt(first.permit(), Outcome.ACCEPTED, acceptedAt).attempt();
        var recorded = new Attempt(accepted.permit(), Outcome.ACCEPTED, accepted.endedAt(), acceptedAt,
                new Protocol(Phase.FINAL_RESPONSE, Action.STOP, 250, "smtp", null), null);
        var second = DeliveryAttempt.grant(SCOPE, frozen.identity().deliveryId(), "attempt-b", 2, 1, "owner-a", frozen.retry(), C,
                frozen.deadline(), C.plusSeconds(61)).orElseThrow();
        var retained = initial(frozen).state().retained().value(frozen).accept(acceptedAt, second.permit().deadline(), acceptedAt).orElseThrow();
        var value = new AlertIntent.Record(frozen, new State(new AlertOwnership(1, "owner-a", C.plusSeconds(180)),
                0, true, StopReason.REVOKED, null, 2, List.of(recorded, Attempt.pending(second)), Outcome.ACCEPTED,
                Retained.capture(retained), PAYLOAD, null, null, null, null, C.plusSeconds(100), C.plusSeconds(60)));
        assertThat(value.state().attempts().getLast().endedAt()).isNull();
        assertThat(value.state().attempts().getLast().permit().deadline()).isEqualTo(C.plusSeconds(121));
        assertThat(value.state().stopReason()).isEqualTo(StopReason.REVOKED);
        assertThat(value.state().retained().stoppedAt()).isNull();
    }

    @Test void recoveryComputesTheOriginalFixedStopWithoutUsingItsLaterRestartTime() {
        Frozen frozen = frozen(false); var original = permit(frozen);
        var retained = initial(frozen).state().retained().value(frozen);
        var waiting = new AlertIntent.Record(frozen, new State(new AlertOwnership(1, "owner-a", C.plusSeconds(120)),
                0, true, StopReason.CANCELLED, null, 1, List.of(Attempt.pending(original)), Outcome.UNKNOWN, Retained.capture(retained),
                PAYLOAD, null, null, null, null, C.plusSeconds(10), null));
        var settled = original.settleDeadline(C.plusSeconds(3 * 3600));
        var attempt = new Attempt(settled.permit(), settled.outcome(), settled.endedAt(), null, null, null);
        var fixed = retained.stop(C.plusSeconds(60), original.permit().deadline());
        var restored = new AlertIntent.Record(frozen, new State(waiting.state().owner(), 0, true, StopReason.CANCELLED, null,
                1, List.of(attempt), Outcome.UNKNOWN, Retained.capture(fixed), PAYLOAD, null, null, null, null, C.plusSeconds(10), C.plusSeconds(60)));
        assertThat(restored.state().retained().stoppedAt()).isEqualTo(C.plusSeconds(60));
        var extended = retained.stop(C.plusSeconds(3 * 3600), original.permit().deadline());
        assertThatThrownBy(() -> new AlertIntent.Record(frozen, new State(waiting.state().owner(), 0, true, StopReason.CANCELLED, null,
                1, List.of(attempt), Outcome.UNKNOWN, Retained.capture(extended), PAYLOAD, null, null, null, null, C.plusSeconds(10), C.plusSeconds(60))))
                .isInstanceOf(TapstateException.class);
    }

    @Test void attemptRestorationRejectsExpandedCutoffsUnpermittedResultsAndOverlappingOriginalRequests() {
        Frozen frozen = frozen(false); DeliveryAttempt attempt = permit(frozen);
        assertThatThrownBy(() -> DeliveryAttempt.restore(attempt.permit(), Outcome.ACCEPTED, null)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> DeliveryAttempt.restore(attempt.permit(), Outcome.NOT_ATTEMPTED, C)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> DeliveryAttempt.restore(attempt.permit(), Outcome.UNKNOWN, C.plusSeconds(61))).isInstanceOf(TapstateException.class);
        var second = DeliveryAttempt.grant(SCOPE, frozen.identity().deliveryId(), "attempt-b", 2, 1, "owner-a", frozen.retry(), C,
                frozen.deadline(), C.plusSeconds(1)).orElseThrow();
        assertThatThrownBy(() -> record(frozen, List.of(Attempt.pending(attempt), Attempt.pending(second)), Outcome.UNKNOWN, null, false, null))
                .isInstanceOf(TapstateException.class);
    }

    @Test void frozenRetentionReopensWithOriginalCutsAndCannotExpandThemUsingCurrentDefaults() {
        Frozen frozen = frozen(false);
        var original = DeliveryRetention.begin(frozen.retention().value(), DeliveryRetention.Kind.OP_EXTERNAL, C, frozen.deadline());
        var stopped = original.stop(C.plusSeconds(1), C.plusSeconds(1));
        var captured = Retained.capture(stopped);
        assertThat(captured.value(frozen).payloadUntil()).isEqualTo(stopped.payloadUntil());
        assertThat(captured.value(frozen).resultUntil()).isEqualTo(stopped.resultUntil());
        assertThatThrownBy(() -> DeliveryRetention.restore(original.policy(), original.kind(), C, frozen.deadline(),
                original.payloadUntil().plusSeconds(1), original.resultUntil(), null, null)).isInstanceOf(TapstateException.class);
    }

    @Test void anUnsavedTestCandidateHasOnlyItsOriginalCommandIdentityAndNoBusinessEpisode() {
        String recipient = "a@example.test";
        var transport = new AlertReceiver.Email("smtp.example.test", 465, AlertReceiver.Encryption.IMPLICIT_TLS,
                AlertReceiver.Authentication.NONE, null, "alerts@example.test", List.of(recipient), AlertReceiver.Trust.SYSTEM);
        var key = new AlertTestCommand.Key(SCOPE, "actor-a",
                new AlertTestCommand.ClientId(Instant.ofEpochSecond(C.getEpochSecond()), "1".repeat(32)));
        var parameters = new AlertTestCommand.Parameters("email", new AlertTestCommand.Candidate("candidate-a", "a".repeat(64)),
                null, recipient, "candidate-binding-a", null, PAYLOAD.sha256());
        var accepted = new AlertTestCommand.Accepted(key, parameters, C, C.plusSeconds(300));
        var identity = new AlertIntent.Test(accepted);
        var policy = Policy.capture(AlertRetentionPolicy.defaults(Deployment.ON_PREM));
        var frozen = new Frozen(identity, new AlertChannelPolicy.Capability("email", 1, AlertChannelPolicy.Family.OP_EXTERNAL),
                null, new CandidateTarget(transport, null, 0, null), new Content("message/rfc822", PAYLOAD.sha256(),
                        new Template("channel-test", 1, 1, "en", MessageSettings.empty()), false), List.of(),
                ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5), Duration.ofSeconds(60)), policy, C, accepted.deadline());
        var retained = Retained.capture(DeliveryRetention.begin(policy.value(), DeliveryRetention.Kind.CHANNEL_TEST, C, accepted.deadline()));
        var record = new AlertIntent.Record(frozen, new State(AlertOwnership.unclaimed(), 0, false, null, C, 0, List.of(), Outcome.NOT_ATTEMPTED,
                retained, PAYLOAD, null, null, null, null));
        assertThat(record.frozen().identity().deliveryId()).isEqualTo(key.testId());
        assertThat(record.frozen().members()).isEmpty();
        assertThat(((CandidateTarget) record.frozen().target()).existingReceiver()).isNull();
        assertThat(record.frozen().retry().maximumTotalAttempts()).isOne();
    }

    @Test void partialAutomaticCyclesDoNotDonateUnusedSlotsToTheLastManualCycle() {
        Frozen base = frozen(false);
        var retry = new ChannelRetryPolicy(5, 2, java.util.Collections.nCopies(4, Duration.ofSeconds(1)), Duration.ofDays(1), Duration.ofSeconds(60));
        Frozen frozen = new Frozen(base.identity(), base.channel(), base.route(), base.target(), base.content(), base.members(), retry,
                base.retention(), C, base.deadline());
        var attempts = new java.util.ArrayList<Attempt>();
        for (int index = 0; index < 7; index++) {
            Instant grant = C.plusSeconds(index * 2L);
            var original = DeliveryAttempt.grant(SCOPE, frozen.identity().deliveryId(), "attempt-" + index, index + 1, 1, "owner-a", retry,
                    C, frozen.deadline(), grant).orElseThrow();
            var ended = original.receipt(original.permit(), Outcome.NOT_ACCEPTED, grant.plusSeconds(1)).attempt();
            attempts.add(new Attempt(ended.permit(), ended.outcome(), ended.endedAt(), null,
                    new Protocol(Phase.FINAL_RESPONSE, index < 2 ? Action.STOP : Action.RETRY, index < 2 ? 535 : 450, "smtp", null), null));
        }
        var retained = initial(frozen).state().retained().value(frozen).stop(C.plusSeconds(13), C.plusSeconds(72));
        var value = new AlertIntent.Record(frozen, new State(new AlertOwnership(1, "owner-a", C.plusSeconds(120)), 2, true,
                StopReason.EXHAUSTED, null, 7, attempts, Outcome.NOT_ACCEPTED, Retained.capture(retained), PAYLOAD, null, null, null, null,
                C.plusSeconds(12), C.plusSeconds(13), List.of(0, 1, 2)));
        assertThat(value.state().currentCycleConsumed()).isEqualTo(5);
        assertThat(value.state().attemptsConsumed()).isEqualTo(7);
        assertThat(value.state().permitsStopped()).isTrue();
        assertThatThrownBy(() -> new AlertIntent.Record(frozen, new State(value.state().owner(), 1, false, null, null, 7, attempts,
                Outcome.NOT_ACCEPTED, Retained.capture(initial(frozen).state().retained().value(frozen)), PAYLOAD, null, null, null, null,
                null, C.plusSeconds(13), List.of(0, 1)))).isInstanceOf(TapstateException.class);
    }

    private static Frozen frozen(boolean cloud) {
        var ruleId = new AlertConfiguration.Identity(AlertConfiguration.Kind.RULE, SCOPE, "rule-a", UUID);
        var routeId = new AlertConfiguration.Identity(AlertConfiguration.Kind.SUBSCRIPTION, SCOPE, "route-a", UUID);
        var receiverId = new AlertConfiguration.Identity(AlertConfiguration.Kind.RECEIVER, SCOPE, "receiver-a", UUID);
        var alarm = new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", "orders", "pipeline-1"), "pipeline.failed", "rule-a");
        var evaluation = new AlertRule(alarm, 1, 1, "producer-a", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(30), Duration.ofSeconds(30));
        String channel = cloud ? "cloud-sdk" : "email", receiver = cloud ? "managed-target" : UUID;
        String targetId = cloud ? "cluster-a" : "a@example.test";
        var key = new DeliveryKey(DeliveryKey.NotificationRound.forEpisode(alarm, 1, "route-a", cloud ? "managed-route" : UUID,
                DeliveryKey.Kind.FAULT, 1), channel, receiver, targetId);
        Target target = cloud ? new SdkTarget(receiver, targetId, "cluster-binding-a") : new ReceiverTarget(receiverId, 1,
                new AlertReceiver.Email("smtp.example.test", 465, AlertReceiver.Encryption.IMPLICIT_TLS,
                        AlertReceiver.Authentication.NONE, null, "alerts@example.test", List.of(targetId), AlertReceiver.Trust.SYSTEM), null);
        Route route = cloud ? new SystemRoute("route-a", "managed-route", 1) : new ConfiguredRoute(routeId, 1);
        var policy = AlertRetentionPolicy.defaults(cloud ? Deployment.CLOUD_MANAGED : Deployment.ON_PREM);
        var retry = cloud ? new ChannelRetryPolicy(1, 0, List.of(), Duration.ofDays(1), Duration.ofSeconds(30)) : ChannelRetryPolicy.emailDefaults();
        return new Frozen(new Business(key), new AlertChannelPolicy.Capability(channel, cloud ? 2 : 1,
                cloud ? AlertChannelPolicy.Family.CLOUD_SDK : AlertChannelPolicy.Family.OP_EXTERNAL), route, target,
                new Content(cloud ? "application/json" : "message/rfc822", PAYLOAD.sha256(),
                        new Template("fault", 1, 1, "en", MessageSettings.empty()), false),
                List.of(new Member(evaluation, 1, ruleId, 0)), retry, Policy.capture(policy), C, C.plus(Duration.ofDays(1)));
    }
    private static AlertIntent.Record initial(Frozen frozen) {
        var retained = Retained.capture(DeliveryRetention.begin(frozen.retention().value(),
                frozen.retention().deployment() == Deployment.CLOUD_MANAGED ? DeliveryRetention.Kind.CLOUD_SDK : DeliveryRetention.Kind.OP_EXTERNAL,
                C, frozen.deadline()));
        return new AlertIntent.Record(frozen, new State(AlertOwnership.unclaimed(), 0, false, null, C, 0, List.of(), Outcome.NOT_ATTEMPTED,
                retained, PAYLOAD, null, null, null, null));
    }
    private static DeliveryAttempt permit(Frozen frozen) {
        return DeliveryAttempt.grant(SCOPE, frozen.identity().deliveryId(), "attempt-a", 1, 1, "owner-a", frozen.retry(), C, frozen.deadline(), C).orElseThrow();
    }
    private static AlertIntent.Record record(Frozen frozen, List<Attempt> attempts, Outcome summary, Instant acceptedAt, boolean stopped, Instant end) {
        DeliveryRetention retained = initial(frozen).state().retained().value(frozen);
        boolean exhausted = attempts.size() == frozen.retry().maximumTotalAttempts();
        Instant stopAt = exhausted ? attempts.getLast().permit().grantedAt() : stopped ? acceptedAt : null;
        boolean allEnded = attempts.stream().allMatch(attempt -> attempt.endedAt() != null);
        Instant lastEnd = attempts.stream().map(Attempt::endedAt).filter(java.util.Objects::nonNull).max(Instant::compareTo).orElse(null);
        Instant fixed = stopAt != null && allEnded ? lastEnd == null || stopAt.isAfter(lastEnd) ? stopAt : lastEnd : null;
        if (fixed != null && (acceptedAt == null || fixed.isBefore(acceptedAt))) { retained = retained.stop(fixed, C.plusSeconds(60)); }
        if (acceptedAt != null) { retained = retained.accept(acceptedAt, C.plusSeconds(60), acceptedAt).orElseThrow(); }
        if (fixed != null && retained.stoppedAt() == null) { retained = retained.stop(fixed, C.plusSeconds(60)); }
        return new AlertIntent.Record(frozen, new State(new AlertOwnership(1, "owner-a", C.plusSeconds(120)), 0, stopped || exhausted,
                exhausted ? StopReason.EXHAUSTED : stopped ? StopReason.ACCEPTED : null, null, attempts.size(), attempts, summary,
                Retained.capture(retained), PAYLOAD, null, null, null, null, stopAt, lastEnd));
    }
}
