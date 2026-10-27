package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import io.tapstate.core.alerting.AlertIntent.*;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

final class AlertDeliveryFixtures {
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    static final Instant AT = Instant.parse("2026-10-27T00:00:00.123456789Z");
    static final String UUID = "11111111-1111-4111-8111-111111111111";
    static final Payload PAYLOAD = new Payload("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    private AlertDeliveryFixtures() { }

    static AlertIntent.Record business() { return business(false, PAYLOAD, ChannelRetryPolicy.emailDefaults()); }
    static AlertIntent.Record business(boolean cloud, Payload payload, ChannelRetryPolicy retry) {
        var alarm = new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", "orders", "pipeline-1"), "pipeline.failed", "rule-a");
        var evaluation = new AlertRule(alarm, 1, 1, "producer-a", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(30), Duration.ofSeconds(30));
        var routeId = id(AlertConfiguration.Kind.SUBSCRIPTION, "route-a");
        var receiver = id(AlertConfiguration.Kind.RECEIVER, "receiver-a");
        String channel = cloud ? "cloud-sdk" : "email", receiverIncarnation = cloud ? "managed-target" : UUID;
        String targetId = cloud ? SCOPE.clusterId() : "a@example.test";
        var key = new DeliveryKey(DeliveryKey.NotificationRound.forEpisode(alarm, 1, "route-a", cloud ? "managed-route" : UUID,
                DeliveryKey.Kind.FAULT, 1), channel, receiverIncarnation, targetId);
        Target target = cloud ? new SdkTarget(receiverIncarnation, targetId, "cluster-binding-a")
                : new ReceiverTarget(receiver, 1, email(), null);
        Route route = cloud ? new SystemRoute("route-a", "managed-route", 1) : new ConfiguredRoute(routeId, 1);
        var policy = Policy.capture(AlertRetentionPolicy.defaults(cloud ? Deployment.CLOUD_MANAGED : Deployment.ON_PREM));
        var frozen = new Frozen(new Business(key), new AlertChannelPolicy.Capability(channel, cloud ? 2 : 1,
                cloud ? AlertChannelPolicy.Family.CLOUD_SDK : AlertChannelPolicy.Family.OP_EXTERNAL), route, target,
                new Content(cloud ? "application/json" : "message/rfc822", payload.sha256(), template("fault"), false),
                List.of(new Member(evaluation, 1, id(AlertConfiguration.Kind.RULE, "rule-a"), 0)), retry, policy, AT, AT.plusSeconds(86400));
        return initial(frozen, payload, null);
    }
    static AlertIntent.Record test() {
        var key = new AlertTestCommand.Key(SCOPE, "actor-a", new AlertTestCommand.ClientId(Instant.ofEpochSecond(AT.getEpochSecond()), "1".repeat(32)));
        var parameters = new AlertTestCommand.Parameters("email", new AlertTestCommand.Candidate("candidate-a", "a".repeat(64)), null,
                "a@example.test", "candidate-binding-a", null, PAYLOAD.sha256());
        var accepted = new AlertTestCommand.Accepted(key, parameters, AT, AT.plusSeconds(300));
        var policy = Policy.capture(AlertRetentionPolicy.defaults(Deployment.ON_PREM));
        return initial(new Frozen(new Test(accepted), new AlertChannelPolicy.Capability("email", 1, AlertChannelPolicy.Family.OP_EXTERNAL),
                null, new CandidateTarget(email(), null, 0, null), new Content("message/rfc822", PAYLOAD.sha256(), template("channel-test"), false),
                List.of(), ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5), Duration.ofSeconds(60)), policy, AT, accepted.deadline()), PAYLOAD, null);
    }
    static AlertIntent.Record initial(Frozen frozen, Payload payload, EncryptedCandidate candidate) {
        DeliveryRetention.Kind kind = frozen.identity() instanceof Test ? DeliveryRetention.Kind.CHANNEL_TEST
                : frozen.retention().deployment() == Deployment.CLOUD_MANAGED ? DeliveryRetention.Kind.CLOUD_SDK : DeliveryRetention.Kind.OP_EXTERNAL;
        var retained = Retained.capture(DeliveryRetention.begin(frozen.retention().value(), kind, frozen.createdAt(), frozen.deadline()));
        return new AlertIntent.Record(frozen, new State(AlertOwnership.unclaimed(), 0, false, null, frozen.createdAt(), 0, List.of(),
                Outcome.NOT_ATTEMPTED, retained, payload, candidate, null, null, null));
    }
    static AlertIntent.Record pending(AlertIntent.Record original, String ownerId, String attemptId) {
        var frozen = original.frozen();
        var value = DeliveryAttempt.grant(frozen.identity().scope(), frozen.identity().deliveryId(), attemptId, 1, 1, ownerId, frozen.retry(),
                frozen.createdAt(), frozen.deadline(), frozen.createdAt()).orElseThrow();
        var s = original.state();
        boolean exhausted = frozen.retry().maximumTotalAttempts() == 1;
        return new AlertIntent.Record(frozen, new State(new AlertOwnership(1, ownerId, frozen.createdAt().plusSeconds(120)),
                0, exhausted, exhausted ? StopReason.EXHAUSTED : null, null, 1, List.of(Attempt.pending(value)), Outcome.UNKNOWN,
                s.retained(), s.payload(), s.candidate(), null, null, null, exhausted ? value.permit().grantedAt() : null, null));
    }
    static AlertIntent.Record accepted(AlertIntent.Record pending, Instant recordedAt) {
        var frozen = pending.frozen(); Attempt previous = pending.state().attempts().getFirst();
        var result = previous.value().receipt(previous.permit(), Outcome.ACCEPTED, recordedAt).attempt();
        boolean sdk = frozen.retention().deployment() == Deployment.CLOUD_MANAGED;
        var stored = new Attempt(result.permit(), Outcome.ACCEPTED, result.endedAt(), recordedAt,
                new Protocol(Phase.FINAL_RESPONSE, Action.STOP, sdk ? 200 : 250, sdk ? "sdk-event-center" : "smtp", null),
                new Receipt(sdk ? "sdk-event-center" : "channel-protocol", sdk ? "event-a" : "provider-a", "request-a"));
        var s = pending.state();
        Instant stopAt = s.permitsStopped() ? s.permitsStoppedAt() : recordedAt;
        Instant fixedStop = stopAt.isAfter(result.endedAt()) ? stopAt : result.endedAt();
        var retained = s.retained().value(frozen);
        if (fixedStop.isBefore(recordedAt)) { retained = retained.stop(fixedStop, result.permit().deadline()); }
        retained = retained.accept(recordedAt, result.permit().deadline(), recordedAt).orElseThrow();
        if (retained.stoppedAt() == null) { retained = retained.stop(fixedStop, result.permit().deadline()); }
        return new AlertIntent.Record(frozen, new State(s.owner(), s.manualCycles(), true, s.permitsStopped() ? s.stopReason() : StopReason.ACCEPTED,
                null, 1, List.of(stored), Outcome.ACCEPTED, Retained.capture(retained), s.payload(), s.candidate(),
                s.payloadPrunedAt(), s.candidatePrunedAt(), null, stopAt, result.endedAt(), s.cycleStarts()));
    }
    static AlertReceiver.Email email() {
        return new AlertReceiver.Email("smtp.example.test", 465, AlertReceiver.Encryption.IMPLICIT_TLS, AlertReceiver.Authentication.NONE,
                null, "alerts@example.test", List.of("a@example.test"), AlertReceiver.Trust.SYSTEM);
    }
    private static Template template(String id) { return new Template(id, 1, 1, "en", MessageSettings.empty()); }
    private static AlertConfiguration.Identity id(AlertConfiguration.Kind kind, String id) { return new AlertConfiguration.Identity(kind, SCOPE, id, UUID); }
}
