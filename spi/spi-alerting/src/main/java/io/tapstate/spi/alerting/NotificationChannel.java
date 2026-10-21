package io.tapstate.spi.alerting;

import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.alerting.DeliveryKey;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** A single protocol attempt. A retry result never authorizes this adapter to send again. */
public interface NotificationChannel extends AutoCloseable {
    io.tapstate.core.alerting.AlertChannelPolicy.Capability capability();
    default String typeId() { return capability().typeId(); }
    default int capabilityVersion() { return capability().version(); }
    Result send(Request request) throws InterruptedException;
    @Override default void close() { }
    enum Action { STOP, RETRY, PAUSE }
    enum Phase { LOCAL, CONNECT, TLS, AUTHENTICATION, ENVELOPE, BODY, FINAL_RESPONSE, DEADLINE, CAPACITY }
    enum Purpose { BUSINESS, CHANNEL_TEST }
    /** Frozen routing data only; business and test-command deduplication keys stay in the host ledger. */
    record Destination(AlarmKey.Scope scope, String channelType, String receiverIncarnation, String targetId, String bindingId) {
        public Destination {
            Objects.requireNonNull(scope, "scope");
            validateIdentity(channelType, "channelType");
            validateIdentity(receiverIncarnation, "receiverIncarnation");
            validateIdentity(targetId, "targetId");
            validateIdentity(bindingId, "bindingId");
        }
        /** Shared by frozen adapter bindings so every representable destination can be matched. */
        public static void validateIdentity(String value, String field) {
            valid(value != null && !value.isBlank() && value.codePointCount(0, value.length()) <= 256
                    && value.codePoints().noneMatch(Character::isISOControl)
                    && java.nio.charset.StandardCharsets.UTF_8.newEncoder().canEncode(value), field);
        }
    }

    /** The host must persist the original permit first; a value object is not evidence of that write. */
    record Request(Destination destination, Purpose purpose, String deliveryId, String attemptId,
            Instant deadline, String contentType, byte[] payload) {
        public Request {
            Objects.requireNonNull(destination, "destination"); Objects.requireNonNull(purpose, "purpose");
            Objects.requireNonNull(deadline, "deadline");
            deliveryId = io.tapstate.core.alerting.DeliveryAttempt.Permit.transportId(deliveryId, "deliveryId");
            attemptId = io.tapstate.core.alerting.DeliveryAttempt.Permit.transportId(attemptId, "attemptId");
            valid(contentType != null && (contentType.equals("application/json") || contentType.equals("message/rfc822")), "contentType");
            valid(payload != null && payload.length > 0 && payload.length <= 256 * 1024, "payload");
            payload = payload.clone();
        }
        public static Request business(DeliveryKey key, String deliveryId, String attemptId, String bindingId,
                Instant deadline, String contentType, byte[] payload) {
            return new Request(new Destination(key.notification().scope(), key.channelType(), key.receiverIncarnation(), key.targetId(), bindingId),
                    Purpose.BUSINESS, deliveryId, attemptId, deadline, contentType, payload);
        }
        public static Request channelTest(Destination destination, String deliveryId, String attemptId,
                Instant deadline, String contentType, byte[] payload) {
            return new Request(destination, Purpose.CHANNEL_TEST, deliveryId, attemptId, deadline, contentType, payload);
        }
        @Override public byte[] payload() { return payload.clone(); }
    }
    private static void valid(boolean valid, String field) {
        if (!valid) { throw new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", field, "reason", "invalid-frozen-request"), null); }
    }
    /** Only bounded protocol metadata crosses the port; no raw response, credentials or server prose. */
    record Result(Outcome outcome, Action action, Instant retryAfter, Phase phase, int protocolStatus, String implementation) {
        public String diagnosticCode() {
            if (outcome == Outcome.ACCEPTED) { return null; }
            return (outcome == Outcome.UNKNOWN ? AlertError.CHANNEL_RESULT_UNKNOWN
                    : action == Action.PAUSE ? AlertError.CHANNEL_UNAVAILABLE : AlertError.CHANNEL_NOT_ACCEPTED).code();
        }
        public Map<String, Object> diagnosticParameters() {
            return Map.of("channel", implementation, "phase", phase.name().toLowerCase(java.util.Locale.ROOT));
        }
        public Result {
            Objects.requireNonNull(outcome, "outcome"); Objects.requireNonNull(action, "action"); Objects.requireNonNull(phase, "phase");
            if (outcome == Outcome.ACCEPTED && action != Action.STOP) { throw new IllegalArgumentException("accepted result cannot retry"); }
            if (protocolStatus < 0 || protocolStatus > 999) { throw new IllegalArgumentException("invalid protocol status"); }
            if (implementation == null || !implementation.matches("[a-z][a-z0-9-]{0,63}")) { throw new IllegalArgumentException("invalid implementation marker"); }
        }
    }
}
