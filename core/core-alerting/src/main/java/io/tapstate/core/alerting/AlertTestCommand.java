package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** Pure admission candidates. The host checks current permissions and atomically commits the intent and quota before returning success. */
public final class AlertTestCommand {
    private AlertTestCommand() { }
    private static final Pattern CLIENT_ID = Pattern.compile("act1\\.(0|[1-9][0-9]{0,11})\\.([0-9a-f]{32})");
    public static final Duration ADMISSION_AGE = Duration.ofMinutes(5);
    public static final Duration FUTURE_SKEW = Duration.ofSeconds(30);
    public record ClientId(Instant createdAt, String nonce) {
        public ClientId {
            AlertValidation.require(createdAt != null && createdAt.getNano() == 0 && createdAt.getEpochSecond() >= 0
                    && createdAt.getEpochSecond() <= 999999999999L, "clientCommandId", "invalid-created-second");
            AlertValidation.require(nonce != null && nonce.matches("[0-9a-f]{32}"), "clientCommandId", "invalid-nonce");
        }
        public String value() { return "act1." + createdAt.getEpochSecond() + "." + nonce; }
        public static ClientId parse(String value) {
            AlertValidation.require(value != null && value.length() <= 50, "clientCommandId", "invalid-format");
            var match = CLIENT_ID.matcher(value);
            AlertValidation.require(match.matches(), "clientCommandId", "invalid-format");
            return new ClientId(Instant.ofEpochSecond(Long.parseLong(match.group(1))), match.group(2));
        }
        public Instant admissionUntil() { return createdAt.plus(ADMISSION_AGE); }
    }
    public record Key(AlarmKey.Scope scope, String actorPrincipal, ClientId clientId) {
        public Key {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(clientId, "clientId");
            actorPrincipal = AlertValidation.text(actorPrincipal, "actorPrincipal", 256);
        }
        public String testId() {
            return "test-" + DeliveryKey.fingerprint(List.of("channel-test", scope.authorityId(), scope.clusterId(), actorPrincipal, clientId.value()));
        }
    }
    public record Route(String incarnation, long revision, long messageSettingsRevision) {
        public Route {
            incarnation = AlertValidation.text(incarnation, "routeIncarnation", 256);
            AlertValidation.require(revision > 0 && messageSettingsRevision > 0, "routeRevision", "must-be-positive");
        }
    }
    /** Only saved receivers carry a CAS baseline; a new candidate has no invented receiver identity. */
    public record ExistingReceiver(String incarnation, long expectedRevision) {
        public ExistingReceiver {
            incarnation = AlertValidation.text(incarnation, "receiverIncarnation", 256);
            AlertValidation.require(expectedRevision > 0, "receiverRevision", "must-be-positive");
        }
    }
    /** The host validates the full candidate and computes its keyed digest; these values are not authorization. */
    public record Candidate(String id, String digest) {
        public Candidate {
            id = AlertValidation.text(id, "candidateId", 256);
            AlertValidation.require(digest != null && digest.matches("[0-9a-f]{64}"), "candidateDigest", "invalid-digest");
        }
    }
    public record Parameters(String channelType, Candidate candidate, ExistingReceiver receiver,
            String targetId, String bindingId, Route route, String payloadSha256) {
        public Parameters {
            channelType = AlertValidation.text(channelType, "channelType", 64);
            AlertValidation.require(channelType.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), "channelType", "invalid-id");
            AlertValidation.require(candidate != null, "candidate", "required");
            targetId = AlertValidation.text(targetId, "targetId", 256);
            bindingId = AlertValidation.text(bindingId, "bindingId", 256);
            AlertValidation.require(payloadSha256 != null && payloadSha256.matches("[0-9a-f]{64}"), "payloadSha256", "invalid-digest");
        }
    }
    public record Accepted(Key key, Parameters parameters, Instant acceptedAt, Instant deadline) {
        public Accepted {
            Objects.requireNonNull(key, "key"); Objects.requireNonNull(parameters, "parameters");
            AlertValidation.require(acceptedAt != null && !acceptedAt.isBefore(key.clientId().createdAt().minus(FUTURE_SKEW))
                    && acceptedAt.isBefore(key.clientId().admissionUntil()), "acceptedAt", "outside-command-admission-window");
            AlertValidation.require(deadline != null && deadline.equals(acceptedAt.plus(ADMISSION_AGE)), "deadline", "must-match-original-test-age");
        }
        public String testId() { return key.testId(); }
    }
    public record Admission(Accepted command, boolean replay) {
        public Admission { Objects.requireNonNull(command, "command"); }
    }
    /** Existing records are checked first, even after first-admission expiry; replay never creates a candidate or extends its clock. */
    public static Admission admit(AlertChannelPolicy.Deployment trustedDeployment, AlertChannelPolicy.Capability channel,
            Key key, Parameters parameters, Accepted existing, Instant now) {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(parameters, "parameters"); Objects.requireNonNull(now, "now");
        AlertValidation.require(AlertChannelPolicy.maySendTest(trustedDeployment, channel)
                && channel.typeId().equals(parameters.channelType()), "channelTest", "not-permitted-for-deployment-or-channel");
        if (existing != null) {
            if (!existing.key().equals(key)) { throw new IllegalStateException("command lookup returned a foreign identity"); }
            if (!existing.parameters().equals(parameters)) { throw new TapstateException(AlertError.COMMAND_CONFLICT, Map.of(), null); }
            return new Admission(existing, true);
        }
        if (!now.isBefore(key.clientId().admissionUntil())) { throw new TapstateException(AlertError.COMMAND_EXPIRED, Map.of(), null); }
        if (now.isBefore(key.clientId().createdAt().minus(FUTURE_SKEW))) { throw new TapstateException(AlertError.COMMAND_NOT_YET_VALID, Map.of(), null); }
        return new Admission(new Accepted(key, parameters, now, now.plus(ADMISSION_AGE)), false);
    }
}
