package io.tapstate.core.alerting;

import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Typed durable facts only. Planning, permission, result and cleanup candidates still require one protected host transaction. */
public final class AlertIntent {
    public static final int FORMAT_VERSION = 1;
    public static final int MAX_BYTES = 512 * 1024;
    public static final int MAX_ATTEMPT_BYTES = 4 * 1024;
    public static final int MAX_DIAGNOSTIC_BYTES = 2 * 1024;
    private AlertIntent() { }

    public sealed interface Identity permits Business, Test {
        AlarmKey.Scope scope();
        String deliveryId();
        String channelType();
        String targetId();
    }
    public record Business(DeliveryKey key) implements Identity {
        public Business { Objects.requireNonNull(key, "key"); }
        @Override public AlarmKey.Scope scope() { return key.notification().scope(); }
        @Override public String channelType() { return key.channelType(); }
        @Override public String targetId() { return key.targetId(); }
        @Override public String deliveryId() {
            var n = key.notification();
            return "delivery-" + DeliveryKey.fingerprint(List.of("business-intent-v1", scope().authorityId(), scope().clusterId(),
                    n.subjectId(), n.subjectIncarnation(), n.routeId(), n.routeIncarnation(), n.kind().name(), Long.toString(n.round()),
                    key.channelType(), key.receiverIncarnation(), key.targetId()));
        }
    }
    public record Test(AlertTestCommand.Accepted command) implements Identity {
        public Test { Objects.requireNonNull(command, "command"); }
        @Override public AlarmKey.Scope scope() { return command.key().scope(); }
        @Override public String deliveryId() { return command.testId(); }
        @Override public String channelType() { return command.parameters().channelType(); }
        @Override public String targetId() { return command.parameters().targetId(); }
    }
    public sealed interface Route permits ConfiguredRoute, SystemRoute { String id(); String incarnation(); }
    public record ConfiguredRoute(AlertConfiguration.Identity identity, long bindingRevision) implements Route {
        public ConfiguredRoute {
            Objects.requireNonNull(identity, "identity");
            AlertValidation.require(identity.kind() == AlertConfiguration.Kind.SUBSCRIPTION && bindingRevision > 0,
                    "routeBinding", "invalid-identity-or-revision");
        }
        @Override public String id() { return identity.id(); }
        @Override public String incarnation() { return identity.incarnation(); }
    }
    /** A trusted managed-Cluster profile binding, not an invented local user or revocation version. */
    public record SystemRoute(String id, String incarnation, int profileVersion) implements Route {
        public SystemRoute {
            id = AlertValidation.text(id, "systemRoute", 256); incarnation = AlertValidation.text(incarnation, "systemRouteIncarnation", 256);
            AlertValidation.require(profileVersion > 0, "systemRouteProfile", "must-be-positive");
        }
    }
    public sealed interface Target permits ReceiverTarget, SdkTarget, CandidateTarget { }
    public record ReceiverTarget(AlertConfiguration.Identity identity, long bindingRevision,
            AlertReceiver.Transport transport, AlertReceiver.CredentialReference credentials) implements Target {
        public ReceiverTarget {
            Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(transport, "transport");
            AlertValidation.require(identity.kind() == AlertConfiguration.Kind.RECEIVER && bindingRevision > 0,
                    "receiverBinding", "invalid-identity-or-revision");
            AlertValidation.require(needsPrivate(transport) == (credentials != null), "receiverCredentials", "private-reference-required-or-unexpected");
            if (credentials != null) {
                AlertValidation.require(credentials.receiver().equals(identity)
                        && credentials.authenticationIdentity().equals(AlertReceiver.authenticationIdentity(transport)),
                        "receiverCredentials", "foreign-reference");
            }
        }
        public static ReceiverTarget capture(AlertReceiver value) {
            AlertValidation.require(value.header().active() && value.enabled(), "receiver", "not-eligible");
            return new ReceiverTarget(value.header().identity(), value.bindingRevision(), value.transport(), value.credentials());
        }
    }
    public record SdkTarget(String incarnation, String logicalTargetId, String clusterBinding) implements Target {
        public SdkTarget {
            incarnation = AlertValidation.text(incarnation, "sdkTargetIncarnation", 256);
            logicalTargetId = AlertValidation.text(logicalTargetId, "sdkLogicalTarget", 256);
            clusterBinding = AlertValidation.text(clusterBinding, "sdkClusterBinding", 256);
        }
    }
    public record CandidateTarget(AlertReceiver.Transport transport, AlertConfiguration.Identity existingReceiver,
            long receiverBindingRevision, String encryptedMaterialSha256) implements Target {
        public CandidateTarget {
            Objects.requireNonNull(transport, "transport");
            AlertValidation.require(existingReceiver == null ? receiverBindingRevision == 0
                    : existingReceiver.kind() == AlertConfiguration.Kind.RECEIVER && receiverBindingRevision > 0,
                    "testReceiver", "invalid-original-binding");
            AlertValidation.require(needsPrivate(transport) == (encryptedMaterialSha256 != null), "candidateMaterial", "required-or-unexpected");
            if (encryptedMaterialSha256 != null) { digest(encryptedMaterialSha256, "candidateMaterial"); }
        }
    }
    public record EncryptedCandidate(String keyId, String ciphertext) {
        public EncryptedCandidate {
            digest(keyId, "candidateKey");
            AlertValidation.require(ciphertext != null && !ciphertext.isEmpty() && ciphertext.getBytes(StandardCharsets.UTF_8).length <= 16 * 1024,
                    "candidateCiphertext", "outside-original-budget");
            AlertValidation.text(ciphertext, "candidateCiphertext", 16 * 1024);
        }
        public String sha256() { return hash(ciphertext.getBytes(StandardCharsets.UTF_8)); }
        @Override public String toString() { return "EncryptedCandidate[redacted]"; }
    }
    public record Template(String id, int version, long settingsRevision, String language, MessageSettings settings) {
        public Template {
            id = AlertValidation.text(id, "templateId", 128); language = AlertValidation.text(language, "language", 35);
            AlertValidation.require(version > 0 && settingsRevision > 0, "templateVersion", "must-be-positive");
            Objects.requireNonNull(settings, "settings");
        }
    }
    public record Content(String contentType, String payloadSha256, Template template, boolean renderingFailed) {
        public Content {
            AlertValidation.require("application/json".equals(contentType) || "message/rfc822".equals(contentType), "contentType", "unsupported");
            Objects.requireNonNull(template, "template");
            if (renderingFailed) { AlertValidation.require(payloadSha256 == null, "payloadSha256", "no-rendered-payload"); }
            else { digest(payloadSha256, "payloadSha256"); }
        }
    }
    /** Exact bytes are privately owned and value-comparable; no mutable array enters record equality. */
    public static final class Payload {
        private final byte[] bytes;
        public Payload(byte[] bytes) {
            AlertValidation.require(bytes != null && bytes.length > 0 && bytes.length <= 256 * 1024, "payload", "outside-payload-budget");
            this.bytes = bytes.clone();
        }
        public byte[] bytes() { return bytes.clone(); }
        public int size() { return bytes.length; }
        public String sha256() { return hash(bytes); }
        @Override public boolean equals(Object other) { return other instanceof Payload payload && Arrays.equals(bytes, payload.bytes); }
        @Override public int hashCode() { return Arrays.hashCode(bytes); }
        @Override public String toString() { return "Payload[redacted]"; }
    }
    public record Policy(Deployment deployment, Map<AlertRetentionPolicy.Setting, Duration> values) {
        public Policy {
            values = Map.copyOf(values); AlertRetentionPolicy.configure(deployment, values);
        }
        public static Policy capture(AlertRetentionPolicy policy) { return new Policy(policy.deployment(), policy.values()); }
        public AlertRetentionPolicy value() { return AlertRetentionPolicy.configure(deployment, values); }
    }
    public record Member(AlertRule evaluation, long episode, AlertConfiguration.Identity ruleIdentity, long sourceBindingRevision) {
        public Member {
            Objects.requireNonNull(evaluation, "evaluation"); Objects.requireNonNull(ruleIdentity, "ruleIdentity");
            AlertValidation.require(ruleIdentity.kind() == AlertConfiguration.Kind.RULE
                    && ruleIdentity.scope().equals(evaluation.key().scope()) && episode >= 0, "notificationMember", "invalid-rule-or-episode");
            AlertValidation.require(evaluation.key().resource().kind().equals("source") ? sourceBindingRevision > 0 : sourceBindingRevision == 0,
                    "sourceBinding", "invalid-resource-binding");
        }
        public FrozenGroupPolicy.Member episodeMember() { return new FrozenGroupPolicy.Member(evaluation.key(), episode); }
    }
    public record Frozen(Identity identity, AlertChannelPolicy.Capability channel, Route route, Target target,
            Content content, List<Member> members, ChannelRetryPolicy retry, Policy retention, Instant createdAt, Instant deadline, AlertTestAuthority testAuthority) {
        public Frozen(Identity identity, AlertChannelPolicy.Capability channel, Route route, Target target, Content content, List<Member> members, ChannelRetryPolicy retry, Policy retention, Instant createdAt, Instant deadline) {
            this(identity,channel,route,target,content,members,retry,retention,createdAt,deadline,null);
        }
        public Frozen {
            Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(channel, "channel"); Objects.requireNonNull(target, "target");
            Objects.requireNonNull(content, "content"); Objects.requireNonNull(retry, "retry"); Objects.requireNonNull(retention, "retention");
            members = List.copyOf(members);
            AlertValidation.require(channel.typeId().equals(identity.channelType()), "channel", "foreign-capability");
            AlertValidation.require(content.contentType().equals(identity.channelType().equals("email") ? "message/rfc822" : "application/json"),
                    "contentType", "does-not-match-frozen-channel");
            AlertValidation.require(retry.attemptTimeout().compareTo(Duration.ofSeconds(identity.channelType().equals("email") ? 60 : 30)) <= 0,
                    "attemptTimeout", "outside-channel-attempt-bound");
            AlertChannelPolicy.requireDeliverable(retention.deployment(), channel);
            DeliveryRetention.begin(retention.value(), kind(identity, retention.deployment()), createdAt, deadline);
            AlertValidation.require(!deadline.isAfter(DeliveryRetention.add(createdAt, retry.maximumAge())), "deadline", "outside-original-retry-age");
            AlertValidation.require(testAuthority == null || identity instanceof Test test && test.command().key().actorPrincipal().equals(testAuthority.principalKey()), "testAuthority", "foreign-original-actor");
            if (identity instanceof Business business) {
                AlertValidation.require(route != null && route.id().equals(business.key().notification().routeId())
                        && route.incarnation().equals(business.key().notification().routeIncarnation()), "route", "foreign-frozen-route");
                AlertValidation.require(!members.isEmpty() && members.size() <= FrozenGroupPolicy.MAX_MEMBERS
                        && members.stream().map(value -> value.evaluation().key()).distinct().count() == members.size()
                        && members.stream().allMatch(value -> value.evaluation().key().scope().equals(identity.scope())
                                && (business.key().notification().kind() == DeliveryKey.Kind.EVENT ? value.episode() == 0 : value.episode() > 0)),
                        "members", "invalid-frozen-members");
            } else {
                var test = ((Test) identity).command();
                AlertValidation.require(retention.deployment() == Deployment.ON_PREM && target instanceof CandidateTarget && members.isEmpty()
                        && createdAt.equals(test.acceptedAt()) && deadline.equals(test.deadline())
                        && Objects.equals(content.payloadSha256(), test.parameters().payloadSha256())
                        && retry.maximumTotalAttempts() == 1, "channelTest", "foreign-command-or-budget");
            }
            if (route instanceof ConfiguredRoute configured) {
                AlertValidation.require(configured.identity().scope().equals(identity.scope()), "route", "foreign-scope");
            }
            if (retention.deployment() == Deployment.CLOUD_MANAGED) {
                AlertValidation.require(identity instanceof Business && route instanceof SystemRoute && target instanceof SdkTarget
                        && identity.channelType().equals("cloud-sdk") && retry.maximumTotalAttempts() == 1,
                        "cloudIntent", "not-a-single-sdk-event");
            } else {
                AlertValidation.require(!(route instanceof SystemRoute)
                        && (identity instanceof Business ? target instanceof ReceiverTarget && route instanceof ConfiguredRoute
                        : target instanceof CandidateTarget), "target", "invalid-on-prem-purpose-or-target");
            }
            if (target instanceof ReceiverTarget receiver) {
                AlertValidation.require(identity instanceof Business business && receiver.identity().scope().equals(identity.scope())
                        && receiver.identity().incarnation().equals(business.key().receiverIncarnation())
                        && receiver.transport().channelType().equals(identity.channelType())
                        && targetMatches(receiver.transport(), identity.targetId()), "target", "foreign-receiver");
            } else if (target instanceof SdkTarget sdk) {
                AlertValidation.require(identity instanceof Business business && sdk.incarnation().equals(business.key().receiverIncarnation())
                        && sdk.logicalTargetId().equals(identity.targetId()), "target", "foreign-sdk-target");
            } else {
                var candidate = (CandidateTarget) target;
                var test = ((Test) identity).command();
                AlertValidation.require(candidate.transport().channelType().equals(identity.channelType())
                        && targetMatches(candidate.transport(), identity.targetId())
                        && (candidate.existingReceiver() == null ? test.parameters().receiver() == null
                        : candidate.existingReceiver().scope().equals(identity.scope()) && test.parameters().receiver() != null
                                && candidate.existingReceiver().incarnation().equals(test.parameters().receiver().incarnation())),
                        "candidate", "foreign-command-target");
            }
        }
    }
    public enum StopReason { ACCEPTED, RENDER_FAILED, REVOKED, DISABLED, EXPIRED, EXHAUSTED, AGE_LIMIT, NON_RETRYABLE, CANCELLED }
    public enum Action { STOP, RETRY, PAUSE }
    public enum Phase { LOCAL, CONNECT, TLS, AUTHENTICATION, ENVELOPE, BODY, FINAL_RESPONSE, DEADLINE, CAPACITY }
    /** No server prose or raw response is retained. */
    public record Protocol(Phase phase, Action action, int status, String implementation, Instant retryAfter) {
        public Protocol {
            Objects.requireNonNull(phase, "phase"); Objects.requireNonNull(action, "action");
            AlertValidation.require(status >= 0 && status <= 999 && implementation != null
                    && implementation.matches("[a-z][a-z0-9-]{0,63}"), "protocol", "invalid-bounded-diagnostic");
        }
    }
    public record Receipt(String source, String remoteId, String requestId) {
        public Receipt {
            AlertValidation.require("channel-protocol".equals(source) || "sdk-event-center".equals(source), "receiptSource", "unsupported");
            remoteId = AlertValidation.text(remoteId, "remoteId", 256);
            if (requestId != null) { requestId = AlertValidation.text(requestId, "requestId", 256); }
        }
    }
    public record Attempt(DeliveryAttempt.Permit permit, Outcome outcome, Instant endedAt, Instant acceptedRecordedAt,
            Protocol protocol, Receipt receipt) {
        public Attempt {
            DeliveryAttempt.restore(permit, outcome, endedAt);
            AlertValidation.require(outcome == Outcome.ACCEPTED ? acceptedRecordedAt != null && !acceptedRecordedAt.isBefore(permit.grantedAt())
                    : acceptedRecordedAt == null && receipt == null, "attemptReceipt", "invalid-accepted-evidence");
            AlertValidation.require(endedAt != null || protocol == null && receipt == null, "pendingAttempt", "cannot-have-final-result");
            AlertValidation.require(outcome != Outcome.ACCEPTED || protocol == null || protocol.action() == Action.STOP,
                    "attemptResult", "accepted-cannot-retry");
        }
        public static Attempt pending(DeliveryAttempt value) {
            AlertValidation.require(value.pending(), "attempt", "not-pending");
            return new Attempt(value.permit(), value.outcome(), null, null, null, null);
        }
        public DeliveryAttempt value() { return DeliveryAttempt.restore(permit, outcome, endedAt); }
    }
    public record Retained(Instant payloadUntil, Instant resultUntil, Instant acceptedAt, Instant stoppedAt) {
        public static Retained capture(DeliveryRetention value) {
            return new Retained(value.payloadUntil(), value.resultUntil(), value.acceptedAt(), value.stoppedAt());
        }
        public DeliveryRetention value(Frozen frozen) {
            return DeliveryRetention.restore(frozen.retention().value(), kind(frozen.identity(), frozen.retention().deployment()),
                    frozen.createdAt(), frozen.deadline(), payloadUntil, resultUntil, acceptedAt, stoppedAt);
        }
    }
    public record State(AlertOwnership owner, int manualCycles, boolean permitsStopped, StopReason stopReason,
            Instant nextAttemptAt, int attemptsConsumed, List<Attempt> attempts, Outcome summary,
            Retained retained, Payload payload, EncryptedCandidate candidate, Instant payloadPrunedAt,
            Instant candidatePrunedAt, Instant detailsPrunedAt, Instant permitsStoppedAt, Instant lastAttemptEnd, List<Integer> cycleStarts,
            Receipt acceptedReceipt) {
        /** Convenience for new candidates whose immediate stop is already fully represented by retention. */
        public State(AlertOwnership owner, int manualCycles, boolean permitsStopped, StopReason stopReason,
                Instant nextAttemptAt, int attemptsConsumed, List<Attempt> attempts, Outcome summary,
                Retained retained, Payload payload, EncryptedCandidate candidate, Instant payloadPrunedAt,
                Instant candidatePrunedAt, Instant detailsPrunedAt) {
            this(owner, manualCycles, permitsStopped, stopReason, nextAttemptAt, attemptsConsumed, attempts, summary,
                    retained, payload, candidate, payloadPrunedAt, candidatePrunedAt, detailsPrunedAt,
                    !permitsStopped ? null : stopReason == StopReason.ACCEPTED ? retained.acceptedAt() : retained.stoppedAt(),
                    attempts.stream().map(Attempt::endedAt).filter(Objects::nonNull).max(Instant::compareTo).orElse(null), initialCycles(manualCycles),
                    firstReceipt(attempts, retained));
        }
        public State(AlertOwnership owner, int manualCycles, boolean permitsStopped, StopReason stopReason,
                Instant nextAttemptAt, int attemptsConsumed, List<Attempt> attempts, Outcome summary,
                Retained retained, Payload payload, EncryptedCandidate candidate, Instant payloadPrunedAt,
                Instant candidatePrunedAt, Instant detailsPrunedAt, Instant permitsStoppedAt, Instant lastAttemptEnd) {
            this(owner, manualCycles, permitsStopped, stopReason, nextAttemptAt, attemptsConsumed, attempts, summary, retained,
                    payload, candidate, payloadPrunedAt, candidatePrunedAt, detailsPrunedAt, permitsStoppedAt, lastAttemptEnd, initialCycles(manualCycles),
                    firstReceipt(attempts, retained));
        }
        public State(AlertOwnership owner, int manualCycles, boolean permitsStopped, StopReason stopReason,
                Instant nextAttemptAt, int attemptsConsumed, List<Attempt> attempts, Outcome summary,
                Retained retained, Payload payload, EncryptedCandidate candidate, Instant payloadPrunedAt,
                Instant candidatePrunedAt, Instant detailsPrunedAt, Instant permitsStoppedAt, Instant lastAttemptEnd, List<Integer> cycleStarts) {
            this(owner, manualCycles, permitsStopped, stopReason, nextAttemptAt, attemptsConsumed, attempts, summary, retained,
                    payload, candidate, payloadPrunedAt, candidatePrunedAt, detailsPrunedAt, permitsStoppedAt, lastAttemptEnd, cycleStarts, firstReceipt(attempts, retained));
        }
        public State {
            Objects.requireNonNull(owner, "owner"); Objects.requireNonNull(summary, "summary"); Objects.requireNonNull(retained, "retained");
            attempts = List.copyOf(attempts);
            cycleStarts = List.copyOf(cycleStarts);
            AlertValidation.require(manualCycles >= 0 && manualCycles <= 2 && attemptsConsumed >= 0 && attemptsConsumed <= 30,
                    "intentBudget", "outside-original-budget");
            AlertValidation.require(cycleStarts.size() == manualCycles + 1 && cycleStarts.getFirst() == 0,
                    "cycles", "missing-original-cycle-starts");
            for (int index = 0; index < cycleStarts.size(); index++) {
                Integer offset = cycleStarts.get(index);
                AlertValidation.require(offset != null && offset >= 0 && offset <= attemptsConsumed
                        && (index == 0 || offset > cycleStarts.get(index - 1)), "cycles", "invalid-cycle-start");
            }
            AlertValidation.require(permitsStopped == (stopReason != null), "stopReason", "missing-or-unexpected");
            AlertValidation.require(permitsStopped == (permitsStoppedAt != null), "permitsStoppedAt", "missing-or-unexpected");
            AlertValidation.require(detailsPrunedAt == null ? attempts.size() == attemptsConsumed : attempts.isEmpty(),
                    "attemptDetails", "lost-budget-or-details");
        }
        private static List<Integer> initialCycles(int manualCycles) {
            AlertValidation.require(manualCycles == 0, "cycles", "explicit-starts-required-for-manual-cycles");
            return List.of(0);
        }
        private static Receipt firstReceipt(List<Attempt> attempts, Retained retained) {
            return attempts.stream().filter(attempt -> attempt.acceptedRecordedAt() != null
                    && attempt.acceptedRecordedAt().equals(retained.acceptedAt()) && attempt.receipt() != null)
                    .map(attempt -> new Receipt(attempt.receipt().source(), attempt.receipt().remoteId(), null)).findFirst().orElse(null);
        }
        public int currentCycleConsumed() { return attemptsConsumed - cycleStarts.getLast(); }
    }
    public record Record(Frozen frozen, State state) {
        public Record {
            Objects.requireNonNull(frozen, "frozen"); Objects.requireNonNull(state, "state");
            DeliveryRetention retained = state.retained().value(frozen);
            AlertValidation.require(state.permitsStoppedAt() == null || !state.permitsStoppedAt().isBefore(frozen.createdAt())
                    && !state.permitsStoppedAt().isAfter(frozen.deadline()), "permitsStoppedAt", "outside-original-lifetime");
            AlertValidation.require(state.lastAttemptEnd() == null || !state.lastAttemptEnd().isBefore(frozen.createdAt())
                    && !state.lastAttemptEnd().isAfter(frozen.deadline()), "lastAttemptEnd", "outside-original-lifetime");
            AlertValidation.require(state.manualCycles() <= frozen.retry().maximumManualCycles()
                    && state.attemptsConsumed() <= frozen.retry().attemptsPerCycle() * (1 + state.manualCycles()),
                    "attemptBudget", "exceeds-opened-cycles");
            for (int index = 0; index < state.cycleStarts().size(); index++) {
                int end = index + 1 < state.cycleStarts().size() ? state.cycleStarts().get(index + 1) : state.attemptsConsumed();
                AlertValidation.require(end - state.cycleStarts().get(index) <= frozen.retry().attemptsPerCycle(), "cycles", "cycle-budget-exceeded");
            }
            boolean finalCycleConsumed = state.manualCycles() == frozen.retry().maximumManualCycles()
                    && state.currentCycleConsumed() == frozen.retry().attemptsPerCycle();
            if (finalCycleConsumed) {
                AlertValidation.require(state.permitsStopped() && state.stopReason() == StopReason.EXHAUSTED,
                        "permitsStopped", "all-original-cycles-consumed");
                if (state.detailsPrunedAt() == null) {
                    AlertValidation.require(state.permitsStoppedAt().equals(state.attempts().getLast().permit().grantedAt()),
                            "permitsStoppedAt", "must-retain-final-budget-consumption-time");
                }
            }
            int pending = 0;
            for (int index = 0; index < state.attempts().size(); index++) {
                Attempt attempt = state.attempts().get(index); var permit = attempt.permit();
                AlertValidation.require(permit.scope().equals(frozen.identity().scope()) && permit.deliveryId().equals(frozen.identity().deliveryId())
                        && permit.ordinal() == index + 1 && permit.ownerGeneration() <= state.owner().generation()
                        && !permit.grantedAt().isBefore(frozen.createdAt())
                        && permit.grantedAt().isBefore(frozen.deadline())
                        && frozen.retry().attemptDeadline(permit.grantedAt(), frozen.deadline()).orElseThrow().equals(permit.deadline()),
                        "attemptPermit", "foreign-or-expanded-permit");
                if (index > 0) {
                    Attempt prior = state.attempts().get(index - 1);
                    AlertValidation.require(prior.endedAt() != null && !permit.grantedAt().isBefore(prior.endedAt())
                            && permit.ownerGeneration() >= prior.permit().ownerGeneration(), "attemptPermit", "overlapping-or-old-owner");
                }
                if (attempt.acceptedRecordedAt() != null) {
                    AlertValidation.require(attempt.acceptedRecordedAt().isBefore(retained.resultUntil()), "acceptedAt", "outside-original-result-window");
                }
                if (attempt.receipt() != null) {
                    AlertValidation.require(attempt.receipt().source().equals(frozen.retention().deployment() == Deployment.CLOUD_MANAGED
                            ? "sdk-event-center" : "channel-protocol"), "receiptSource", "foreign-evidence-source");
                }
                if (attempt.endedAt() == null) { pending++; }
                AlertValidation.require(retained.stoppedAt() == null || attempt.endedAt() != null
                        && !retained.stoppedAt().isBefore(attempt.endedAt()), "stoppedAt", "before-original-attempt-settlement");
                AlertValidation.require(!permit.deadline().isAfter(retained.resultUntil()), "resultWindow", "lost-permit-protection");
                if (attempt.outcome() == Outcome.ACCEPTED && frozen.retention().deployment() == Deployment.CLOUD_MANAGED) {
                    AlertValidation.require(attempt.receipt() != null && attempt.receipt().source().equals("sdk-event-center"),
                            "sdkReceipt", "missing-event-acceptance-evidence");
                }
            }
            AlertValidation.require(state.attempts().stream().map(value -> value.permit().attemptId()).distinct().count() == state.attempts().size(),
                    "attemptId", "duplicate-original-identity");
            AlertValidation.require(pending <= 1 && (retained.stoppedAt() == null || state.permitsStopped() && pending == 0),
                    "attempts", "overlapping-or-premature-stop");
            if (state.detailsPrunedAt() == null) {
                AlertValidation.require(state.summary() == DeliveryEvidence.aggregate(state.attempts().stream().map(Attempt::outcome).toList()),
                        "summary", "does-not-match-original-attempts");
                AlertValidation.require(retained.acceptedAt() == null || state.attempts().stream()
                        .anyMatch(attempt -> retained.acceptedAt().equals(attempt.acceptedRecordedAt())), "acceptedAt", "missing-original-receipt");
                AlertValidation.require(Objects.equals(state.lastAttemptEnd(), state.attempts().stream().map(Attempt::endedAt)
                        .filter(Objects::nonNull).max(Instant::compareTo).orElse(null)), "lastAttemptEnd", "does-not-match-original-attempts");
            } else {
                AlertValidation.require(retained.stoppedAt() != null && !state.detailsPrunedAt().isBefore(retained.resultUntil()),
                        "detailsPrunedAt", "result-window-still-protected");
                AlertValidation.require(state.attemptsConsumed() == 0 || state.lastAttemptEnd() != null,
                        "lastAttemptEnd", "missing-pruned-control-fact");
            }
            AlertValidation.require((state.summary() == Outcome.ACCEPTED) == (retained.acceptedAt() != null),
                    "acceptedAt", "does-not-match-accepted-evidence");
            AlertValidation.require(state.acceptedReceipt() == null || state.summary() == Outcome.ACCEPTED && state.acceptedReceipt().requestId() == null,
                    "acceptedReceipt", "invalid-retained-receipt");
            AlertValidation.require(state.acceptedReceipt() == null || state.acceptedReceipt().source().equals(
                    frozen.retention().deployment() == Deployment.CLOUD_MANAGED ? "sdk-event-center" : "channel-protocol"),
                    "acceptedReceipt", "foreign-retained-receipt-source");
            if (frozen.retention().deployment() == Deployment.CLOUD_MANAGED && state.summary() == Outcome.ACCEPTED) {
                AlertValidation.require(state.acceptedReceipt() != null && state.acceptedReceipt().source().equals("sdk-event-center"),
                        "acceptedReceipt", "missing-retained-sdk-event");
            }
            if (state.acceptedReceipt() != null && state.detailsPrunedAt() == null) {
                AlertValidation.require(state.attempts().stream().anyMatch(attempt -> attempt.receipt() != null
                        && Objects.equals(attempt.acceptedRecordedAt(), retained.acceptedAt())
                        && attempt.receipt().source().equals(state.acceptedReceipt().source())
                        && attempt.receipt().remoteId().equals(state.acceptedReceipt().remoteId())), "acceptedReceipt", "not-from-first-durable-accepted");
            }
            AlertValidation.require(state.summary() != Outcome.ACCEPTED || state.permitsStopped(),
                    "permitsStopped", "accepted-cannot-grant-again");
            AlertValidation.require(state.stopReason() != StopReason.ACCEPTED || state.summary() == Outcome.ACCEPTED,
                    "stopReason", "accepted-reason-without-evidence");
            AlertValidation.require(state.stopReason() != StopReason.EXHAUSTED || finalCycleConsumed,
                    "stopReason", "original-budget-not-exhausted");
            AlertValidation.require(state.stopReason() != StopReason.ACCEPTED || state.permitsStoppedAt().equals(retained.acceptedAt()),
                    "permitsStoppedAt", "does-not-match-first-accepted-stop");
            if (retained.stoppedAt() != null) {
                Instant fixedStop = state.lastAttemptEnd() == null || state.permitsStoppedAt().isAfter(state.lastAttemptEnd())
                        ? state.permitsStoppedAt() : state.lastAttemptEnd();
                AlertValidation.require(retained.stoppedAt().equals(fixedStop), "stoppedAt", "must-use-original-stop-and-settlement");
            }
            AlertValidation.require((state.stopReason() == StopReason.RENDER_FAILED) == frozen.content().renderingFailed(),
                    "renderFailure", "inconsistent-original-render-result");
            if (state.nextAttemptAt() != null) {
                AlertValidation.require(!state.permitsStopped() && pending == 0 && !state.nextAttemptAt().isBefore(frozen.createdAt())
                        && state.nextAttemptAt().isBefore(frozen.deadline()), "nextAttemptAt", "outside-open-original-window");
            }
            if (frozen.content().renderingFailed()) {
                AlertValidation.require(state.payload() == null && state.attemptsConsumed() == 0 && state.permitsStopped()
                        && state.stopReason() == StopReason.RENDER_FAILED && retained.stoppedAt() != null,
                        "renderFailure", "must-be-nonsendable-original-intent");
            } else if (state.payload() != null) {
                AlertValidation.require(state.payloadPrunedAt() == null && state.payload().sha256().equals(frozen.content().payloadSha256())
                        && (!frozen.identity().channelType().equals("email") ? state.payload().size() <= 64 * 1024 : true),
                        "payload", "changed-or-oversized-frozen-content");
            } else {
                AlertValidation.require(state.payloadPrunedAt() != null && retained.stoppedAt() != null && pending == 0
                        && !state.payloadPrunedAt().isBefore(retained.payloadUntil()), "payload", "missing-required-content");
            }
            if (frozen.target() instanceof CandidateTarget candidate && candidate.encryptedMaterialSha256() != null) {
                AlertValidation.require(state.candidate() != null ? state.candidatePrunedAt() == null
                        && state.candidate().sha256().equals(candidate.encryptedMaterialSha256())
                        : state.candidatePrunedAt() != null && pending == 0
                                && (retained.stoppedAt() != null && !state.candidatePrunedAt().isBefore(retained.stoppedAt())
                                || !state.candidatePrunedAt().isBefore(frozen.deadline())), "candidateMaterial", "missing-or-changed-snapshot");
            } else {
                AlertValidation.require(state.candidate() == null && state.candidatePrunedAt() == null, "candidateMaterial", "unexpected");
            }
        }
    }
    public static boolean needsPrivate(AlertReceiver.Transport transport) {
        return !(transport instanceof AlertReceiver.Email) || transport.authentication() != AlertReceiver.Authentication.NONE
                || transport.trust() == AlertReceiver.Trust.PRIVATE_CA;
    }
    public static boolean targetMatches(AlertReceiver.Transport transport, String targetId) {
        return transport instanceof AlertReceiver.Email email ? email.recipients().contains(targetId)
                : targetId.equals(AlertReceiver.httpsTargetId(transport));
    }
    private static DeliveryRetention.Kind kind(Identity identity, Deployment deployment) {
        return identity instanceof Test ? DeliveryRetention.Kind.CHANNEL_TEST
                : deployment == Deployment.CLOUD_MANAGED ? DeliveryRetention.Kind.CLOUD_SDK : DeliveryRetention.Kind.OP_EXTERNAL;
    }
    static void digest(String value, String field) {
        AlertValidation.require(value != null && value.matches("[a-f0-9]{64}"), field, "invalid-digest");
    }
    public static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException unavailable) { throw new IllegalStateException("SHA-256 is required", unavailable); }
    }
}
