package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Durable state metadata, separate from the pure evaluator and from notification outcomes. */
public final class AlertStateRecord {
    public static final int FORMAT_VERSION = 2;
    private AlertStateRecord() { }
    public enum DiagnosticStatus { CAPTURED, NOT_CAPTURED, NOT_APPLICABLE, PRUNED }
    public record Diagnostics(DiagnosticStatus status, AlarmDiagnostics.History history,
            AlarmDiagnostics.Occurrence first, AlarmDiagnostics.Occurrence latest, Instant prunedAt) {
        public Diagnostics {
            Objects.requireNonNull(status, "status");
            AlertValidation.require((first == null) == (latest == null), "diagnosticReferences", "incomplete-pair");
            AlertValidation.require(status == DiagnosticStatus.CAPTURED ? history != null && first != null
                    && first.equals(history.first().occurrence()) && latest.equals(history.latest().occurrence()) && prunedAt == null
                    : history == null && (status == DiagnosticStatus.PRUNED ? first != null && prunedAt != null : first == null && prunedAt == null),
                    "diagnostics", "inconsistent-status");
            if (first != null) {
                AlertValidation.require(first.bindingDigest().equals(latest.bindingDigest()) && first.episode() == latest.episode()
                        && first.sequence() <= latest.sequence() && (first.sequence() == latest.sequence() ? first.equals(latest)
                        : first.observedAt().isBefore(latest.observedAt())), "diagnosticReferences", "mixed-or-reordered");
            }
        }
        public static Diagnostics empty(boolean applicable) {
            return new Diagnostics(applicable ? DiagnosticStatus.NOT_CAPTURED : DiagnosticStatus.NOT_APPLICABLE, null, null, null, null);
        }
        public static Diagnostics captured(AlarmDiagnostics.History value) {
            return new Diagnostics(DiagnosticStatus.CAPTURED, value, value.first().occurrence(), value.latest().occurrence(), null);
        }
        public Diagnostics prune(Instant now) { return new Diagnostics(DiagnosticStatus.PRUNED, null, first, latest, now); }
    }
    public record Episode(AlertRule originalBinding, AlertSnapshot.Episode value, AlertIntent.Policy policy,
            Diagnostics diagnostics, boolean summariesHandedOff) {
        public Episode {
            Objects.requireNonNull(originalBinding, "originalBinding"); Objects.requireNonNull(value, "value");
            Objects.requireNonNull(policy, "policy"); Objects.requireNonNull(diagnostics, "diagnostics");
            if (diagnostics.first() != null) {
                AlertValidation.require(diagnostics.first().episode() == value.number()
                        && diagnostics.first().bindingDigest().equals(AlarmDiagnostics.bindingDigest(originalBinding))
                        && !diagnostics.first().observedAt().isBefore(value.firedAt())
                        && (value.endedAt() == null || !diagnostics.latest().observedAt().isAfter(value.endedAt())),
                        "diagnosticBinding", "foreign-episode-or-occurrence");
            }
            AlertValidation.require(diagnostics.prunedAt() == null || AlarmDiagnostics.cutoff(policy.value(), value).isPresent()
                    && !diagnostics.prunedAt().isBefore(AlarmDiagnostics.cutoff(policy.value(), value).orElseThrow()), "diagnosticPruning", "before-original-cutoff");
        }
        public Instant historyUntil() {
            return value.endedAt() == null ? null : DeliveryRetention.add(value.endedAt(), policy.value().duration(AlertRetentionPolicy.Setting.ENDED_EPISODE));
        }
        public Instant diagnosticUntil() { return AlarmDiagnostics.cutoff(policy.value(), value).orElse(null); }
    }
    public record Head(AlertSnapshot snapshot, AlertChannelPolicy.Deployment deployment,
            AlertOwnership owner, Instant retiredAt, boolean summariesHandedOff, Episode episode, AlertIntent.Member binding, OperationalEvidence operationalEvidence) {
        public Head(AlertSnapshot snapshot,AlertChannelPolicy.Deployment deployment,AlertOwnership owner,Instant retiredAt,boolean handedOff,Episode episode,AlertIntent.Member binding) {
            this(snapshot,deployment,owner,retiredAt,handedOff,episode,binding,null);
        }
        public Head(AlertSnapshot snapshot,AlertChannelPolicy.Deployment deployment,AlertOwnership owner,Instant retiredAt,boolean handedOff,Episode episode) {
            this(snapshot,deployment,owner,retiredAt,handedOff,episode,null,null);
        }
        public Head {
            Objects.requireNonNull(snapshot, "snapshot"); Objects.requireNonNull(deployment, "deployment"); Objects.requireNonNull(owner, "owner");
            AlertValidation.require(deployment == AlertChannelPolicy.Deployment.ON_PREM || deployment == AlertChannelPolicy.Deployment.CLOUD_MANAGED,
                    "deployment", "unresolved-state-profile");
            AlertValidation.require((snapshot.episode() == null) == (episode == null) && (episode == null
                    || snapshot.episode().equals(episode.value()) && snapshot.rule().key().equals(episode.originalBinding().key())
                    && deployment == episode.policy().deployment()), "stateEpisode", "incomplete-or-foreign");
            AlertValidation.require(binding == null || binding.evaluation().equals(snapshot.rule()) && binding.episode() == snapshot.episodeSequence(), "stateBinding", "foreign-evaluation");
            AlertValidation.require(retiredAt == null || snapshot.status() != AlertSnapshot.Status.FIRING
                    && (episode == null || episode.value().endedAt() != null && !retiredAt.isBefore(episode.value().endedAt())),
                    "stateRetirement", "active-or-before-end");
        }
        public Instant cleanupAt() { return retiredAt == null ? null : DeliveryRetention.add(retiredAt,
                deployment == AlertChannelPolicy.Deployment.ON_PREM ? Duration.ofDays(7) : Duration.ofDays(1)); }
    }
    public record Transition(AlertUpdate.Transition value, AlertSnapshot.EndReason endReason,
            DiagnosticStatus diagnosticStatus, AlarmDiagnostics.Occurrence diagnosticReference,
            AlertIntent.Policy policy) {
        public Transition {
            Objects.requireNonNull(value, "value"); Objects.requireNonNull(diagnosticStatus, "diagnosticStatus"); Objects.requireNonNull(policy, "policy");
            AlertValidation.require(value.kind() == AlertUpdate.Kind.ADMINISTRATIVELY_ENDED ? endReason != null
                    && endReason != AlertSnapshot.EndReason.RECOVERED : value.kind() == AlertUpdate.Kind.RESOLVED
                    ? endReason == AlertSnapshot.EndReason.RECOVERED : endReason == null, "transitionReason", "missing-or-unexpected");
            AlertValidation.require((diagnosticStatus == DiagnosticStatus.CAPTURED || diagnosticStatus == DiagnosticStatus.PRUNED)
                    == (diagnosticReference != null), "transitionDiagnostic", "inconsistent-reference");
            AlertValidation.require(diagnosticReference == null || diagnosticReference.episode() == value.episode(), "transitionDiagnostic", "foreign-episode");
        }
        public Instant cleanupAt() { return DeliveryRetention.add(value.at(), policy.value().duration(AlertRetentionPolicy.Setting.TRANSITION)); }
    }
}
