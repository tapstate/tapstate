package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Absolute target restriction independent of any intent or receiver configuration incarnation. */
public record AlertTargetLimit(Domain domain, Instant notBefore) {
    /** The trusted host derives the semantic target and authentication identity; neither is a credential or a mutable display name. */
    public record Domain(AlarmKey.Scope scope, String channelType, String semanticTargetId, String authenticationIdentity) {
        public Domain {
            Objects.requireNonNull(scope, "scope");
            channelType = AlertValidation.text(channelType, "channelType", 64);
            AlertValidation.require(channelType.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), "channelType", "invalid-id");
            semanticTargetId = AlertValidation.text(semanticTargetId, "semanticTargetId", 256);
            authenticationIdentity = AlertValidation.text(authenticationIdentity, "authenticationIdentity", 256);
        }
    }
    public AlertTargetLimit { Objects.requireNonNull(domain, "domain"); Objects.requireNonNull(notBefore, "notBefore"); }
    /** Response receipt time was already converted once by the adapter. Replays never reinterpret a relative delay using a new clock. */
    public AlertTargetLimit observe(Domain originalDomain, Instant fixedRetryAfter) {
        AlertValidation.require(domain.equals(originalDomain), "targetLimit", "foreign-domain");
        Objects.requireNonNull(fixedRetryAfter, "fixedRetryAfter");
        return fixedRetryAfter.isAfter(notBefore) ? new AlertTargetLimit(domain, fixedRetryAfter) : this;
    }
    public AlertTargetLimit merge(AlertTargetLimit other) {
        Objects.requireNonNull(other, "other"); return observe(other.domain(), other.notBefore());
    }
    /** Applies equally to business, manual retry, recovery and configuration-test candidates. It never extends their original deadline. */
    public Optional<Instant> nextPermitAt(Instant candidateAt, Instant originalDeadline) {
        Objects.requireNonNull(candidateAt, "candidateAt"); Objects.requireNonNull(originalDeadline, "originalDeadline");
        Instant next = candidateAt.isAfter(notBefore) ? candidateAt : notBefore;
        return next.isBefore(originalDeadline) ? Optional.of(next) : Optional.empty();
    }
    public boolean restrictionElapsed(Instant now) { return !now.isBefore(notBefore); }
}
