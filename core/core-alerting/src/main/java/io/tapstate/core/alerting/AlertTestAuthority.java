package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Original server-verified test caller continuation. Raw credentials and mutable session data are never persisted. */
public record AlertTestAuthority(Mechanism mechanism, String realm, String subject, String grantedScope,
        String credentialBinding, Instant expiresAt) {
    public enum Mechanism { MACHINE_TOKEN, HUMAN_JWT }
    public AlertTestAuthority {
        Objects.requireNonNull(mechanism, "mechanism"); realm = AlertValidation.text(realm, "actorRealm", 256);
        subject = AlertValidation.text(subject, "actorSubject", 256); grantedScope = AlertValidation.text(grantedScope, "actorScope", 32);
        AlertValidation.require(credentialBinding != null && credentialBinding.matches("[0-9a-f]{64}"), "actorBinding", "invalid-binding");
        AlertValidation.require((mechanism == Mechanism.HUMAN_JWT) == (expiresAt != null), "actorExpiry", "wrong-mechanism");
    }
    /** Credential rotation and session replacement do not create a new per-actor quota bucket. */
    public String principalKey() { return "actor-" + DeliveryKey.fingerprint(List.of("alert-actor-v1",mechanism.name(),realm,subject)); }
    @Override public String toString() { return "AlertTestAuthority[verified]"; }
}
