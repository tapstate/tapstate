package io.tapstate.core.alerting;

import java.util.Objects;

/** Fixed operation capabilities retained with the original alarm head, never a raw operation log. */
public record OperationalEvidence(String bindingDigest, int failedCapabilities,int version) {
    public OperationalEvidence(String bindingDigest,int failedCapabilities){this(bindingDigest,failedCapabilities,1);}
    public enum Capability { INITIALIZE, METADATA_READ, SNAPSHOT_READ, STREAM_READ, TARGET_READ, TARGET_WRITE, CACHE_READ, CACHE_WRITE }
    public OperationalEvidence {
        Objects.requireNonNull(bindingDigest);
        AlertValidation.require((version==1 || version==2) && bindingDigest.matches("[0-9a-f]{64}") && failedCapabilities >= 0 && failedCapabilities <= 255,
                "operationalEvidence", "invalid-binding-or-mask");
    }
    public OperationalEvidence observe(Capability capability, boolean succeeded) {
        int bit = 1 << Objects.requireNonNull(capability).ordinal();
        return new OperationalEvidence(bindingDigest, succeeded ? failedCapabilities & ~bit : failedCapabilities | bit,version);
    }
    public AlertSignal.Condition condition() {
        return failedCapabilities == 0 ? AlertSignal.Condition.GOOD : AlertSignal.Condition.BAD;
    }
}
