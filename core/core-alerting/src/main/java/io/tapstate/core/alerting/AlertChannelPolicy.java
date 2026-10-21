package io.tapstate.core.alerting;

import java.util.Objects;

/** Deployment comes from trusted host settings; channel families come from static adapter registration. */
public final class AlertChannelPolicy {
    private AlertChannelPolicy() { }
    public enum Deployment { ON_PREM, CLOUD_MANAGED, UNRESOLVED }
    public enum Family { IN_APP, OP_EXTERNAL, CLOUD_SDK }
    public record Capability(String typeId, int version, Family family) {
        public Capability {
            typeId = AlertValidation.text(typeId, "channelType", 64);
            AlertValidation.require(typeId.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), "channelType", "invalid-id");
            AlertValidation.require(version > 0, "channelVersion", "must-be-positive");
            Objects.requireNonNull(family, "family");
        }
    }
    public static boolean mayDeliver(Deployment deployment, Capability channel) {
        Objects.requireNonNull(deployment, "deployment"); Objects.requireNonNull(channel, "channel");
        return switch (deployment) {
            case ON_PREM -> channel.family() == Family.IN_APP || channel.family() == Family.OP_EXTERNAL;
            case CLOUD_MANAGED -> channel.family() == Family.IN_APP || channel.family() == Family.CLOUD_SDK;
            case UNRESOLVED -> false;
        };
    }
    public static boolean mayConfigureExternal(Deployment deployment, Capability channel) {
        return deployment == Deployment.ON_PREM && channel.family() == Family.OP_EXTERNAL;
    }
    public static boolean maySendTest(Deployment deployment, Capability channel) { return mayConfigureExternal(deployment, channel); }
    public static void requireDeliverable(Deployment deployment, Capability channel) {
        AlertValidation.require(mayDeliver(deployment, channel), "deployment", "channel-not-permitted");
    }
}
