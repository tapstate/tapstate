package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Pure compatibility decisions. None grants a send permit, a budget change, or permission to delete. */
public final class DeliveryCapabilityPolicy {
    public static final int CONTRACT_VERSION = 1;
    public static final int MAX_CHANNELS = 64;
    public enum Disposition { COMPATIBLE, PAUSED, EXPIRED, STOPPED, ISOLATED }
    public enum Reason { SUPPORTED, MISSING_RECORD_VERSION, UNSUPPORTED_RECORD_VERSION, INVALID_IDENTITY,
        SCOPE_MISMATCH, INVALID_CHANNEL, MISSING_DEADLINE, ALREADY_STOPPED, DEADLINE_REACHED,
        UNKNOWN_CHANNEL, UNSUPPORTED_CHANNEL_VERSION, ADAPTER_UNAVAILABLE, DEPLOYMENT_UNRESOLVED,
        CHANNEL_NOT_PERMITTED }

    /** A bounded decoded metadata view, not a storage codec. Missing fields must remain missing. */
    public record Header(Integer recordVersion, AlarmKey.Scope scope, String deliveryId, String channelType,
            Integer channelVersion, Instant deadline) { }
    public record Decision(Disposition disposition, Reason reason) { }

    private final Map<String, AlertChannelPolicy.Capability> channels;
    private final Set<AlertChannelPolicy.Capability> available;

    /** Static descriptions are distinct from the exact subset of adapters installed by the trusted host. */
    public DeliveryCapabilityPolicy(Collection<AlertChannelPolicy.Capability> descriptions,
            Collection<AlertChannelPolicy.Capability> available) {
        Objects.requireNonNull(descriptions, "descriptions"); Objects.requireNonNull(available, "available");
        AlertValidation.require(descriptions.size() <= MAX_CHANNELS && available.size() <= MAX_CHANNELS,
                "channels", "too-many");
        Map<String, AlertChannelPolicy.Capability> registered = new HashMap<>();
        for (var capability : descriptions) {
            Objects.requireNonNull(capability, "capability");
            AlertValidation.require(registered.putIfAbsent(capability.typeId(), capability) == null,
                    "channelType", "duplicate-id");
        }
        Set<AlertChannelPolicy.Capability> installed = new HashSet<>();
        for (var capability : available) {
            Objects.requireNonNull(capability, "capability");
            AlertValidation.require(capability.equals(registered.get(capability.typeId())),
                    "availableChannels", "unregistered-capability");
            AlertValidation.require(installed.add(capability), "availableChannels", "duplicate-capability");
        }
        channels = Map.copyOf(registered);
        this.available = Set.copyOf(installed);
    }

    /** The host must still verify authorization, original attempts, budget and all conditional writes. */
    public Decision assess(AlarmKey.Scope expectedScope, Header header, boolean stopped,
            AlertChannelPolicy.Deployment deployment, Instant now) {
        Objects.requireNonNull(expectedScope, "expectedScope"); Objects.requireNonNull(header, "header");
        Objects.requireNonNull(deployment, "deployment"); Objects.requireNonNull(now, "now");
        if (header.recordVersion() == null) { return decision(Disposition.ISOLATED, Reason.MISSING_RECORD_VERSION); }
        if (header.recordVersion() != AlarmTypeCatalog.RECORD_FORMAT_VERSION) {
            return decision(Disposition.ISOLATED, Reason.UNSUPPORTED_RECORD_VERSION);
        }
        if (header.scope() == null || header.deliveryId() == null
                || !header.deliveryId().matches("[A-Za-z0-9._-]{1,256}")) {
            return decision(Disposition.ISOLATED, Reason.INVALID_IDENTITY);
        }
        if (!expectedScope.equals(header.scope())) { return decision(Disposition.ISOLATED, Reason.SCOPE_MISMATCH); }
        if (header.channelType() == null || header.channelType().length() > 64
                || !header.channelType().matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*")
                || header.channelVersion() == null || header.channelVersion() <= 0) {
            return decision(Disposition.ISOLATED, Reason.INVALID_CHANNEL);
        }
        if (header.deadline() == null) { return decision(Disposition.ISOLATED, Reason.MISSING_DEADLINE); }
        if (stopped) { return decision(Disposition.STOPPED, Reason.ALREADY_STOPPED); }
        if (!now.isBefore(header.deadline())) { return decision(Disposition.EXPIRED, Reason.DEADLINE_REACHED); }
        var capability = channels.get(header.channelType());
        if (capability == null) { return decision(Disposition.PAUSED, Reason.UNKNOWN_CHANNEL); }
        if (capability.version() != header.channelVersion()) {
            return decision(Disposition.PAUSED, Reason.UNSUPPORTED_CHANNEL_VERSION);
        }
        if (deployment == AlertChannelPolicy.Deployment.UNRESOLVED) {
            return decision(Disposition.PAUSED, Reason.DEPLOYMENT_UNRESOLVED);
        }
        if (!AlertChannelPolicy.mayDeliver(deployment, capability)) {
            return decision(Disposition.STOPPED, Reason.CHANNEL_NOT_PERMITTED);
        }
        if (!available.contains(capability)) { return decision(Disposition.PAUSED, Reason.ADAPTER_UNAVAILABLE); }
        return decision(Disposition.COMPATIBLE, Reason.SUPPORTED);
    }

    private static Decision decision(Disposition disposition, Reason reason) { return new Decision(disposition, reason); }
}
