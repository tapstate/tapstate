package io.tapstate.core.alerting;

import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/** Frozen policy values only. The host supplies a verified deployment and commits the whole candidate. */
public final class AlertRetentionPolicy {
    public static final int POLICY_VERSION = 1;
    public enum Setting {
        ACCEPTED_PAYLOAD, ACCEPTED_RESULT, FAILED_PAYLOAD, FAILED_RESULT, UNREAD_NOTIFICATION,
        READ_NOTIFICATION, TEST_PAYLOAD, TEST_ACCEPTED_RESULT, TEST_FAILED_RESULT, ENDED_EPISODE, TRANSITION
    }
    public record Range(Duration defaultValue, Duration minimum, Duration maximum) { }
    private final Deployment deployment;
    private final Map<Setting, Duration> values;
    private AlertRetentionPolicy(Deployment deployment, Map<Setting, Duration> values) {
        this.deployment = deployment; this.values = Map.copyOf(values);
    }
    public Deployment deployment() { return deployment; }
    public Map<Setting, Duration> values() { return values; }
    public Duration duration(Setting setting) {
        AlertValidation.require(values.containsKey(setting), "retention." + setting, "not-configurable-for-deployment");
        return values.get(setting);
    }
    public Duration acceptedPayload() {
        return deployment == Deployment.CLOUD_MANAGED ? Duration.ofHours(1) : duration(Setting.ACCEPTED_PAYLOAD);
    }
    public static AlertRetentionPolicy defaults(Deployment trustedDeployment) {
        var values = new EnumMap<Setting, Duration>(Setting.class);
        ranges(trustedDeployment).forEach((setting, range) -> values.put(setting, range.defaultValue()));
        return configure(trustedDeployment, values);
    }
    /** Requires every applicable field, rejects forbidden fields, and never clamps or partially applies. */
    public static AlertRetentionPolicy configure(Deployment trustedDeployment, Map<Setting, Duration> candidate) {
        Map<Setting, Range> ranges = ranges(trustedDeployment);
        AlertValidation.require(candidate != null && candidate.keySet().equals(ranges.keySet()), "retention", "wrong-fields-for-deployment");
        var frozen = new EnumMap<Setting, Duration>(Setting.class);
        for (var entry : ranges.entrySet()) {
            Duration value = candidate.get(entry.getKey()); Range range = entry.getValue();
            AlertValidation.require(value != null && value.getNano() == 0 && value.getSeconds() % 60 == 0
                    && value.compareTo(range.minimum()) >= 0 && value.compareTo(range.maximum()) <= 0,
                    "retention." + entry.getKey(), "outside-whole-minute-range");
            frozen.put(entry.getKey(), value);
        }
        atMost(frozen, Setting.ACCEPTED_RESULT, Setting.FAILED_RESULT);
        atMost(frozen, Setting.FAILED_PAYLOAD, Setting.FAILED_RESULT);
        atMost(frozen, Setting.READ_NOTIFICATION, Setting.UNREAD_NOTIFICATION);
        if (trustedDeployment == Deployment.ON_PREM) {
            atMost(frozen, Setting.ACCEPTED_PAYLOAD, Setting.FAILED_PAYLOAD);
            atMost(frozen, Setting.ACCEPTED_PAYLOAD, Setting.ACCEPTED_RESULT);
            atMost(frozen, Setting.TEST_ACCEPTED_RESULT, Setting.TEST_FAILED_RESULT);
            atMost(frozen, Setting.TEST_PAYLOAD, Setting.TEST_ACCEPTED_RESULT);
            atMost(frozen, Setting.TEST_PAYLOAD, Setting.TEST_FAILED_RESULT);
        }
        return new AlertRetentionPolicy(trustedDeployment, frozen);
    }
    private static void atMost(Map<Setting, Duration> values, Setting shorter, Setting longer) {
        AlertValidation.require(values.get(shorter).compareTo(values.get(longer)) <= 0,
                "retention." + shorter, "must-not-exceed-" + longer);
    }
    /** The same immutable declarations can drive a configuration UI; they do not authenticate a request. */
    public static Map<Setting, Range> ranges(Deployment trustedDeployment) {
        AlertValidation.require(trustedDeployment == Deployment.ON_PREM || trustedDeployment == Deployment.CLOUD_MANAGED,
                "deployment", "retention-profile-unresolved");
        var ranges = new EnumMap<Setting, Range>(Setting.class);
        if (trustedDeployment == Deployment.ON_PREM) {
            put(ranges, Setting.ACCEPTED_PAYLOAD, 1440, 15, 10080);
            put(ranges, Setting.ACCEPTED_RESULT, 10080, 360, 43200);
            put(ranges, Setting.FAILED_PAYLOAD, 4320, 360, 10080);
            put(ranges, Setting.FAILED_RESULT, 20160, 1440, 129600);
            put(ranges, Setting.UNREAD_NOTIFICATION, 43200, 1440, 129600);
            put(ranges, Setting.READ_NOTIFICATION, 4320, 60, 10080);
            put(ranges, Setting.TEST_PAYLOAD, 60, 5, 1440);
            put(ranges, Setting.TEST_ACCEPTED_RESULT, 1440, 60, 10080);
            put(ranges, Setting.TEST_FAILED_RESULT, 4320, 360, 10080);
            put(ranges, Setting.ENDED_EPISODE, 43200, 1440, 129600);
            put(ranges, Setting.TRANSITION, 20160, 360, 43200);
        } else {
            put(ranges, Setting.ACCEPTED_RESULT, 1440, 360, 2880);
            put(ranges, Setting.FAILED_PAYLOAD, 1440, 360, 1440);
            put(ranges, Setting.FAILED_RESULT, 4320, 1440, 10080);
            put(ranges, Setting.UNREAD_NOTIFICATION, 4320, 1440, 10080);
            put(ranges, Setting.READ_NOTIFICATION, 720, 60, 1440);
            put(ranges, Setting.ENDED_EPISODE, 10080, 1440, 10080);
            put(ranges, Setting.TRANSITION, 4320, 360, 10080);
        }
        return Map.copyOf(ranges);
    }
    private static void put(Map<Setting, Range> ranges, Setting setting, long defaultMinutes, long minimum, long maximum) {
        ranges.put(setting, new Range(Duration.ofMinutes(defaultMinutes), Duration.ofMinutes(minimum), Duration.ofMinutes(maximum)));
    }
}
