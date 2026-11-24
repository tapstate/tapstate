package io.tapstate.control.core;

import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RetrySettingsStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** The administrative projection of the cluster's original CONFIG retry policy. */
public final class RetrySettingsService {
    public record PolicyBody(Boolean enabled, Long intervalMillis, Long maxDurationMillis) {}

    public record SettingsBody(Long revision, PolicyBody defaults, Map<String, PolicyBody> operations) {}

    private final RetrySettingsStore store;
    private final AuditGate audit;

    public RetrySettingsService(RetrySettingsStore store, AuditGate audit) {
        this.store = Objects.requireNonNull(store, "retry settings store");
        this.audit = Objects.requireNonNull(audit, "audit gate");
    }

    public SettingsBody get() {
        return response(store.read());
    }

    public SettingsBody replace(String principal, SettingsBody body) {
        if (body == null) throw malformed();
        RetrySettingsStore.Settings input;
        try {
            var global = body.defaults();
            if (body.revision() == null || body.revision() < 0 || body.revision() == Long.MAX_VALUE
                    || global == null || global.enabled() == null
                    || global.intervalMillis() == null || global.maxDurationMillis() == null) throw malformed();
            var defaults = new RetryPolicy(global.enabled(), duration(global.intervalMillis()),
                    duration(global.maxDurationMillis()));
            var overrides = new LinkedHashMap<String, RetrySettingsStore.OperationOverride>();
            if (body.operations() != null) body.operations().forEach((operation, value) -> {
                if (value == null) throw malformed();
                overrides.put(operation, new RetrySettingsStore.OperationOverride(
                        value.enabled(), duration(value.intervalMillis()), duration(value.maxDurationMillis())));
            });
            input = new RetrySettingsStore.Settings(body.revision(), defaults, overrides);
        } catch (IllegalArgumentException | ArithmeticException invalid) {
            throw malformed();
        }
        var saved = audit.dispatch(ControlOperations.RETRY_SETTINGS_REPLACE,
                new AuditContext(principal, "retry-settings"),
                () -> store.replace(input.revision(), input.defaults(), input.operations()));
        return response(saved.orElseThrow(() -> new TapstateException(
                ControlError.RETRY_SETTINGS_REVISION_CONFLICT, Map.of(), null)));
    }

    private static SettingsBody response(RetrySettingsStore.Settings policy) {
        var global = policy.defaults();
        var overrides = new LinkedHashMap<String, PolicyBody>();
        policy.operations().forEach((operation, value) -> overrides.put(operation,
                new PolicyBody(value.enabled(), millis(value.interval()), millis(value.maxDuration()))));
        var defaults = new PolicyBody(global.enabled(), global.interval().toMillis(), global.maxDuration().toMillis());
        return new SettingsBody(policy.revision(), defaults, overrides);
    }

    private static Long millis(Duration value) {
        return value == null ? null : value.toMillis();
    }

    private static Duration duration(Long value) {
        return value == null ? null : Duration.ofMillis(value);
    }

    private static TapstateException malformed() {
        return new TapstateException(ControlError.MALFORMED_REQUEST,
                Map.of("reason", "Retry settings require a revision, positive durations, and registered operations"), null);
    }

}
