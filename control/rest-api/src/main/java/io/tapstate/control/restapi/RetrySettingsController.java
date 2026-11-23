package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditContext;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RetrySettingsStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
class RetrySettingsController {
    private final ObjectProvider<RetrySettingsStore> stores;
    private final ObjectProvider<AuditGate> audits;

    RetrySettingsController(ObjectProvider<RetrySettingsStore> stores, ObjectProvider<AuditGate> audits) {
        this.stores = stores;
        this.audits = audits;
    }

    @Verb("retry-settings.get")
    @GetMapping("/retry-settings")
    ResponseEntity<SettingsBody> get() {
        return response(store().read());
    }

    @Verb("retry-settings.replace")
    @PutMapping("/retry-settings")
    ResponseEntity<SettingsBody> put(@RequestBody SettingsBody body) {
        var store = store();
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
        var audit = audits.getIfAvailable();
        if (audit == null) throw unavailable();
        var saved = audit.dispatch(ControlOperations.RETRY_SETTINGS_REPLACE,
                new AuditContext(AuthenticatedCaller.subject(), "retry-settings"),
                () -> store.replace(input.revision(), input.defaults(), input.operations()));
        return response(saved.orElseThrow(() -> new TapstateException(
                ControlError.RETRY_SETTINGS_REVISION_CONFLICT, Map.of(), null)));
    }

    private RetrySettingsStore store() {
        var store = stores.getIfAvailable();
        if (store == null) throw unavailable();
        return store;
    }

    private static ResponseEntity<SettingsBody> response(RetrySettingsStore.Settings policy) {
        var global = policy.defaults();
        var overrides = new LinkedHashMap<String, PolicyBody>();
        policy.operations().forEach((operation, value) -> overrides.put(operation,
                new PolicyBody(value.enabled(), millis(value.interval()), millis(value.maxDuration()))));
        var defaults = new PolicyBody(global.enabled(), global.interval().toMillis(), global.maxDuration().toMillis());
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(new SettingsBody(policy.revision(), defaults, overrides));
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

    private static TapstateException unavailable() {
        return new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
    }

    record PolicyBody(Boolean enabled, Long intervalMillis, Long maxDurationMillis) {}

    record SettingsBody(Long revision, PolicyBody defaults, Map<String, PolicyBody> operations) {}
}
