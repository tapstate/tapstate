package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Removes submitted credentials from connector-controlled connection diagnostics. */
final class ConnectionTestResultRedactor {

    private ConnectionTestResultRedactor() {
    }

    static ConnectionTestResult redact(ConnectionTestResult result, List<String> secretValues) {
        List<ConnectionTestItem> items = result.items().stream()
                .map(item -> new ConnectionTestItem(
                        redact(item.name(), secretValues),
                        item.status(),
                        redact(item.message(), secretValues),
                        redact(item.reason(), secretValues),
                        redact(item.solution(), secretValues),
                        redact(item.connectorErrorCode(), secretValues)))
                .toList();
        return new ConnectionTestResult(
                result.connectionId(), result.connectorId(), result.outcome(), items,
                result.testedAt(), true);
    }

    /** Hides legacy diagnostics whose original credential values are no longer available. */
    static ConnectionTestResult hideUntrustedDiagnostics(ConnectionTestResult result) {
        if (result.diagnosticsRedacted()) {
            return result;
        }
        List<ConnectionTestItem> safeItems = new ArrayList<>(result.items().size());
        for (int index = 0; index < result.items().size(); index++) {
            ConnectionTestItem item = result.items().get(index);
            safeItems.add(new ConnectionTestItem(
                    "Check " + (index + 1), item.status(), null, null, null, null));
        }
        return new ConnectionTestResult(
                result.connectionId(), result.connectorId(), result.outcome(), safeItems,
                result.testedAt(), true);
    }

    static TapstateException redact(TapstateException failure, List<String> secretValues) {
        Map<String, Object> safeArgs = new LinkedHashMap<>();
        failure.args().forEach((name, value) ->
                safeArgs.put(redact(name, secretValues), redactValue(value, secretValues)));
        if (failure.getCause() == null && safeArgs.equals(failure.args())) {
            return failure;
        }
        return failure.withoutCause(safeArgs);
    }

    private static Object redactValue(Object value, List<String> secretValues) {
        if (value == null || value instanceof Number || value instanceof Boolean || value instanceof Enum<?>) {
            return value;
        }
        if (value instanceof CharSequence text) {
            return redact(text.toString(), secretValues);
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> safe = new LinkedHashMap<>();
            map.forEach((key, child) -> safe.put(redactValue(key, secretValues), redactValue(child, secretValues)));
            return safe;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> safe = new ArrayList<>();
            iterable.forEach(child -> safe.add(redactValue(child, secretValues)));
            return safe;
        }
        if (value.getClass().isArray()) {
            if (value.getClass().getComponentType().isPrimitive()) {
                return MongoUriUserInfo.REDACTED;
            }
            List<Object> safe = new ArrayList<>(Array.getLength(value));
            for (int index = 0; index < Array.getLength(value); index++) {
                safe.add(redactValue(Array.get(value, index), secretValues));
            }
            return safe;
        }
        return MongoUriUserInfo.REDACTED;
    }

    private static String redact(String text, List<String> secretValues) {
        return SecretRedactor.redactValues(text, secretValues, MongoUriUserInfo.REDACTED);
    }
}
