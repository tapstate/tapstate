package io.tapstate.control.core;

import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Removes submitted shared transport secrets from connector-controlled connection-test diagnostics. */
final class ConnectionTestResultRedactor {

    private ConnectionTestResultRedactor() {
    }

    static List<String> sharedSecretValues(Map<String, Object> settings) {
        if (settings == null) return List.of();
        LinkedHashSet<String> values = new LinkedHashSet<>();
        add(values, settings.get("sslKey"));
        add(values, settings.get("sslKeyPassword"));
        Object tapdata = settings.get("__TAPDATA");
        if (tapdata instanceof Map<?, ?> nested) add(values, nested.get("sshPassword"));
        Object uri = settings.get("uri");
        if (uri instanceof String text) values.addAll(MongoUriUserInfo.secretValues(text));
        return List.copyOf(values);
    }

    static ConnectionTestResult redact(ConnectionTestResult result, List<String> secretValues) {
        List<String> ordered = new ArrayList<>(new LinkedHashSet<>(secretValues));
        ordered.removeIf(value -> value == null || value.isBlank());
        ordered.sort((left, right) -> Integer.compare(right.length(), left.length()));
        if (ordered.isEmpty()) return result;
        List<ConnectionTestItem> items = result.items().stream()
                .map(item -> new ConnectionTestItem(
                        redact(item.name(), ordered),
                        item.status(),
                        redact(item.message(), ordered),
                        redact(item.reason(), ordered),
                        redact(item.solution(), ordered),
                        redact(item.connectorErrorCode(), ordered)))
                .toList();
        return new ConnectionTestResult(
                result.connectionId(), result.connectorId(), result.outcome(), items, result.testedAt());
    }

    private static void add(LinkedHashSet<String> values, Object candidate) {
        if (candidate instanceof String text && !text.isBlank()) values.add(text);
    }

    private static String redact(String text, List<String> secretValues) {
        if (text == null) return null;
        String safe = text;
        for (String value : secretValues) {
            safe = safe.replace(value, MongoUriUserInfo.REDACTED);
        }
        return safe;
    }
}
