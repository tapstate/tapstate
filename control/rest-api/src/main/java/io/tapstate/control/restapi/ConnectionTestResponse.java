package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.tapstate.control.core.ConnectionTestReport;

import java.util.List;
import java.util.Map;

/** Projects both live and stored reports without changing the stored check or its legacy fields. */
record ConnectionTestResponse(String connectionId, String connectorId, ConnectionTestReport.Outcome outcome,
        List<Check> checks, long testedAt) {
    static ConnectionTestResponse from(ConnectionTestReport report, io.tapstate.messages.MessageCatalog catalog, java.util.Locale locale) {
        return new ConnectionTestResponse(report.connectionId(), report.connectorId(), report.outcome(),
                report.checks().stream().map(check -> Check.from(check, catalog, locale)).toList(), report.testedAt());
    }

    record Check(@JsonUnwrapped ConnectionTestReport.Check item,
            @JsonInclude(JsonInclude.Include.NON_NULL) ApiError error) {
        static Check from(ConnectionTestReport.Check item, io.tapstate.messages.MessageCatalog catalog, java.util.Locale locale) {
            if (item.status() != ConnectionTestReport.Check.Status.FAILED) return new Check(item, null);
            Map<String, Object> diagnostic = item.diagnostic() == null ? Map.of() : ApiError.localizeDiagnostic(item.diagnostic(), catalog, locale);
            String code = diagnostic.get("code") instanceof String text ? text : null;
            String message = diagnostic.get("message") instanceof String text ? text
                    : item.message() != null ? item.message() : item.reason();
            String solution = diagnostic.get("solution") instanceof String text ? text : item.solution();
            Map<String, Object> params = new java.util.TreeMap<>();
            if (diagnostic.get("params") instanceof Map<?, ?> captured) {
                captured.forEach((key, value) -> { if (key instanceof String name) params.put(name, value); });
            }
            return new Check(item, new ApiError(code, params, message, solution, diagnostic));
        }
    }
}
