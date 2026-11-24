package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectionTestReport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionTestResponseTest {
    @Test
    void failingChecksKeepTheirOwnDiagnosticsAndSuccessfulChecksHaveNoError() {
        var child = Map.of("code", "28P01", "codeSource", "jdbc-sqlstate");
        var diagnostic = Map.<String, Object>of("code", "10002", "codeSource", "pdk", "cause", child);
        var failed = new ConnectionTestReport.Check("Login", ConnectionTestReport.Check.Status.FAILED,
                "Login rejected", null, null, "opaque vendor text", diagnostic);
        var uncoded = new ConnectionTestReport.Check("Read", ConnectionTestReport.Check.Status.FAILED,
                null, "Table unavailable", null, "opaque vendor text");
        var passed = new ConnectionTestReport.Check("Version", ConnectionTestReport.Check.Status.PASSED,
                "OK", null, null, null);
        var report = new ConnectionTestReport("orders", "postgres", ConnectionTestReport.Outcome.FAILED,
                List.of(failed, uncoded, passed), 1);
        var checks = ConnectionTestResponse.from(report, io.tapstate.messages.MessageCatalog.bundled(), java.util.Locale.ENGLISH).checks();
        assertThat(checks.get(0).error()).isEqualTo(new ApiError("10002", Map.of(), "Login rejected", null, diagnostic));
        assertThat(checks.get(1).error()).isEqualTo(new ApiError(null, Map.of(), "Table unavailable"));
        assertThat(checks.get(2).error()).isNull();
    }
}
