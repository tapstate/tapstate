package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoConnectionDiagnosticTest {
    @Test
    void compactionKeepsBothCauseFormsAndParameterFormatsWithoutSplittingUnicode() {
        String prefix = "x".repeat(1023);
        String text = prefix + "\uD83D\uDD0E" + "tail";
        String shortened = prefix + "\u2026";
        var diagnostic = Map.<String, Object>of("code", "driver.denied", "message", text, "solution", text,
                "rawMessage", "driver detail", "stackTrace", "x".repeat(256 * 1024),
                "causes", List.of(Map.of("code", "driver.named", "params", Map.of("detail", text, "attempt", 2),
                                "cause", Map.of("message", text)),
                        Map.of("code", "driver.positional", "arguments", List.of(text, "kept"))));
        var result = report(item("Login", "Rejected", diagnostic));

        var fitted = MongoConnectionDiagnostic.fit(result, candidate -> true);

        assertThat(fitted.items()).containsExactly(item("Login", "Rejected",
                Map.of("code", "driver.denied", "message", shortened, "solution", shortened, "truncated", true,
                        "causes", List.of(
                                Map.of("code", "driver.named", "params", Map.of("detail", shortened, "attempt", 2),
                                        "cause", Map.of("message", shortened, "truncated", true), "truncated", true),
                                Map.of("code", "driver.positional", "arguments", List.of(shortened, "kept"),
                                        "truncated", true)))));
        assertThat(MongoConnectionTestResultStore.strictCacheResult(cacheDocument(fitted))).isEqualTo(fitted);
    }

    @Test
    void reportReferenceBudgetCountsNestedReferencesAcrossChecksAndKeepsOneTruncationMarker() {
        var reference = Map.of("type", "connection", "id", "test-connection");
        var full = item("Login", "Rejected", Map.of("cause", Map.of("references", Collections.nCopies(128, reference))));
        var extra = item("Read", null, Map.of("references", List.of(reference)));
        var undecorated = item("Connection", null, Map.of());
        var exactlyAtBudget = report(undecorated, full);
        var exceedsBudget = report(undecorated, full, extra);

        MongoConnectionDiagnostic.requireBounded(exactlyAtBudget);
        assertThatIllegalArgumentException().isThrownBy(() -> MongoConnectionDiagnostic.requireBounded(exceedsBudget));
        assertThat(MongoConnectionDiagnostic.fit(exceedsBudget, candidate -> true).items()).containsExactly(
                undecorated, item("Login", "Rejected", Map.of("truncated", true)), item("Read", null, Map.of()));
    }

    @Test
    void wholeRowBudgetCanShortenDisplayFieldsWithoutDroppingTheTruncationMarker() {
        String text = "x".repeat(2048);
        String shortened = "x".repeat(1024) + "\u2026";
        var original = report(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                text, text, text, "28P01", Map.of("message", "Authentication failed")));
        var expected = report(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                shortened, shortened, shortened, "28P01", Map.of("truncated", true)));

        var fitted = MongoConnectionDiagnostic.fit(original,
                candidate -> MongoConnectionDiagnostic.bsonSize(cacheDocument(candidate)) < 4 * 1024);

        assertThat(fitted).isEqualTo(expected);
        assertThat(MongoConnectionTestResultStore.strictCacheResult(cacheDocument(fitted))).isEqualTo(expected);
    }

    @Test
    void tightRowBudgetPreservesLegacyOutcomeAndRejectsAReportTooLargeEvenWithoutTheExtension() {
        var full = report(item("Login", "Rejected", Map.of("message", "Authentication failed")));
        var legacy = report(item("Login", "Rejected", Map.of()));
        int legacyBytes = MongoConnectionDiagnostic.bsonSize(cacheDocument(legacy));

        var fitted = MongoConnectionDiagnostic.fit(full,
                candidate -> MongoConnectionDiagnostic.bsonSize(cacheDocument(candidate)) <= legacyBytes);

        assertThat(fitted).isEqualTo(legacy);
        assertThatThrownBy(() -> MongoConnectionDiagnostic.fit(full,
                candidate -> MongoConnectionDiagnostic.bsonSize(cacheDocument(candidate)) < legacyBytes))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE);
                    assertThat(failure.getCause()).isNull();
                });
    }

    @Test
    void versionsByRecordedNodeSourcesAndKeepsHistoricalReportsUntouched() {
        var previous = new Document("formatVersion", 1).append("projection",
                new Document("code", "10002").append("cause", new Document("code", "28P01")));
        var historical = Map.<String, Object>of("code", "10002", "cause", Map.of("code", "28P01"));
        assertThat(MongoConnectionDiagnostic.read(previous)).isEqualTo(historical);
        assertThat(MongoConnectionDiagnostic.document(historical)).isEqualTo(previous);

        var recorded = Map.<String, Object>of("code", "10002", "cause",
                Map.of("code", "28P01", "codeSource", "jdbc-sqlstate"));
        var current = MongoConnectionDiagnostic.document(recorded);
        assertThat(current.getInteger("formatVersion")).isEqualTo(2);
        assertThat(MongoConnectionDiagnostic.read(current)).isEqualTo(recorded);
        assertThatIllegalArgumentException().isThrownBy(() ->
                MongoConnectionDiagnostic.read(new Document(current).append("formatVersion", 1)));
        assertThatIllegalArgumentException().isThrownBy(() ->
                MongoConnectionDiagnostic.read(new Document("formatVersion", 2).append("projection",
                        new Document("code", "10002").append("codeSource", "not-recorded"))));
    }

    @Test
    void strictCacheReaderRejectsUnsupportedOrNoncanonicalDiagnosticsAndUnredactedRows() {
        var original = report(item("Login", "Rejected", Map.of("message", "Authentication failed")));
        for (var extension : List.of("not BSON",
                new Document("formatVersion", 2).append("projection", new Document("message", "Authentication failed")),
                new Document("formatVersion", 1).append("projection", new Document("message", false)),
                new Document("formatVersion", 1).append("projection",
                        new Document("message", "Authentication failed").append("unknown", true)))) {
            var row = cacheDocument(original);
            row.getList("items", Document.class).getFirst().put("diagnostic", extension);
            assertThatIllegalArgumentException().isThrownBy(() -> MongoConnectionTestResultStore.strictCacheResult(row));
        }
        assertThatIllegalArgumentException().isThrownBy(() ->
                MongoConnectionTestResultStore.strictCacheResult(cacheDocument(original).append("diagnosticsRedacted", false)));
    }

    private static ConnectionTestItem item(String name, String message, Map<String, Object> diagnostic) {
        return new ConnectionTestItem(name, ConnectionTestItem.Status.FAILED, message, null, null, "28P01", diagnostic);
    }

    private static ConnectionTestResult report(ConnectionTestItem... items) {
        return new ConnectionTestResult("test-connection", "postgresql", ConnectionTestResult.Outcome.FAILED,
                List.of(items), 1000L, true);
    }

    private static Document cacheDocument(ConnectionTestResult result) {
        return MongoConnectionTestResultStore.compatibleCacheDocument(result);
    }
}
