package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResult.Outcome;
import io.tapstate.spi.store.ConnectionTestResultStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The read side of the connection-test result: the query peer of {@link ConnectionTestService}. It returns
 * a connection's latest stored result projected onto the surface {@link ConnectionTestReport}, or empty when
 * the connection has never been tested. The store is the truth layer; this only projects the storage-port
 * result onto the control-ring type, so the faces render a control-ring report and never reach into the
 * storage ports.
 */
class ConnectionTestResultQueryServiceTest {

    private static final ConnectionTestResult RESULT = new ConnectionTestResult(
            "conn-orders", "mongodb", Outcome.FAILED,
            List.of(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                    "authentication failed", "SCRAM rejected", "check the password", "11000")),
            1_700_000_000_000L, true);

    @Test
    void findReturnsTheLatestResultProjectedOntoTheSurfaceReport() {
        MapResultStore store = new MapResultStore();
        store.save(RESULT);
        ConnectionTestResultQueryService query = new ConnectionTestResultQueryService(store);

        Optional<ConnectionTestReport> found = query.find("conn-orders");

        assertThat(found).isPresent();
        ConnectionTestReport report = found.get();
        assertThat(report.connectionId()).isEqualTo("conn-orders");
        assertThat(report.connectorId()).isEqualTo("mongodb");
        assertThat(report.outcome()).isEqualTo(ConnectionTestReport.Outcome.FAILED);
        assertThat(report.testedAt()).isEqualTo(1_700_000_000_000L);
        // The storage-port items project onto the surface checks field-for-field, diagnostics included.
        assertThat(report.checks()).singleElement().satisfies(check -> {
            assertThat(check.name()).isEqualTo("Login");
            assertThat(check.status()).isEqualTo(ConnectionTestReport.Check.Status.FAILED);
            assertThat(check.message()).isEqualTo("authentication failed");
            assertThat(check.reason()).isEqualTo("SCRAM rejected");
            assertThat(check.solution()).isEqualTo("check the password");
            assertThat(check.connectorErrorCode()).isEqualTo("11000");
        });
    }

    @Test
    void rereadingAnEarlierPostgresReportKeepsItsOriginalPrimaryCodeAndCauseChain() {
        var diagnostic = Map.<String, Object>of("code", "28P01", "cause",
                Map.of("code", "10002", "cause", Map.of("code", "28P01")));
        var stored = new ConnectionTestResult("conn-orders", "postgres", Outcome.FAILED,
                List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.FAILED,
                        "password authentication failed", null, null, "28P01", diagnostic)),
                1_700_000_000_000L, true);
        var store = new MapResultStore();
        store.save(stored);

        var check = new ConnectionTestResultQueryService(store).find("conn-orders").orElseThrow().checks().getFirst();

        assertThat(check.connectorErrorCode()).isEqualTo("28P01");
        assertThat(check.diagnostic()).isEqualTo(diagnostic);
        assertThat(store.find("conn-orders")).contains(stored);
    }

    @Test
    void findReturnsEmptyWhenTheConnectionWasNeverTested() {
        ConnectionTestResultQueryService query = new ConnectionTestResultQueryService(new MapResultStore());

        assertThat(query.find("never-tested")).isEmpty();
    }

    @Test
    void hidesDiagnosticsFromLegacyResultsWithoutAStoredSafetyMarker() {
        ConnectionTestResult legacy = new ConnectionTestResult(
                "conn-orders", "mongodb", Outcome.FAILED,
                List.of(new ConnectionTestItem("Login private credential", ConnectionTestItem.Status.FAILED,
                        "old password value", "old private key", "rotate the password", "secret-code")),
                1_700_000_000_000L);
        MapResultStore store = new MapResultStore();
        store.save(legacy);

        ConnectionTestReport report = new ConnectionTestResultQueryService(store)
                .find("conn-orders").orElseThrow();

        assertThat(report.outcome()).isEqualTo(ConnectionTestReport.Outcome.FAILED);
        assertThat(report.testedAt()).isEqualTo(1_700_000_000_000L);
        assertThat(report.checks()).singleElement().satisfies(check -> {
            assertThat(check.name()).isEqualTo("Check 1");
            assertThat(check.status()).isEqualTo(ConnectionTestReport.Check.Status.FAILED);
            assertThat(check.message()).isNull();
            assertThat(check.reason()).isNull();
            assertThat(check.solution()).isNull();
            assertThat(check.connectorErrorCode()).isNull();
        });
    }

    @Test
    void requiresItsStore() {
        assertThatThrownBy(() -> new ConnectionTestResultQueryService(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void hidesDriverCredentialsWhenTheOldReportAlreadyHasASafetyMarker() {
        MapResultStore store = new MapResultStore();
        store.save(new ConnectionTestResult("conn-orders", "mongodb-atlas", Outcome.FAILED,
                List.of(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                        "SCRAM rejected MongoCredential{userName='fixture-account', mechanismProperties=<hidden>}",
                        null, null, null)), 1_700_000_000_000L, true));

        var report = new ConnectionTestResultQueryService(store).find("conn-orders").orElseThrow();

        assertThat(report.outcome()).isEqualTo(ConnectionTestReport.Outcome.FAILED);
        assertThat(report.checks().getFirst().message())
                .contains("SCRAM rejected", "<redacted>").doesNotContain("fixture-account");
    }

    @Test
    void leavesEndpointDescriptionsAndContactAddressesIntact() {
        String message = "No reply from mongodb://db.example:27017 after 3 attempts; contact ops@example.org";
        String reason = "A proxy was used; server replied {code=18}";
        MapResultStore store = new MapResultStore();
        store.save(new ConnectionTestResult("conn-orders", "mongodb", Outcome.FAILED,
                List.of(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                        message, reason, null, null)), 1_700_000_000_000L, true));

        var check = new ConnectionTestResultQueryService(store).find("conn-orders").orElseThrow()
                .checks().getFirst();

        assertThat(check.message()).isEqualTo(message);
        assertThat(check.reason()).isEqualTo(reason);
    }

    @Test
    void hidesUserInfoAndProxyCredentialsInPreviouslyStoredReports() {
        MapResultStore store = new MapResultStore();
        store.save(new ConnectionTestResult("conn-orders", "mongodb", Outcome.FAILED,
                List.of(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                        "Unable to connect to mongodb://fixture-user:fixture-secret@db.example/data",
                        "Refused ProxySettings{host=proxy, port=1080, username=fixture-user, password=fixture-secret}",
                        "Check the server address", null)), 1_700_000_000_000L, true));

        var check = new ConnectionTestResultQueryService(store).find("conn-orders").orElseThrow()
                .checks().getFirst();

        assertThat(check.message()).isEqualTo("Unable to connect to mongodb://<redacted>@db.example/data");
        assertThat(check.reason()).isEqualTo("Refused ProxySettings<redacted>");
        assertThat(check.solution()).isEqualTo("Check the server address");
    }

    @Test
    void hidesAnIncompleteCredentialDumpAfterACompleteOneInStoredDiagnostics() {
        String credentials = "MongoCredential{userName='first', mechanismProperties=<hidden>}"
                + " then MongoCredential{userName='hidden-tail-user'";
        String proxy = "ProxySettings{username=first, password=first-secret}"
                + " then ProxySettings{username=hidden-tail-proxy, password=hidden-tail-secret";
        MapResultStore store = new MapResultStore();
        store.save(new ConnectionTestResult("conn-orders", "mongodb", Outcome.FAILED,
                List.of(new ConnectionTestItem("Login", ConnectionTestItem.Status.FAILED,
                        "Login failed", null, null, null,
                        Map.of("rawMessage", credentials, "stackTrace", proxy))), 1_700_000_000_000L, true));

        var diagnostic = new ConnectionTestResultQueryService(store).find("conn-orders").orElseThrow()
                .checks().getFirst().diagnostic();
        assertThat(diagnostic).containsEntry("rawMessage", "MongoCredential<redacted>")
                .containsEntry("stackTrace", "ProxySettings<redacted>");
    }

    @Test
    void aNullConnectionIdIsRejected() {
        ConnectionTestResultQueryService query = new ConnectionTestResultQueryService(new MapResultStore());
        assertThatThrownBy(() -> query.find(null)).isInstanceOf(NullPointerException.class);
    }

    /** A latest-only result store keyed by connection id. */
    private static final class MapResultStore implements ConnectionTestResultStore {
        private final Map<String, ConnectionTestResult> byId = new LinkedHashMap<>();

        @Override
        public void save(ConnectionTestResult result) {
            byId.put(result.connectionId(), result);
        }

        @Override
        public Optional<ConnectionTestResult> find(String connectionId) {
            return Optional.ofNullable(byId.get(connectionId));
        }
    }
}
