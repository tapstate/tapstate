package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestItem.Status;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResult.Outcome;
import io.tapstate.spi.store.ConnectionTester;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The connection-test PDK bridge: {@link PdkConnectionTester} driving a connector's own
 * {@code connectionTest} through the frozen PDK contract and normalizing the streamed test items into a
 * {@link ConnectionTestResult}. A connector that reports a failed check is a normal FAILED result; only
 * a connector that throws out of its test is a coded connector-domain failure. Synthetic connectors
 * compiled at test time prove the drive and the coded-error path without a real connector jar or the
 * PDK runtime.
 */
class PdkConnectionTesterTest {

    private static final long FIXED_MILLIS = 1783939200000L;

    /** A tester over a provisioner that hands back one fixed connector ref, whatever id is asked for. */
    private static ConnectionTester tester(Path jar, String className) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        ConnectorProvisioner provisioner = connectorId -> ref;
        return new PdkConnectionTester(provisioner, Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC));
    }

    private static ConnectionConfig config() {
        return new ConnectionConfig("conn-1", "demo", Map.of());
    }

    @Test
    void passingConnectorYieldsPassedOutcomeAndStampsIdsAndTime(@TempDir Path dir) {
        ConnectionTester tester = tester(Synthetic.passingTest(dir), "synthetic.PassingTest");

        ConnectionTestResult result = tester.test(config());

        assertThat(result.connectionId()).isEqualTo("conn-1");
        assertThat(result.connectorId()).isEqualTo("demo");
        assertThat(result.testedAt()).isEqualTo(FIXED_MILLIS);
        assertThat(result.outcome()).isEqualTo(Outcome.PASSED);
        assertThat(result.items()).extracting(ConnectionTestItem::name).containsExactly("Connection");
        assertThat(result.items()).extracting(ConnectionTestItem::status).containsExactly(Status.PASSED);
    }

    @Test
    void aWarningItemDoesNotFailTheOutcomeAndCarriesItsInformationAsMessage(@TempDir Path dir) {
        ConnectionTester tester = tester(Synthetic.warningTest(dir), "synthetic.WarningTest");

        ConnectionTestResult result = tester.test(config());

        assertThat(result.outcome()).isEqualTo(Outcome.PASSED);
        assertThat(result.items()).extracting(ConnectionTestItem::status)
                .containsExactly(Status.PASSED, Status.WARNING);
        ConnectionTestItem warn = result.items().get(1);
        assertThat(warn.name()).isEqualTo("Time detection");
        assertThat(warn.message()).isEqualTo("clock skew 3s");
        assertThat(warn.reason()).isNull();
    }

    @Test
    void aFailedItemMakesTheOutcomeFailedAndMapsTheStructuredDiagnostics(@TempDir Path dir) {
        ConnectionTester tester = tester(Synthetic.failingTest(dir), "synthetic.FailingTest");

        ConnectionTestResult result = tester.test(config());

        assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
        assertThat(result.items()).extracting(ConnectionTestItem::name).containsExactly("Connection", "Login");
        ConnectionTestItem login = result.items().get(1);
        assertThat(login.status()).isEqualTo(Status.FAILED);
        assertThat(login.message()).isEqualTo("Login denied");
        assertThat(login.reason()).isEqualTo("bad credentials");
        assertThat(login.solution()).isEqualTo("check username/password");
        assertThat(login.connectorErrorCode()).isEqualTo("28000");
    }

    /**
     * Some connectors report a check with a bare numeric code and the statements they tried, and no
     * message at all - the postgres connector reports a failed replication-slot probe exactly this
     * way, which is the check that decides whether change capture can work on a stranger's database.
     * Read literally there is nothing to show but a number. What the connector tried is the one thing
     * present, and dropping it leaves the reader a code and no way to act on it.
     */
    @Test
    void anItemWithOnlyACodeCarriesWhatTheConnectorTriedAsItsMessage(@TempDir Path dir) {
        ConnectionTester tester = tester(Synthetic.codeOnlyTest(dir), "synthetic.CodeOnlyTest");

        ConnectionTestResult result = tester.test(config());

        ConnectionTestItem item = result.items().get(0);
        assertThat(item.status()).isEqualTo(Status.WARNING);
        assertThat(item.connectorErrorCode()).isEqualTo("410003");
        assertThat(item.message()).contains("pg_create_logical_replication_slot");
    }

    @Test
    void aConnectorWhoseConnectionTestThrowsIsACodedTestFailure(@TempDir Path dir) {
        ConnectionTester tester = tester(Synthetic.throwingTest(dir), "synthetic.ThrowingTest");

        assertThatThrownBy(() -> tester.test(config()))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code()).isEqualTo(ConnectorError.TEST_FAILED);
                    assertThat(ce.args()).containsEntry("connector", "demo").containsKey("detail");
                });
    }

    @Test void aClosedTesterRefusesDefaultWorkBeforeResolvingAnotherConnector() {
        var resolves = new java.util.concurrent.atomic.AtomicInteger();
        var tester = new PdkConnectionTester(ignored -> { resolves.incrementAndGet(); throw new AssertionError("must not resolve"); }, Clock.systemUTC(), ignored -> true);
        tester.close(); long deadline = System.nanoTime() + java.time.Duration.ofSeconds(1).toNanos();
        assertThatThrownBy(() -> tester.testDefault(config(), deadline)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ConnectorError.INSTANCES_BUSY));
        assertThat(tester.defaultTestAvailable()).isFalse(); assertThat(resolves).hasValue(0);
    }

    @Test void anUnmanagedConnectionKeepsItsOrdinaryTestEvenThroughTheDefaultPort(@TempDir Path dir) {
        var ref = new ConnectorRef(List.of(Synthetic.passingTest(dir)), "synthetic.PassingTest", "2.0.8", null);
        try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> false)) {
            long expiredDefaultDeadline = System.nanoTime() - 1;
            assertThat(tester.testDefault(config(), expiredDefaultDeadline).outcome()).isEqualTo(Outcome.PASSED);
            assertThat(tester.defaultTestAvailable()).isTrue();
        }
    }

    @Test void theRealDefaultDrivePreservesStrongerLimitsAndDoesNotRepairMalformedOptions(@TempDir Path dir) {
        var ref = new ConnectorRef(List.of(Synthetic.testSettingsEcho(dir)), "synthetic.TestSettingsEcho", "2.0.8", null);
        String original = "mongodb://host/views?serverSelectionTimeoutMS=7&connectTimeoutMS=0&socketTimeoutMS=bad&retryWrites=false";
        var config = new ConnectionConfig("views", "mongodb", Map.of("uri", original));
        try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true)) {
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
            var report = tester.testDefault(config, deadline);
            String echoed = report.items().getFirst().message();
            assertThat(echoed).contains("serverselectiontimeoutms=7", "socketTimeoutMS=bad", "retryWrites=false");
            var options = java.util.Arrays.stream(echoed.substring(echoed.indexOf('?') + 1).split("&"))
                    .map(value -> value.split("=", 2)).collect(java.util.stream.Collectors.toMap(value -> value[0].toLowerCase(java.util.Locale.ROOT), value -> value[1]));
            assertThat(Long.parseLong(options.get("connecttimeoutms"))).isBetween(1L, 10_000L);
            assertThat(config.settings()).containsEntry("uri", original);
            var noPath = new ConnectionConfig("views", "mongodb", Map.of("uri", "mongodb://host"));
            var normalized = tester.testDefault(noPath, System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos());
            assertThat(normalized.items().getFirst().message()).startsWith("mongodb://host/?")
                    .contains("serverselectiontimeoutms=", "connecttimeoutms=", "sockettimeoutms=");
        }
    }

    @Test void excessiveDiagnosticCountsAndAccumulatedTextStopTheOwnedDefaultDrive(@TempDir Path dir) {
        for (String kind : List.of("items", "sum", "parameters")) {
            var ref = new ConnectorRef(List.of(Synthetic.excessiveDiagnosticTest(dir.resolve(kind), kind)), "synthetic.ExcessiveDiagnostic", "2.0.8", null);
            try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true)) {
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
                assertThatThrownBy(() -> tester.testDefault(config(), deadline)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(io.tapstate.spi.store.IoError.DOCUMENT_TOO_LARGE));
                assertThat(tester.defaultTestAvailable()).isTrue();
            }
        }
    }

    @Test void anEmptyManagedTestCannotCertifyAConnection(@TempDir Path dir) {
        var ref = new ConnectorRef(List.of(Synthetic.emptyTestReport(dir)), "synthetic.EmptyTestReport", "2.0.8", null);
        try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true)) {
            assertThatThrownBy(() -> tester.testDefault(config(), System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos())).isInstanceOf(IllegalStateException.class);
            assertThat(tester.defaultTestAvailable()).isTrue();
        }
    }

    @Test void theOriginalExpiredDeadlineDoesNotGetAnotherBudgetOrOpenAConnector() {
        var resolves = new java.util.concurrent.atomic.AtomicInteger();
        try (var tester = new PdkConnectionTester(ignored -> { resolves.incrementAndGet(); throw new AssertionError("must not resolve"); }, Clock.systemUTC(), ignored -> true)) {
            assertThatThrownBy(() -> tester.testDefault(config(), System.nanoTime() - 1)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ConnectorError.TEST_TIMEOUT));
            assertThat(resolves).hasValue(0); assertThat(tester.defaultTestAvailable()).isTrue();
        }
    }

    @Test void replacingABusinessTesterCannotBypassAnUnfinishedNodeSlot(@TempDir Path dir) throws Exception {
        var release = new java.util.concurrent.CountDownLatch(1);
        var ref = new ConnectorRef(List.of(Synthetic.passingTest(dir)), "synthetic.PassingTest", "2.0.8", null);
        try (var slot = new PdkConnectionTester.DefaultSourceSlot(Clock.systemUTC(), java.time.Duration.ofMillis(30), java.time.Duration.ofMillis(40))) {
            ConnectorProvisioner blocked = ignored -> {
                boolean interrupted = false;
                while (true) try { if (!release.await(3, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("release missing"); break; }
                catch (InterruptedException retry) { interrupted = true; }
                if (interrupted) Thread.currentThread().interrupt();
                return ref;
            };
            var retired = new PdkConnectionTester(blocked, Clock.systemUTC(), ignored -> true, slot);
            assertThatThrownBy(() -> retired.testDefault(config(), System.nanoTime() + java.time.Duration.ofMillis(30).toNanos())).isInstanceOf(TapstateException.class);
            retired.close();
            try (var replacement = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true, slot)) {
                assertThatThrownBy(() -> replacement.testDefault(config(), System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos())).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ConnectorError.INSTANCES_BUSY));
                release.countDown();
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
                while (!replacement.defaultTestAvailable() && System.nanoTime() < deadline) Thread.sleep(1);
                assertThat(replacement.defaultTestAvailable()).isTrue();
            }
        } finally { release.countDown(); }
    }

    @Test void aKnownTypedPdkDiagnosticIsAFailedReportAfterOwnedCleanup(@TempDir Path dir) {
        var ref = new ConnectorRef(List.of(Synthetic.thrownTypedTestDiagnostic(dir)), "synthetic.TypedTestDiagnostic", "2.0.8", null);
        try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true)) {
            var result = tester.testDefault(config(), System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos());
            assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
            assertThat(result.items().getFirst().connectorErrorCode()).isEqualTo("28000");
            assertThat(result.items().getFirst().reason()).isEqualTo("bad credentials");
            assertThat(tester.defaultTestAvailable()).isTrue();
        }
    }

    @Test void anOversizedManagedReportRemainsAnUncertainCodedResult(@TempDir Path dir) {
        var ref = new ConnectorRef(List.of(Synthetic.oversizedTestReport(dir)), "synthetic.OversizedReport", "2.0.8", null);
        try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true)) {
            assertThatThrownBy(() -> tester.testDefault(config(), System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(io.tapstate.spi.store.IoError.DOCUMENT_TOO_LARGE));
            assertThat(tester.defaultTestAvailable()).isTrue();
        }
    }

    @Test void managedSourceProgrammingDefectsRemainBareThroughTheRealPdkDrive(@TempDir Path dir) {
        for (String defect : List.of("NullPointerException", "IllegalStateException", "LinkageError")) {
            String name = "Default" + defect;
            var jar = Synthetic.defectTest(dir.resolve(name), name, defect);
            var ref = new ConnectorRef(List.of(jar), "synthetic." + name, "2.0.8", null);
            try (var tester = new PdkConnectionTester(ignored -> ref, Clock.systemUTC(), ignored -> true,
                    java.time.Duration.ofSeconds(5), java.time.Duration.ofMillis(100))) {
                assertThatThrownBy(() -> tester.testDefault(config(), System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos())).isInstanceOf(defect.equals("LinkageError") ? LinkageError.class
                        : defect.equals("NullPointerException") ? NullPointerException.class : IllegalStateException.class);
                assertThat(tester.defaultTestAvailable()).isTrue();
            }
        }
    }

    @Test
    void anIncompatibleConnectorIsStillRefusedBeforeAnyTestRuns(@TempDir Path dir) {
        // The connection tester provisions through the same open path as the read/write ports: an
        // incompatible declared level is refused up front, never downgraded into a test result.
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.passingTest(dir)), "synthetic.PassingTest", "2.0.8", "99");
        ConnectionTester tester = new PdkConnectionTester(
                connectorId -> ref, Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC));

        assertThatThrownBy(() -> tester.test(config()))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.API_LEVEL_INCOMPATIBLE));
    }

    @Test
    void anUnrecognizedDeclaredVersionIsRefusedWithACodeNotABareCrash(@TempDir Path dir) {
        // A version the Tapstate-side level table has no row for is a registry gap, not a compatibility
        // verdict: the open path refuses it up front with a coded, version-naming diagnosis rather than
        // the bare IllegalStateException the strict level lookup would raise mid-load.
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.passingTest(dir)), "synthetic.PassingTest", "9.9.9", null);
        ConnectionTester tester = new PdkConnectionTester(
                connectorId -> ref, Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC));

        assertThatThrownBy(() -> tester.test(config()))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.API_LEVEL_UNKNOWN));
    }
}
