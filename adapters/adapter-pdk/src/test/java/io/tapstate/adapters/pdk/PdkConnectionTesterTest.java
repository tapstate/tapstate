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
        assertThat(login.diagnostic()).isEmpty();
    }

    @Test
    void anIsolatedAssemblyCapturesTheChecksWithoutChangingTheirOutcome(@TempDir Path dir) {
        var ref = new ConnectorRef(List.of(Synthetic.failingTest(dir)), "synthetic.FailingTest", "2.0.8", null);
        var clock = Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC);
        try (var slot = new PdkConnectionTester.DefaultSourceSlot(clock);
             var tester = new PdkConnectionTester(ignored -> ref, clock, ignored -> false, slot, () -> java.util.Locale.ENGLISH)) {
            var result = tester.test(config());
            assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
            assertThat(result.items().getFirst().diagnostic()).isEmpty();
            assertThat(result.items().getLast().diagnostic()).containsEntry("code", "28000")
                    .containsEntry("message", "Login denied").containsEntry("solution", "check username/password");
        }
    }

    @Test
    void thrownAuthenticationAndRetryableConnectionRefusalsBecomeFailedChecks(@TempDir Path dir) {
        var jar = rejectionFixture(dir);
        var ref = new ConnectorRef(List.of(jar), "synthetic.ConnectionRefusal", "2.0.8", null);
        var clock = Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC);
        try (var slot = new PdkConnectionTester.DefaultSourceSlot(clock);
             var tester = new PdkConnectionTester(ignored -> ref, clock, ignored -> false, slot, () -> java.util.Locale.ENGLISH)) {
            assertRefusal(tester, "wrapped-auth", "10002", "28P01");
            assertRefusal(tester, "28P01", "28P01", null);
            assertRefusal(tester, "28000", "28000", null);
            assertRefusal(tester, "08001", "10012", "08001");
            assertRefusal(tester, "08004", "10012", "08004");
            assertRefusal(tester, "network", "10012", null);
            assertRefusal(tester, "tls", "10012", null);

            assertUnexpectedFailure(tester, "08006");
            assertUnexpectedFailure(tester, "suppressed");
            assertUnexpectedFailure(tester, "untyped");
            assertUnexpectedFailure(tester, "missing-driver");
        }
    }

    private static void assertRefusal(ConnectionTester tester, String scenario, String code, String causeCode) {
        var result = tester.test(new ConnectionConfig("conn-1", "demo", Map.of("scenario", scenario)));
        assertThat(result.outcome()).as(scenario).isEqualTo(Outcome.FAILED);
        assertThat(result.items()).singleElement().satisfies(item -> {
            assertThat(item.status()).isEqualTo(Status.FAILED);
            assertThat(item.connectorErrorCode()).isEqualTo(code);
            var root = item.diagnostic();
            assertThat(root).containsEntry("code", code).containsEntry("message", "driver refusal");
            if (root.get("cause") instanceof Map<?, ?> cause) {
                assertThat(cause.get("code")).as(scenario + " direct cause code").isEqualTo(causeCode);
                assertThat(cause.containsKey("cause")).isFalse();
                assertThat(cause.get("rawMessage")).isEqualTo("driver refusal");
            } else {
                assertThat(causeCode).as(scenario + " expected cause code").isNull();
                assertThat(root).containsEntry("rawMessage", "driver refusal");
            }
        });
    }

    private static void assertUnexpectedFailure(ConnectionTester tester, String scenario) {
        var config = new ConnectionConfig("conn-1", "demo", Map.of("scenario", scenario));
        assertThatThrownBy(() -> tester.test(config)).as(scenario).isInstanceOfSatisfying(TapstateException.class,
                exception -> assertThat(exception.code()).isEqualTo(ConnectorError.TEST_FAILED));
    }

    private static Path rejectionFixture(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.ConnectionRefusal", """
                package synthetic;

                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.schema.TapTable;
                import io.tapdata.exception.TapPdkRetryableEx;
                import io.tapdata.exception.TapPdkUserPwdInvalidEx;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import java.net.ConnectException;
                import java.sql.SQLException;
                import java.util.List;
                import java.util.function.Consumer;
                import javax.net.ssl.SSLException;

                public class ConnectionRefusal implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {}
                    public void init(TapConnectionContext context) {}
                    public void stop(TapConnectionContext context) {}
                    public void discoverSchema(TapConnectionContext c, List<String> tables, int size, Consumer<List<TapTable>> sink) {}
                    public int tableCount(TapConnectionContext context) { return 0; }

                    public ConnectionOptions connectionTest(TapConnectionContext ctx, Consumer<TestItem> sink) {
                        String scenario = (String) ctx.getConnectionConfig().get("scenario");
                        String driverMessage = "driver refusal";
                        throw switch (scenario) {
                            case "wrapped-auth" -> new RuntimeException(new TapPdkUserPwdInvalidEx("4000", "authentication",
                                    new SQLException(driverMessage, "28P01")));
                            case "08001", "08004" -> new TapPdkRetryableEx("4001", new SQLException(driverMessage, scenario));
                            case "network" -> new TapPdkRetryableEx("4001", new ConnectException(driverMessage));
                            case "tls" -> new TapPdkRetryableEx("4001", new SSLException(driverMessage));
                            case "suppressed" -> {
                                var driver = new SQLException(driverMessage, "28P01");
                                driver.addSuppressed(new IllegalStateException("cleanup failed"));
                                yield new RuntimeException(driver);
                            }
                            case "untyped" -> new RuntimeException("wrapper without a cause");
                            case "missing-driver" -> new TapPdkRetryableEx("4001", (Throwable) null);
                            default -> new RuntimeException(new SQLException(driverMessage, scenario));
                        };
                    }
                }
                """);
    }

    @Test
    void aNestedDriverCauseSurvivesWhileOversizedSensitiveTextIsOmittedBeforeRedaction() throws Exception {
        var cause = new java.sql.SQLException("driver rejected the account", "28P01");
        var failure = new io.tapdata.pdk.apis.exception.TapTestItemException();
        failure.initCause(cause);
        var empty = PdkDiagnosticCatalog.read("demo",
                "{\"formatVersion\":1,\"connectorId\":\"demo\",\"definitions\":[]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var projection = PdkConnectionDiagnostic.capture(failure, "x".repeat(4090) + "sensitive-value",
                "Review credentials", "28000", empty, java.util.Locale.ENGLISH);
        assertThat(projection).containsEntry("code", "28000").containsEntry("truncated", true)
                .doesNotContainKey("message");
        assertThat(projection.get("cause").toString()).contains("28P01", "driver rejected the account");
        assertThat(projection.toString()).doesNotContain("sensitive-value", "sensit");
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
