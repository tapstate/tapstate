package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResult.Outcome;
import io.tapstate.spi.store.ConnectionTester;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.TapTestItemException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The PDK implementation of the connection-test port: it provisions a connector, refuses it with a code
 * if it will not load or its declared API level is incompatible, drives the connector's own
 * {@code connectionTest} through the frozen PDK contract, and normalizes each streamed test item into a
 * {@link ConnectionTestItem}. The connector creates and releases its own connection inside
 * {@code connectionTest}, so the drive neither inits nor stops the node around it — cleanup is only
 * closing the connector's isolated class loader.
 *
 * <p>A connector that reports a failed check is a normal result whose overall {@link Outcome} is FAILED,
 * not an exception; a warning check never fails the outcome. Only a failure that prevents the test from
 * running — the connector cannot be loaded / level-gated, or throws out of {@code connectionTest} —
 * surfaces as a coded connector-domain exception. A connector's own per-item diagnostic code is carried
 * through verbatim as an opaque display string, taking no part in the first-party error-code system.
 * The PDK types stay inside this class; the port and the result carry none of them.
 */
public final class PdkConnectionTester implements ConnectionTester, AutoCloseable {

    private static final String CONNECTOR_PARAM = "connector";
    private static final String TIMEOUT_PARAM = "timeout";
    private final ConnectorProvisioner provisioner;
    private final Clock clock;
    private final java.util.function.Predicate<ConnectionConfig> managed;
    private final DefaultSourcePdkTest defaults;
    private final java.time.Duration defaultBudget;
    private final boolean ownsSlot;
    private volatile boolean closed;

    /** One parent node owns this slot across replacement business contexts. It is not persistent work. */
    public static final class DefaultSourceSlot implements AutoCloseable {
        private final DefaultSourcePdkTest tests;
        private final java.time.Duration budget;
        public DefaultSourceSlot(Clock clock) { this(clock, java.time.Duration.ofSeconds(60), java.time.Duration.ofSeconds(5)); }
        public DefaultSourceSlot(Clock clock, java.time.Duration budget, java.time.Duration cleanup) {
            this.budget = budget; tests = new DefaultSourcePdkTest(clock, budget, cleanup);
        }
        @Override public void close() { tests.close(); }
    }

    public PdkConnectionTester(ConnectorProvisioner provisioner, Clock clock) {
        this(provisioner, clock, ignored -> false);
    }
    public PdkConnectionTester(ConnectorProvisioner provisioner, Clock clock, java.util.function.Predicate<ConnectionConfig> managed) {
        this(provisioner, clock, managed, java.time.Duration.ofSeconds(60), java.time.Duration.ofSeconds(5));
    }
    PdkConnectionTester(ConnectorProvisioner provisioner, Clock clock, java.util.function.Predicate<ConnectionConfig> managed,
            java.time.Duration budget, java.time.Duration cleanupBudget) {
        this(provisioner, clock, managed, new DefaultSourceSlot(clock, budget, cleanupBudget), true);
    }
    public PdkConnectionTester(ConnectorProvisioner provisioner, Clock clock,
            java.util.function.Predicate<ConnectionConfig> managed, DefaultSourceSlot slot) {
        this(provisioner, clock, managed, slot, false);
    }
    private PdkConnectionTester(ConnectorProvisioner provisioner, Clock clock,
            java.util.function.Predicate<ConnectionConfig> managed, DefaultSourceSlot slot, boolean ownsSlot) {
        this.provisioner = java.util.Objects.requireNonNull(provisioner);
        this.clock = java.util.Objects.requireNonNull(clock);
        this.managed = java.util.Objects.requireNonNull(managed);
        this.defaults = java.util.Objects.requireNonNull(slot).tests;
        this.defaultBudget = slot.budget; this.ownsSlot = ownsSlot;
    }

    @Override
    public ConnectionTestResult test(ConnectionConfig config) {
        return testOrdinary(config);
    }
    @Override public ConnectionTestResult testDefault(ConnectionConfig config, long deadlineNanos) {
        if (closed) throw new TapstateException(ConnectorError.INSTANCES_BUSY, Map.of(CONNECTOR_PARAM, config.connectorId(), TIMEOUT_PARAM, "0s"), null);
        if (!managed.test(config)) return testOrdinary(config);
        return defaults.test(config, new DefaultCall(config, deadlineNanos), deadlineNanos);
    }
    public boolean defaultTestAvailable() { return !closed && defaults.available(); }
    @Override public void close() {
        closed = true;
        if (ownsSlot) defaults.close(); else defaults.cancelOwned(this);
    }

    private ConnectionTestResult testOrdinary(ConnectionConfig config) {
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), provisioner.resolve(config.connectorId()), config.settings());
        try {
            List<TestItem> raw = new ArrayList<>();
            drive(connector, raw::add, false);
            List<ConnectionTestItem> items = translate(raw);
            Outcome outcome = anyFailed(items) ? Outcome.FAILED : Outcome.PASSED;
            return new ConnectionTestResult(config.id(), config.connectorId(), outcome, items, clock.millis());
        } finally {
            connector.close();
        }
    }

    /**
     * Drives the connector's own connectionTest under its class loader, collecting each streamed item. A
     * connector that throws out of connectionTest could not complete the test — a coded connector-domain
     * failure, distinct from a connector that reports a failed check (which is a normal FAILED result).
     */
    private static void drive(PdkConnector connector, Consumer<TestItem> sink, boolean strict) {
        try {
            connector.underLoader(() -> {
                connector.connector().connectionTest(connector.context(), sink::accept);
                return null;
            });
        } catch (TapstateException e) {
            throw e;
        } catch (TapTestItemException diagnostic) {
            if (strict) { sink.accept(new TestItem("Connection", diagnostic, TestItem.RESULT_FAILED)); return; }
            throw new TapstateException(ConnectorError.TEST_FAILED,
                    Map.of(CONNECTOR_PARAM, connector.connectorId(), "detail", detail(diagnostic)), diagnostic);
        } catch (Throwable t) {
            if (strict && t instanceof RuntimeException runtime) throw runtime;
            if (strict && t instanceof Error error) throw error;
            if (t instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new TapstateException(ConnectorError.TEST_FAILED,
                    Map.of(CONNECTOR_PARAM, connector.connectorId(), "detail", detail(t)), t);
        }
    }

    private final class DefaultCall implements DefaultSourcePdkTest.Call {
        private final ConnectionConfig config;
        private final java.util.concurrent.atomic.AtomicBoolean closeClaimed = new java.util.concurrent.atomic.AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicReference<PdkConnector> connector = new java.util.concurrent.atomic.AtomicReference<>();
        private volatile boolean cancelled;
        private volatile boolean released;
        private volatile boolean cleanupUncertain;
        // Publishes the original defect before owned cleanup can block.
        private final java.util.concurrent.atomic.AtomicReference<Throwable> observedFailure = new java.util.concurrent.atomic.AtomicReference<>();
        private final long deadlineNanos;
        DefaultCall(ConnectionConfig config, long deadlineNanos) { this.config = config; this.deadlineNanos = deadlineNanos; }
        @Override public ConnectionTestResult run() {
            ConnectionTestResult result = null;
            Throwable failure = null;
            try {
                result = runOwnedTest();
            } catch (RuntimeException | Error original) {
                observedFailure.set(original);
                if (original instanceof TapstateException coded && coded.code() == ConnectorError.TLS_MATERIAL_CLEANUP_FAILED)
                    cleanupUncertain = true;
                failure = original;
            }
            var selected = closeAndSelectFailure(failure);
            if (selected instanceof RuntimeException runtime) throw runtime;
            if (selected instanceof Error error) throw error;
            return result;
        }
        private ConnectionTestResult runOwnedTest() {
            if (deadlineNanos - System.nanoTime() <= 0) throw new TapstateException(ConnectorError.TEST_TIMEOUT,
                    Map.of(CONNECTOR_PARAM, config.connectorId(), TIMEOUT_PARAM, defaultBudget.toString()), null);
            var ref = provisioner.resolveDefaultTest(config.connectorId());
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0 || cancelled) throw new TapstateException(ConnectorError.TEST_TIMEOUT,
                    Map.of(CONNECTOR_PARAM, config.connectorId(), TIMEOUT_PARAM, defaultBudget.toString()), null);
            var owned = PdkConnector.openDefaultTest(config.connectorId(), ref, defaultSettings(config, remaining));
            connector.set(owned);
            if (cancelled || Thread.currentThread().isInterrupted())
                throw new TapstateException(ConnectorError.TEST_TIMEOUT, Map.of(CONNECTOR_PARAM, config.connectorId(), TIMEOUT_PARAM, "cancelled"), null);
            var raw = new ArrayList<TestItem>();
            long[] diagnosticCharacters = {0};
            drive(owned, item -> collectDiagnostic(raw, diagnosticCharacters, item), true);
            var items = translate(raw);
            if (items.isEmpty()) throw new IllegalStateException("default Source connector returned no connection checks");
            return new ConnectionTestResult(config.id(), config.connectorId(), anyFailed(items) ? Outcome.FAILED : Outcome.PASSED,
                    items, clock.millis());
        }
        private void collectDiagnostic(List<TestItem> raw, long[] diagnosticCharacters, TestItem item) {
            if (raw.size() >= 1024) throw resultTooLarge(config.id());
            var failure = item.getTapTestItemException();
            countDiagnostic(config.id(), diagnosticCharacters, item.getItem(), item.getInformation(),
                    failure == null ? null : failure.getMessage(), failure == null ? null : failure.getReason(),
                    failure == null ? null : failure.getSolution(), failure == null ? null : failure.getErrorCode());
            if (failure != null && failure.getDynamicDescriptionParameters() != null) {
                int count = 0;
                for (String parameter : failure.getDynamicDescriptionParameters()) {
                    if (++count > 1024) throw resultTooLarge(config.id());
                    countDiagnostic(config.id(), diagnosticCharacters, parameter);
                }
            }
            raw.add(item);
        }
        private Throwable closeAndSelectFailure(Throwable original) {
            try {
                if (connector.get() == null) released = !cleanupUncertain;
                else closeOwned();
            } catch (RuntimeException | Error cleanup) {
                var selected = selectDefaultFailure(original, cleanup);
                observedFailure.set(selected); return selected;
            }
            return original;
        }
        @Override public void cancel() {
            cancelled = true;
            var owned = connector.get();
            if (owned == null) return;
            // connectionTest owns its connection lifecycle; stop is not valid before connector init.
            try { owned.cancelDrive(); }
            finally { closeOwned(); }
        }
        private void closeOwned() {
            var owned = connector.get();
            if (owned != null && closeClaimed.compareAndSet(false, true)) { owned.closeDefaultTest(); released = true; }
        }
        @Override public boolean resourcesReleased() { return released; }
        @Override public Throwable observedFailure() { return observedFailure.get(); }
        @Override public Object owner() { return PdkConnectionTester.this; }
    }

    /** Preserve both failures while keeping a cleanup defect visible over a controlled connector failure. */
    static Throwable selectDefaultFailure(Throwable original, Throwable cleanup) {
        if (original == null || original == cleanup) return cleanup;
        if (original instanceof TapstateException && !(cleanup instanceof TapstateException)) {
            cleanup.addSuppressed(original); return cleanup;
        }
        original.addSuppressed(cleanup); return original;
    }

    private static void countDiagnostic(String id, long[] count, String... values) {
        for (String value : values) if (value != null) {
            count[0] += value.length();
            if (value.length() > 65_536 || count[0] > 131_072) throw resultTooLarge(id);
        }
    }
    private static TapstateException resultTooLarge(String id) {
        return new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), null);
    }

    private static Map<String,Object> defaultSettings(ConnectionConfig config, long remainingNanos) {
        if (!java.util.Set.of("mongodb", "mongodb-atlas").contains(config.connectorId())
                || !(config.settings().get("uri") instanceof String uri)) return config.settings();
        long millis = Math.max(1, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos));
        int query = uri.indexOf('?');
        String base = query < 0 ? uri : uri.substring(0, query);
        if (base.indexOf('/', base.indexOf("://") + 3) < 0) base += "/";
        var limits = new java.util.LinkedHashMap<String,Long>();
        for (String key : List.of("serverselectiontimeoutms", "connecttimeoutms", "sockettimeoutms")) limits.put(key, millis);
        var options = new ArrayList<String>();
        if (query >= 0) for (String entry : uri.substring(query + 1).split("[&;]")) {
            String[] pair = entry.split("=", 2);
            String key = java.net.URLDecoder.decode(pair[0], java.nio.charset.StandardCharsets.UTF_8).toLowerCase(java.util.Locale.ROOT);
            if (limits.containsKey(key) && pair.length == 2) {
                try {
                    long original = Long.parseLong(java.net.URLDecoder.decode(pair[1], java.nio.charset.StandardCharsets.UTF_8));
                    if (original > 0) limits.put(key, Math.min(original, millis));
                } catch (NumberFormatException invalid) { options.add(entry); limits.remove(key); }
            } else options.add(entry);
        }
        limits.forEach((key, value) -> options.add(key + "=" + value));
        var settings = new java.util.LinkedHashMap<String,Object>(config.settings());
        settings.put("uri", base + "?" + String.join("&", options));
        return settings;
    }

    private static List<ConnectionTestItem> translate(List<TestItem> raw) {
        List<ConnectionTestItem> items = new ArrayList<>(raw.size());
        for (TestItem item : raw) {
            items.add(translate(item));
        }
        return items;
    }

    /**
     * Normalizes one PDK test item. Diagnostics come from the item's structured exception when present,
     * falling back to its free-text information for the message; the exception's own code is passed
     * through as the opaque connector-error-code display string.
     */
    private static ConnectionTestItem translate(TestItem item) {
        TapTestItemException ex = item.getTapTestItemException();
        String message = firstNonBlank(
                firstNonBlank(ex != null ? ex.getMessage() : null, item.getInformation()),
                attempted(ex));
        String reason = ex != null ? ex.getReason() : null;
        String solution = ex != null ? ex.getSolution() : null;
        String connectorErrorCode = ex != null ? ex.getErrorCode() : null;
        return new ConnectionTestItem(item.getItem(), status(item.getResult()),
                message, reason, solution, connectorErrorCode);
    }

    /**
     * Maps the connector's result int to a status. The frozen contract defines exactly three values
     * (success / success-with-warning / failed); any other value is treated as a failure, since only the
     * two success codes count as a pass.
     */
    private static ConnectionTestItem.Status status(int result) {
        return switch (result) {
            case TestItem.RESULT_SUCCESSFULLY -> ConnectionTestItem.Status.PASSED;
            case TestItem.RESULT_SUCCESSFULLY_WITH_WARN -> ConnectionTestItem.Status.WARNING;
            default -> ConnectionTestItem.Status.FAILED;
        };
    }

    /** The overall test fails when any check failed; a warning alone does not fail it. */
    private static boolean anyFailed(List<ConnectionTestItem> items) {
        for (ConnectionTestItem item : items) {
            if (item.status() == ConnectionTestItem.Status.FAILED) {
                return true;
            }
        }
        return false;
    }

    /**
     * What the connector reports having tried, as a last resort for a check that carried no message.
     *
     * <p>A connector may report a check with nothing but a numeric code and the statements behind it —
     * the postgres connector reports a failed replication-slot probe that way, and that probe is what
     * decides whether change capture can work at all. Read literally such an item has nothing to show
     * but a number, and a number alone tells a reader neither what happened nor what to do. The
     * statements are the only thing present that carries meaning, so they become the message rather
     * than being dropped.
     */
    private static String attempted(TapTestItemException ex) {
        if (ex == null || ex.getDynamicDescriptionParameters() == null) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        for (String parameter : ex.getDynamicDescriptionParameters()) {
            if (parameter != null && !parameter.isBlank()) {
                parts.add(parameter.trim());
            }
        }
        return parts.isEmpty() ? null : String.join("; ", parts);
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) {
            return a;
        }
        if (b != null && !b.isBlank()) {
            return b;
        }
        return null;
    }

    private static String detail(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }
}
