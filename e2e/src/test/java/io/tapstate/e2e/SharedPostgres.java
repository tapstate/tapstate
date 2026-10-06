package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One PostgreSQL server for every specification in the JVM, the arrangement {@link SharedMySql} and
 * {@link SharedMongo} already make and for the same reason: a container per test class costs a start-up
 * each time and buys nothing, because runs stay independent by taking a database of their own rather
 * than a daemon of their own. Ryuk reaps the container when the JVM exits, so there is no stop to forget.
 *
 * <p>Started with logical decoding on, which is the one setting that cannot be added later. The image
 * ships {@code wal_level=replica}: a write-ahead log physical replication can follow and logical decoding
 * cannot. A change-data-capture read against that server does not fail where it is configured - it fails
 * much later, when the connector asks for a replication slot, and only the streaming half fails, so the
 * snapshot half of the same specification passes. Turning it on here means a case never meets that
 * half-working state.
 *
 * <p>Two settings differ from MySQL's arrangement and are worth naming, because both are the kind of
 * thing that reads as a connector defect when it is really a server that was never asked for it:
 * replication needs a sender slot per stream, and this is one server per JVM shared by every postgres
 * case, whose slots are not dropped when a stream stops. Both settings boot at 10 on this image, which a
 * full pass exhausts; the ceiling is raised rather than merely restated, and exhaustion surfaces as
 * {@code all replication slots are in use} rather than as anything naming the server.
 *
 * <p>Unlike MySQL, no privilege grant is needed. The image's own superuser is the account the harness
 * connects as, and it already carries the replication attribute; the MySQL equivalent exists only
 * because that image's default test user does not.
 */
final class SharedPostgres {

    private static final DockerImageName IMAGE = DockerImageName.parse("postgres:16");

    private static PostgreSQLContainer<?> container;

    private SharedPostgres() {
    }

    private static final Map<String, Integer> ownedDatabases = new LinkedHashMap<>();
    private static final java.util.Set<String> unownedDatabases = new java.util.HashSet<>();
    private static final int MAX_OWNED_SLOT_ROWS = 32;

    /** One explicit fixture lifetime; stream stop and an intermediate restart do not end it. */
    static Fixture fixture() { return new Fixture(); }

    record SlotCleanup(String database, String status, java.util.List<String> dropped, java.util.List<String> retained) {
        SlotCleanup { dropped = java.util.List.copyOf(dropped); retained = java.util.List.copyOf(retained); }
    }

    static final class Closing implements AutoCloseable {
        private final String label;
        private AutoCloseable resource;
        private java.util.function.BooleanSupplier terminal;
        private boolean attempted, confirmed;
        Closing(String label, AutoCloseable resource, java.util.function.BooleanSupplier terminal) {
            this.label = label; this.resource = resource; this.terminal = terminal;
        }
        @Override public void close() throws Exception {
            if (attempted) { return; }
            attempted = true;
            if (resource == null) { return; }
            resource.close();
            confirmed = !Thread.currentThread().isInterrupted() && terminal.getAsBoolean();
        }
    }

    static final class Fixture implements AutoCloseable {
        private final java.util.Set<String> databases = new java.util.LinkedHashSet<>();
        private final java.util.List<Closing> resources = new java.util.ArrayList<>();
        private final java.util.List<SlotCleanup> receipts = new java.util.ArrayList<>();
        private boolean closed;

        Map<String, Object> settings(String database) {
            synchronized (SharedPostgres.class) {
                if (closed) { throw new AssertionError("a closed PostgreSQL fixture cannot acquire a database"); }
                Map<String, Object> settings = settingsFor(database);
                if (databases.add(database)) { ownedDatabases.merge(database, 1, Math::addExact); }
                return settings;
            }
        }

        Closing track(String label, AutoCloseable resource, java.util.function.BooleanSupplier terminal) {
            if (closed || resources.size() >= 64) { throw new AssertionError("invalid PostgreSQL fixture resource registration"); }
            Closing closing = new Closing(java.util.Objects.requireNonNull(label), java.util.Objects.requireNonNull(resource),
                    java.util.Objects.requireNonNull(terminal));
            resources.add(closing); return closing;
        }

        Closing pending(String label) {
            if (closed || resources.size() >= 64) { throw new AssertionError("invalid PostgreSQL fixture resource registration"); }
            Closing closing = new Closing(label, null, () -> false); resources.add(closing); return closing;
        }

        void bind(Closing closing, AutoCloseable resource, java.util.function.BooleanSupplier terminal) {
            if (!resources.contains(closing) || closing.resource != null || closing.attempted) {
                throw new AssertionError("an owned close ticket cannot be rebound");
            }
            closing.resource = java.util.Objects.requireNonNull(resource);
            closing.terminal = java.util.Objects.requireNonNull(terminal);
        }

        ServerHandle launch(java.util.function.Supplier<ServerHandle> launcher) {
            Closing closing = pending("owned server launch");
            ServerHandle server = launcher.get();
            bind(closing, server, server::terminated);
            return new ServerHandle() {
                @Override public java.net.URI baseUrl() { return server.baseUrl(); }
                @Override public boolean terminated() { return closing.confirmed; }
                @Override public void close() {
                    try { closing.close(); }
                    catch (RuntimeException | Error failure) { throw failure; }
                    catch (Exception failure) { throw new AssertionError("the owned fixture server close failed", failure); }
                }
            };
        }

        java.util.List<SlotCleanup> receipts() { return java.util.List.copyOf(receipts); }

        @Override public void close() {
            if (closed) { return; }
            closed = true;
            java.util.List<String> incomplete = resources.stream().filter(resource -> !resource.confirmed)
                    .map(resource -> resource.label).toList();
            boolean trackedSource = resources.stream().anyMatch(resource -> resource.resource instanceof ServerHandle
                    || resource.resource instanceof BenchmarkForkEnvironment.OwnedBoot);
            long settleDeadline = incomplete.isEmpty() && trackedSource
                    ? System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos() : 0;
            synchronized (SharedPostgres.class) {
                for (String database : databases) {
                    if (!incomplete.isEmpty()) {
                        receipts.add(new SlotCleanup(database, "OWNER_END_UNCONFIRMED", java.util.List.of(), incomplete));
                        System.err.println("PostgreSQL fixture keeps slots for " + database + ": owner end unconfirmed " + incomplete);
                        continue;
                    }
                    Integer owners = ownedDatabases.get(database);
                    if (owners == null || owners < 1) { throw new AssertionError("PostgreSQL fixture owner accounting is missing"); }
                    if (owners > 1) {
                        ownedDatabases.put(database, owners - 1);
                        receipts.add(new SlotCleanup(database, "BORROWER_REMAINS", java.util.List.of(), java.util.List.of()));
                    } else {
                        ownedDatabases.remove(database);
                        if (unownedDatabases.contains(database)) {
                            receipts.add(new SlotCleanup(database, "UNSCOPED_BORROWER_UNCONFIRMED", java.util.List.of(), java.util.List.of()));
                            System.err.println("PostgreSQL fixture keeps slots for " + database + ": unscoped caller was registered");
                        } else { receipts.add(cleanupOwnedSlots(database, settleDeadline)); }
                    }
                }
            }
        }
    }

    /** Exact-database cleanup follows confirmed fixture end, never a global inactive-slot heuristic. */
    private static SlotCleanup cleanupOwnedSlots(String database, long settleDeadline) {
        java.util.List<String> dropped = new java.util.ArrayList<>(), retained = new java.util.ArrayList<>();
        try (Connection admin = asAdmin(server());
                var query = admin.prepareStatement("SELECT slot_name, active FROM pg_replication_slots "
                        + "WHERE database = ? AND slot_type = 'logical' ORDER BY slot_name")) {
            query.setString(1, database); query.setQueryTimeout(10); query.setMaxRows(MAX_OWNED_SLOT_ROWS + 1);
            java.util.List<Map.Entry<String, Boolean>> slots;
            if (settleDeadline == 0) { slots = ownedSlotInventory(query); }
            else {
                long started = System.nanoTime();
                var observed = new java.util.concurrent.atomic.AtomicReference<java.util.List<Map.Entry<String, Boolean>>>(java.util.List.of());
                var polls = new java.util.concurrent.atomic.AtomicInteger();
                var firstActive = new java.util.concurrent.atomic.AtomicLong();
                var firstInactive = new java.util.concurrent.atomic.AtomicLong();
                try {
                    long left = settleDeadline - System.nanoTime();
                    if (left <= 0) { throw new AssertionError("owned PostgreSQL fixture cleanup deadline expired for " + database); }
                    Await.until("confirmed owned PostgreSQL source backend to release " + database,
                            java.time.Duration.ofNanos(left), () -> {
                                long remaining = settleDeadline - System.nanoTime();
                                if (remaining <= 0) { return false; }
                                try {
                                    query.setQueryTimeout((int) Math.max(1, Math.min(5,
                                            (remaining + 999_999_999L) / 1_000_000_000L)));
                                    var reading = ownedSlotInventory(query); observed.set(reading); polls.incrementAndGet();
                                    long observedAt = System.nanoTime();
                                    boolean inactive = reading.stream().noneMatch(Map.Entry::getValue);
                                    if (inactive) { firstInactive.compareAndSet(0, observedAt); }
                                    else { firstActive.compareAndSet(0, observedAt); }
                                    return inactive && observedAt < settleDeadline;
                                } catch (SQLException failure) { throw new AssertionError("cannot read owned PostgreSQL close inventory for " + database, failure); }
                            }, () -> "active owned slots=" + observed.get().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());
                } finally {
                    System.err.println("PostgreSQL fixture close inventory for " + database + ": elapsedNanos="
                            + (System.nanoTime() - started) + ", polls=" + polls.get() + ", active="
                            + observed.get().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList()
                            + ", firstActiveObservedNanos=" + firstActive.get() + ", firstInactiveObservedNanos=" + firstInactive.get()
                            + ", observedReleaseIntervalNanos=" + (firstActive.get() != 0 && firstInactive.get() != 0
                                    ? firstInactive.get() - firstActive.get() : "UNAVAILABLE"));
                }
                slots = observed.get();
            }
            for (var slot : slots) {
                if (slot.getValue()) { retained.add(slot.getKey()); continue; }
                try (var drop = admin.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                    drop.setString(1, slot.getKey()); drop.setQueryTimeout(10); drop.execute(); dropped.add(slot.getKey());
                } catch (SQLException changed) {
                    // PostgreSQL atomically refuses a slot that became active after the inventory read.
                    if (!"55006".equals(changed.getSQLState())) { throw changed; }
                    retained.add(slot.getKey());
                }
            }
            if (!retained.isEmpty()) {
                System.err.println("PostgreSQL fixture preserves active owned slots for " + database + ": " + retained);
            }
            return new SlotCleanup(database, retained.isEmpty() ? "RELEASED" : "ACTIVE_SLOTS_RETAINED", dropped, retained);
        } catch (SQLException failure) { throw new AssertionError("cannot reclaim ended PostgreSQL fixture slots for " + database, failure); }
    }


    private static java.util.List<Map.Entry<String, Boolean>> ownedSlotInventory(java.sql.PreparedStatement query) throws SQLException {
        java.util.List<Map.Entry<String, Boolean>> slots = new java.util.ArrayList<>();
        try (var rows = query.executeQuery()) {
            while (rows.next()) { slots.add(Map.entry(rows.getString(1), rows.getBoolean(2))); }
        }
        if (slots.size() > MAX_OWNED_SLOT_ROWS) { throw new AssertionError("owned PostgreSQL slot inventory exceeded its fixed bound"); }
        return slots;
    }

    /**
     * The settings addressing a database of the caller's own on the shared server, spelled the way a
     * resource spells them - host, port, database, username, password, no one of which is the address.
     */
    static synchronized Map<String, Object> settings(String database) {
        unownedDatabases.add(database);
        return settingsFor(database);
    }

    private static Map<String, Object> settingsFor(String database) {
        PostgreSQLContainer<?> server = server();
        provision(server, database);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", server.getHost());
        settings.put("port", server.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        settings.put("database", database);
        settings.put("username", server.getUsername());
        settings.put("password", server.getPassword());
        return settings;
    }

    /**
     * A connection to the database those settings address, for a test laying down a fixture of its own.
     *
     * <p>Deliberately not the path {@link PostgresEndpoints} takes, for the reason its MySQL counterpart
     * gives: that driver builds its address out of the resource, because what it dials has to be what the
     * product was given. This one is plumbing for a test that already holds the settings.
     */
    static Connection connect(Map<String, Object> settings) throws SQLException {
        String url = "jdbc:postgresql://" + settings.get("host") + ":" + settings.get("port") + "/"
                + settings.get("database");
        return DriverManager.getConnection(
                url, String.valueOf(settings.get("username")), String.valueOf(settings.get("password")));
    }

    private static PostgreSQLContainer<?> server() {
        if (container == null) {
            DockerGate.require();
            PostgreSQLContainer<?> starting = new PostgreSQLContainer<>(IMAGE)
                    .withCommand("postgres",
                            // withCommand replaces the command wholesale, so the image default this
                            // wrapper otherwise supplies has to be restated here or every write on this
                            // server waits on a real disk flush.
                            "-c", "fsync=off",
                            "-c", "wal_level=logical",
                            "-c", "max_wal_senders=32",
                            "-c", "max_replication_slots=32");
            starting.start();
            container = starting;
        }
        return container;
    }

    /**
     * Creates the database if it is not already there.
     *
     * <p>Two queries rather than one, because PostgreSQL has no {@code CREATE DATABASE IF NOT EXISTS}.
     * Nothing guards the gap between them and nothing needs to: the container is one per JVM and the
     * only caller is synchronized, so there is no second writer to lose a race with. A tolerance for
     * "already exists" was written here first and taken out again - it could not be reached, and
     * untested code for an unreachable case is worse than no code, because the next reader has to work
     * out whether the case is real.
     */
    private static void provision(PostgreSQLContainer<?> server, String database) {
        try (Connection admin = asAdmin(server); Statement statement = admin.createStatement()) {
            try (var existing = statement.executeQuery(
                    "SELECT 1 FROM pg_database WHERE datname = '" + database + "'")) {
                if (existing.next()) {
                    return;
                }
            }
            statement.execute("CREATE DATABASE \"" + database + "\"");
        } catch (SQLException e) {
            throw new IllegalStateException("cannot provision the database " + database, e);
        }
    }

    private static Connection asAdmin(PostgreSQLContainer<?> server) throws SQLException {
        return DriverManager.getConnection(
                server.getJdbcUrl(), server.getUsername(), server.getPassword());
    }
}
