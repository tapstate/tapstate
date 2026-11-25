package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * What a PostgreSQL source database holds for a change stream reading it: its replication slots, and how far
 * each one's reader has confirmed it may let go of the log. Read with the harness's own connection, never
 * through the product, because the product saying where a slot is would be the claim these cases check.
 *
 * <p>Every reading is for one database. The server is shared by every PostgreSQL case in the JVM and a slot
 * outlives the stream that made it, so a reading across the whole server would be measuring the other
 * cases.
 *
 * <p>Positions are compared by the server, as {@code pg_lsn} values. A log sequence number is written as two
 * hexadecimal halves, and comparing the text puts {@code 0/A0000000} after {@code 1/0}; the server's own
 * comparison is the only one that cannot be spelled wrong.
 */
final class PostgresSlots {

    private PostgresSlots() {
    }

    /** One replication slot on the database: its name, whether a reader holds it, how far it is confirmed. */
    record Slot(String name, boolean active, String confirmedFlushLsn) {
    }

    /** The database's replication slots, by name. */
    static List<Slot> of(Map<String, Object> settings) {
        List<Slot> slots = new ArrayList<>();
        try (Connection connection = SharedPostgres.connect(settings);
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT slot_name, active, confirmed_flush_lsn::text FROM pg_replication_slots"
                                + " WHERE database = ? ORDER BY slot_name")) {
            statement.setString(1, String.valueOf(settings.get("database")));
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    slots.add(new Slot(rows.getString(1), rows.getBoolean(2), rows.getString(3)));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot read the replication slots of " + settings.get("database"), e);
        }
        return List.copyOf(slots);
    }

    /** The names of the database's slots. */
    static List<String> names(Map<String, Object> settings) {
        return of(settings).stream().map(Slot::name).toList();
    }

    /** The database's one slot, failing when it holds none or several. */
    static Slot only(Map<String, Object> settings) {
        List<Slot> slots = of(settings);
        if (slots.size() != 1) {
            throw new AssertionError("expected exactly one replication slot on " + settings.get("database")
                    + ", found " + slots);
        }
        return slots.getFirst();
    }

    /** Whether the database's slot is held by a reader. */
    static boolean active(Map<String, Object> settings) {
        return of(settings).stream().anyMatch(Slot::active);
    }

    /**
     * Where the server will write its next log record, read on {@code connection} -- inside a transaction
     * before its commit, this is a position every change of that transaction lies at or before.
     */
    static String insertPosition(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("SELECT pg_current_wal_insert_lsn()::text")) {
            row.next();
            return row.getString(1);
        }
    }

    /** How far the server has written its log, across every database on it: only ever an upper bound here. */
    static String writtenPosition(Map<String, Object> settings) {
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("SELECT pg_current_wal_lsn()::text")) {
            row.next();
            return row.getString(1);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read where the server's log is", e);
        }
    }

    /** Whether the database's one slot has been confirmed at or past {@code position}. */
    static boolean confirmedAtOrPast(Map<String, Object> settings, String position) {
        return compare(settings, "confirmed_flush_lsn >= ?::pg_lsn", position);
    }

    /** Whether the database's one slot has been confirmed no further than {@code position}. */
    static boolean confirmedAtOrBefore(Map<String, Object> settings, String position) {
        return compare(settings, "confirmed_flush_lsn <= ?::pg_lsn", position);
    }

    /**
     * How far behind the reader of the database's slot is in flushing, in seconds, as the server measures
     * it; empty while no reader holds the slot or the server has not measured one yet.
     */
    static Optional<Double> flushLagSeconds(Map<String, Object> settings) {
        try (Connection connection = SharedPostgres.connect(settings);
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT EXTRACT(EPOCH FROM r.flush_lag) FROM pg_stat_replication r"
                                + " JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.database = ?")) {
            statement.setString(1, String.valueOf(settings.get("database")));
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                double seconds = row.getDouble(1);
                return row.wasNull() ? Optional.empty() : Optional.of(seconds);
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot read the replication lag of " + settings.get("database"), e);
        }
    }

    /** The server's own clock, for a moment the cases compare the server's other readings against. */
    static String now(Map<String, Object> settings) {
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("SELECT now()::text")) {
            row.next();
            return row.getString(1);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read the server's clock", e);
        }
    }

    /**
     * Whether the server's own clock has run more than {@code seconds} past {@code moment}, a reading of
     * {@link #now}. What a slot shows is only ever as fresh as the last time its reader told the server how
     * far it had flushed, so a case asserting a slot has not moved first lets that much of the server's time
     * go by. Both times are the server's: the reader's own clock, which is what the server records for its
     * replies, is not comparable with it.
     */
    static boolean serverTimePassed(Map<String, Object> settings, String moment, int seconds) {
        try (Connection connection = SharedPostgres.connect(settings);
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT now() > ?::timestamptz + (?::int * interval '1 second')")) {
            statement.setString(1, moment);
            statement.setInt(2, seconds);
            try (ResultSet row = statement.executeQuery()) {
                row.next();
                return row.getBoolean(1);
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot read the server's clock", e);
        }
    }

    private static boolean compare(Map<String, Object> settings, String condition, String position) {
        try (Connection connection = SharedPostgres.connect(settings);
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT " + condition + " FROM pg_replication_slots WHERE database = ?")) {
            statement.setString(1, position);
            statement.setString(2, String.valueOf(settings.get("database")));
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new AssertionError("no replication slot on " + settings.get("database"));
                }
                boolean answer = rows.getBoolean(1);
                if (rows.next()) {
                    throw new AssertionError("more than one replication slot on " + settings.get("database"));
                }
                return answer;
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("cannot compare the slot on " + settings.get("database"), e);
        }
    }
}
