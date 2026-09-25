package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** A fork's live source identity, sampled before writes and checked again after terminal ACK. */
final class BenchmarkSourceLineage {

    private BenchmarkSourceLineage() {
    }

    record Address(String host, int port, String database) {
        Address {
            if (host == null || host.isBlank() || port <= 0 || port > 65_535
                    || database == null || database.isBlank()) {
                throw new IllegalArgumentException("incomplete benchmark source address");
            }
        }
    }

    sealed interface Witness permits MySql, Postgres {
        Address address();
    }

    record MySql(Address address, String serverUuid, long serverId) implements Witness {
        MySql {
            if (address == null || serverUuid == null || serverId <= 0) {
                throw new IllegalArgumentException("incomplete MySQL source lineage");
            }
            serverUuid = UUID.fromString(serverUuid).toString();
        }

        BenchmarkConnectorPositionCoverage.MySqlSourceLineage decoderLineage() {
            return new BenchmarkConnectorPositionCoverage.MySqlSourceLineage(
                    address.host(), address.port(), address.database(), serverUuid, serverId);
        }
    }

    record Postgres(Address address, String systemIdentifier, int timeline) implements Witness {
        Postgres {
            if (address == null || systemIdentifier == null || timeline <= 0) {
                throw new IllegalArgumentException("incomplete PostgreSQL source lineage");
            }
            systemIdentifier = normalizeUnsignedSystemIdentifier(systemIdentifier);
        }
    }

    static MySql readMySql(Map<String, Object> sourceSettings) {
        Address address = address(sourceSettings);
        try (Connection connection = SharedMySql.connect(sourceSettings);
                Statement query = connection.createStatement();
                ResultSet row = query.executeQuery("SELECT @@server_uuid, @@server_id")) {
            if (!row.next()) {
                throw new AssertionError("MySQL source returned no server identity");
            }
            String uuid = row.getString(1);
            long serverId = row.getLong(2);
            if (row.wasNull() || row.next()) {
                throw new AssertionError("MySQL source returned an invalid server identity");
            }
            return new MySql(address, uuid, serverId);
        } catch (SQLException failure) {
            throw new AssertionError("cannot read MySQL source lineage", failure);
        }
    }

    static Postgres readPostgres(Map<String, Object> sourceSettings) {
        Address address = address(sourceSettings);
        Map<String, Object> connectionSettings = new LinkedHashMap<>(sourceSettings);
        Object user = sourceSettings.get("user");
        if (user != null) {
            if (sourceSettings.containsKey("username") && !user.equals(sourceSettings.get("username"))) {
                throw new IllegalArgumentException("PostgreSQL source user settings disagree");
            }
            connectionSettings.put("username", user);
        }
        try (Connection connection = SharedPostgres.connect(connectionSettings);
                Statement query = connection.createStatement()) {
            String systemIdentifier;
            try (ResultSet system = query.executeQuery("SELECT system_identifier FROM pg_control_system()")) {
                if (!system.next()) {
                    throw new AssertionError("PostgreSQL source returned no system identifier");
                }
                systemIdentifier = system.getString(1);
                if (system.next()) {
                    throw new AssertionError("PostgreSQL source returned multiple system identifiers");
                }
            }
            int timeline;
            try (ResultSet checkpoint = query.executeQuery("SELECT timeline_id FROM pg_control_checkpoint()")) {
                if (!checkpoint.next()) {
                    throw new AssertionError("PostgreSQL source returned no timeline");
                }
                timeline = checkpoint.getInt(1);
                if (checkpoint.wasNull() || checkpoint.next()) {
                    throw new AssertionError("PostgreSQL source returned an invalid timeline");
                }
            }
            return new Postgres(address, systemIdentifier, timeline);
        } catch (SQLException failure) {
            throw new AssertionError("cannot read PostgreSQL source lineage", failure);
        }
    }

    /** Re-queries the configured source after target ACK, rejecting any identity or address change. */
    static void verifyAfterTerminalAck(Witness expected, Map<String, Object> sourceSettings) {
        if (expected == null) {
            throw new IllegalArgumentException("a sampled source lineage is required");
        }
        Witness current = expected instanceof MySql ? readMySql(sourceSettings) : readPostgres(sourceSettings);
        if (!expected.equals(current)) {
            throw new AssertionError("benchmark source lineage changed before terminal ACK validation");
        }
    }

    private static Address address(Map<String, Object> settings) {
        if (settings == null || !(settings.get("host") instanceof String host)
                || !(settings.get("port") instanceof Number port)
                || !(port instanceof Integer || port instanceof Long)
                || port.longValue() < 1 || port.longValue() > 65_535
                || !(settings.get("database") instanceof String database)) {
            throw new IllegalArgumentException("benchmark source settings have no valid address");
        }
        return new Address(host, port.intValue(), database);
    }

    private static String normalizeUnsignedSystemIdentifier(String value) {
        if (!value.matches("-?[0-9]+")) {
            throw new IllegalArgumentException("invalid PostgreSQL system identifier");
        }
        try {
            long bits = value.charAt(0) == '-' ? Long.parseLong(value) : Long.parseUnsignedLong(value);
            return Long.toUnsignedString(bits);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("PostgreSQL system identifier exceeds 64 bits", invalid);
        }
    }
}
