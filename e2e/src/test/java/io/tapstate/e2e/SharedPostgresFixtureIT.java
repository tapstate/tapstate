package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real logical-slot inventory distinguishes fixture lifetime from one stream stop or one borrower. */
@RequiresDocker
class SharedPostgresFixtureIT {
    @Test
    void theLastBorrowerReleasesTheSameOwnedSlotOnlyAfterItsWholeFixtureEnds() throws Exception {
        String database = name("borrowers");
        var first = SharedPostgres.fixture(); var second = SharedPostgres.fixture();
        Map<String, Object> settings = first.settings(database); second.settings(database);
        String slot = name("slot");
        createSlot(settings, slot);
        List<String> original = slots(settings);
        assertThat(original).containsExactly(slot);
        first.close();
        assertThat(first.receipts().getFirst().status()).isEqualTo("BORROWER_REMAINS");
        assertThat(slots(settings)).as("one borrower closing does not reset the retained slot identity").isEqualTo(original);
        second.close();
        assertThat(slots(settings)).as("only the final fixture owner releases its logical slot").isEmpty();
    }

    @Test
    void anotherDatabaseAndAnUnconfirmedServerEndAreNeverReclaimed() throws Exception {
        var current = SharedPostgres.fixture(); var other = SharedPostgres.fixture();
        Map<String, Object> own = current.settings(name("current"));
        Map<String, Object> foreign = other.settings(name("foreign"));
        String ownSlot = name("slot"), foreignSlot = name("slot");
        createSlot(own, ownSlot); createSlot(foreign, foreignSlot);
        var closing = current.track("controlled still-alive resource", () -> { }, () -> false);
        closing.close(); current.close();
        assertThat(current.receipts().getFirst().status()).isEqualTo("OWNER_END_UNCONFIRMED");
        assertThat(slots(own)).containsExactly(ownSlot);
        assertThat(slots(foreign)).as("an unrelated live fixture keeps its inactive slot").containsExactly(foreignSlot);
        // This is a controlled SQL slot with no native server. Its oracle owns explicit final cleanup.
        dropSlot(own, ownSlot); other.close(); assertThat(slots(foreign)).isEmpty();
    }

    @Test
    void anUnscopedBorrowerDoesNotGrantPermissionToReclaimItsDatabase() throws Exception {
        var fixture = SharedPostgres.fixture();
        String database = name("unscoped");
        Map<String, Object> settings = fixture.settings(database);
        assertThat(SharedPostgres.settings(database)).isEqualTo(settings);
        String slot = name("slot");
        createSlot(settings, slot);
        fixture.close();
        assertThat(fixture.receipts().getFirst().status()).isEqualTo("UNSCOPED_BORROWER_UNCONFIRMED");
        assertThat(slots(settings)).as("an unscoped caller's inactive slot remains protected").containsExactly(slot);
        dropSlot(settings, slot);
    }

    @Test
    void aThrowingCloseCannotAuthorizeCleanupEvenWhenItsProbeWouldSayEnded() throws Exception {
        var fixture = SharedPostgres.fixture();
        Map<String, Object> settings = fixture.settings(name("close_failure")); String slot = name("slot");
        createSlot(settings, slot);
        var closing = fixture.track("controlled throwing resource", () -> { throw new java.io.IOException("controlled close refusal"); }, () -> true);
        assertThatThrownBy(closing::close).isInstanceOf(java.io.IOException.class);
        fixture.close();
        assertThat(fixture.receipts().getFirst().status()).isEqualTo("OWNER_END_UNCONFIRMED");
        assertThat(slots(settings)).containsExactly(slot);
        dropSlot(settings, slot);
    }

    @Test
    void thirtyThreeEndedFixturesDoNotAccumulateToTheSharedServersThirtyTwoSlotLimit() throws Exception {
        for (int index = 0; index < 33; index++) {
            var fixture = SharedPostgres.fixture(); Map<String, Object> settings = fixture.settings(name("ended"));
            String slot = name("slot"); createSlot(settings, slot);
            assertThat(slots(settings)).containsExactly(slot);
            fixture.close();
            assertThat(slots(settings)).as("ended fixture %s releases its real slot before another is opened", index).isEmpty();
        }
    }

    @Test
    void anUnexpectedActiveConnectionIsPreservedAfterTheFixtureOwnerEnds() throws Exception {
        var fixture = SharedPostgres.fixture(); Map<String, Object> settings = fixture.settings(name("active"));
        String slot = name("slot"), publication = name("publication");
        try (Connection setup = SharedPostgres.connect(settings); var statement = setup.createStatement()) {
            statement.execute("CREATE TABLE records (id INT PRIMARY KEY)");
            statement.execute("CREATE PUBLICATION " + publication + " FOR ALL TABLES");
        }
        createSlot(settings, slot);
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("user", String.valueOf(settings.get("username")));
        properties.setProperty("password", String.valueOf(settings.get("password")));
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        properties.setProperty("replication", "database"); properties.setProperty("preferQueryMode", "simple");
        String url = "jdbc:postgresql://" + settings.get("host") + ":" + settings.get("port") + "/" + settings.get("database");
        // The control deliberately leaves an external active replication client unconfirmed. No native
        // product handle or shutdown proof is fabricated; active server inventory must still prevent drop.
        try (Connection connection = java.sql.DriverManager.getConnection(url, properties);
                var stream = connection.unwrap(PGConnection.class).getReplicationAPI().replicationStream().logical()
                        .withSlotName(slot).withSlotOption("proto_version", 1).withSlotOption("publication_names", publication).start()) {
            assertThat(active(settings, slot)).isTrue();
            fixture.close();
            assertThat(fixture.receipts().getFirst().status()).isEqualTo("ACTIVE_SLOTS_RETAINED");
            assertThat(fixture.receipts().getFirst().retained()).containsExactly(slot);
            assertThat(active(settings, slot)).isTrue(); assertThat(slots(settings)).containsExactly(slot);
        } finally {
            Await.until("the controlled replication client to release its actual active slot", () -> !active(settings, slot), () -> slot);
            dropSlot(settings, slot);
        }
    }

    @Test
    void aConfirmedOwnedServerEndCannotDropABackendThatStaysActiveThroughTheBound() throws Exception {
        var fixture = SharedPostgres.fixture(); Map<String, Object> settings = fixture.settings(name("tracked_active"));
        String slot = name("slot"), publication = name("publication");
        try (Connection setup = SharedPostgres.connect(settings); var statement = setup.createStatement()) {
            statement.execute("CREATE TABLE records (id INT PRIMARY KEY)");
            statement.execute("CREATE PUBLICATION " + publication + " FOR ALL TABLES");
        }
        createSlot(settings, slot);
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("user", String.valueOf(settings.get("username")));
        properties.setProperty("password", String.valueOf(settings.get("password")));
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        properties.setProperty("replication", "database"); properties.setProperty("preferQueryMode", "simple");
        String url = "jdbc:postgresql://" + settings.get("host") + ":" + settings.get("port") + "/" + settings.get("database");
        // A real owned context closes normally, while this controlled native replication connection
        // remains active. A finite close-inventory wait cannot manufacture permission to drop it.
        try (Connection connection = java.sql.DriverManager.getConnection(url, properties);
                var stream = connection.unwrap(PGConnection.class).getReplicationAPI().replicationStream().logical()
                        .withSlotName(slot).withSlotOption("proto_version", 1).withSlotOption("publication_names", publication).start()) {
            assertThat(active(settings, slot)).isTrue();
            String controlStore = SharedMongo.replicaSetUrl(name("tracked_close"));
            ServerHandle owned = fixture.launch(() -> Tiers.IN_PROCESS.launch(controlStore));
            try (owned) { assertThat(owned.terminated()).isFalse(); }
            assertThat(owned.terminated()).as("the actual owned server returned normal close and its terminal probe is true").isTrue();
            assertThatThrownBy(fixture::close).isInstanceOf(AssertionError.class).hasMessageContaining("timed out");
            assertThat(active(settings, slot)).isTrue(); assertThat(slots(settings)).containsExactly(slot);
        } finally {
            Await.until("the controlled replication client to release its actual active slot", () -> !active(settings, slot), () -> slot);
            dropSlot(settings, slot);
        }
    }

    private static boolean active(Map<String, Object> settings, String slot) {
        try (Connection connection = SharedPostgres.connect(settings);
                var statement = connection.prepareStatement("SELECT active FROM pg_replication_slots WHERE slot_name = ? AND database = ?")) {
            statement.setString(1, slot); statement.setString(2, String.valueOf(settings.get("database"))); statement.setQueryTimeout(10);
            try (var rows = statement.executeQuery()) { return rows.next() && rows.getBoolean(1); }
        } catch (Exception failure) { throw new AssertionError("cannot read the controlled logical slot activity", failure); }
    }

    private static String name(String prefix) { return "fixture_" + prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12); }
    private static void createSlot(Map<String, Object> settings, String slot) throws Exception {
        try (Connection connection = SharedPostgres.connect(settings);
                var statement = connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, 'pgoutput')")) {
            statement.setString(1, slot); statement.setQueryTimeout(10); statement.execute();
        }
    }
    private static void dropSlot(Map<String, Object> settings, String slot) throws Exception {
        try (Connection connection = SharedPostgres.connect(settings); var statement = connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
            statement.setString(1, slot); statement.setQueryTimeout(10); statement.execute();
        }
    }
    private static List<String> slots(Map<String, Object> settings) throws Exception {
        List<String> values = new ArrayList<>();
        try (Connection connection = SharedPostgres.connect(settings);
                var statement = connection.prepareStatement("SELECT slot_name FROM pg_replication_slots WHERE database = ? ORDER BY slot_name")) {
            statement.setString(1, String.valueOf(settings.get("database"))); statement.setQueryTimeout(10); statement.setMaxRows(33);
            try (var rows = statement.executeQuery()) { while (rows.next()) { values.add(rows.getString(1)); } }
        }
        return List.copyOf(values);
    }
}
