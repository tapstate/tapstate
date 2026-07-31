package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MySQL endpoint driver, checked against a real MySQL and nothing else - no product, no connector.
 *
 * <p>This is the harness's independent reading of a JDBC store, so it is checked the way the product
 * never gets to define: by writing rows with this driver and reading them back over a connection this
 * test opens itself, and by reading rows this driver did not write. A count taken through the
 * connector's own code would agree with the connector by construction and would keep agreeing while
 * nothing crossed the product at all.
 *
 * <p>Row shape mirrors the file and Mongo drivers - an id and a sequence, ids being the whole numbers
 * 1..N and an insert continuing them. That uniformity is what lets one specification say "seed 3, expect
 * 3" against any store: an author choosing a store should not have to learn a different row shape.
 *
 * <p>Needs Docker for the database, and nothing else - no connector jar. What a real PDK connector does
 * with this data is a different witness (the real-connector examples under {@code examples/}); this one
 * is about whether the harness can lay down and read back rows in MySQL at all.
 */
class MySqlEndpointsIT {

    private static final String TABLE = "orders";

    /** A database of this class's own on the shared server; nothing else writes in it. */
    private static Map<String, Object> settings;

    private final MySqlEndpoints endpoints = new MySqlEndpoints();

    @BeforeAll
    static void takeADatabase() {
        DockerGate.require();
        settings = SharedMySql.settings("e2e_mysql_endpoints");
    }

    @AfterEach
    void releaseTheDriver() {
        endpoints.close();
    }

    /** The table is the shape of the rows: columns from the row, id the key, strings as strings. */
    @Test
    void seedsTheRowsThemselvesAndReadsOneBack() {
        endpoints.seed(at(), TABLE, List.of(
                Map.of("id", 1L, "name", "widget"),
                Map.of("id", 2L, "name", "gadget")));

        assertThat(endpoints.count(at(), TABLE)).isEqualTo(2L);
        assertThat(endpoints.fetch(at(), TABLE, Map.of("id", 1L)))
                .hasValueSatisfying(row -> assertThat(row)
                        .containsEntry("name", "widget")
                        .containsEntry("id", 1L));
    }

    /** Empty is a reading; ambiguity is an error. A matcher polling absence must not poll ambiguity. */
    @Test
    void fetchAnswersEmptyForNoMatchAndRefusesMoreThanOne() {
        endpoints.seed(at(), TABLE, List.of(
                Map.of("id", 1L, "name", "twin"),
                Map.of("id", 2L, "name", "twin")));

        assertThat(endpoints.fetch(at(), TABLE, Map.of("id", 9L))).isEmpty();
        assertThat(endpoints.fetch(at(), TABLE, Map.of("name", "no-such-row"))).isEmpty();
        assertThatThrownBy(() -> endpoints.fetch(at(), TABLE, Map.of("name", "twin")))
                .hasMessageContaining("more than one");
    }

    /** An absent table answers empty like an absent row: both read as "nothing there yet". */
    @Test
    void fetchAnswersEmptyForATableNoOneHasCreatedYet() {
        assertThat(endpoints.fetch(at(), "never_created", Map.of("id", 1L))).isEmpty();
    }

    @Test
    void seedingWritesTheRowsNumberedFromOne() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));

        assertThat(rowsReadBackIndependently()).containsExactly("1,1", "2,2", "3,3");
    }

    @Test
    void seedingReplacesWhateverTheTableHeld() {
        endpoints.seed(at(), TABLE, SeedRows.generated(5));
        endpoints.seed(at(), TABLE, SeedRows.generated(2));

        assertThat(rowsReadBackIndependently()).containsExactly("1,1", "2,2");
    }

    @Test
    void countingReadsTheRowsBack() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));

        assertThat(endpoints.count(at(), TABLE)).isEqualTo(3L);
    }

    /**
     * The reading that makes an {@code await} on a target meaningful: before the product creates it, the
     * target table does not exist, and the honest count of a table that is not there is zero. Letting the
     * database's "no such table" reach the specification instead would turn every wait for a first write
     * into a failure, and no specification could ever await a target the product has yet to create.
     */
    @Test
    void aTableNoOneHasCreatedYetCountsZero() {
        assertThat(endpoints.count(at(), "never_created")).isZero();
    }

    @Test
    void countingReadsRowsThisDriverDidNotWrite() throws SQLException {
        execute("DROP TABLE IF EXISTS handwritten");
        execute("CREATE TABLE handwritten (id BIGINT PRIMARY KEY, seq BIGINT)");
        execute("INSERT INTO handwritten (id, seq) VALUES (7, 7), (8, 8)");

        assertThat(endpoints.count(at(), "handwritten")).isEqualTo(2L);
    }

    @Test
    void insertingAppendsRowsAfterTheHighestIdTheTableHolds() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));

        endpoints.cdc(at(), TABLE, CdcOp.INSERT, 2);

        assertThat(rowsReadBackIndependently()).containsExactly("1,1", "2,2", "3,3", "4,4", "5,5");
    }

    @Test
    void deletingRemovesTheLowestIdsAndLowersTheCount() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));

        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        assertThat(rowsReadBackIndependently()).containsExactly("3,3", "4,4");
        assertThat(endpoints.count(at(), TABLE)).isEqualTo(2L);
    }

    /** An update changes rows without adding or removing any, so a count cannot witness it. */
    @Test
    void updatingRewritesTheSequenceOfTheLowestIdsAndLeavesTheCountAlone() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));

        endpoints.cdc(at(), TABLE, CdcOp.UPDATE, 2);

        assertThat(rowsReadBackIndependently()).containsExactly("1,-1", "2,-2", "3,3");
        assertThat(endpoints.count(at(), TABLE)).isEqualTo(3L);
    }

    /**
     * "The lowest ids" stops meaning "the low numbers" the moment a row is deleted.
     *
     * <p>Every other case here seeds ids 1..N and changes them straight away, so a driver that read
     * "lowest two" as {@code id <= 2} would pass all of them. It would then quietly do nothing to a table
     * whose surviving ids start at three - and a specification asserting an unchanged count would still be
     * green, because doing nothing changes no count either. This is the case that tells the two apart.
     */
    @Test
    void changesReachTheRowsThatAreActuallyLowestNotTheLowNumbers() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));
        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        endpoints.cdc(at(), TABLE, CdcOp.UPDATE, 2);

        assertThat(rowsReadBackIndependently()).containsExactly("3,-3", "4,-4");
    }

    /**
     * An insert continues from the highest id present, not from the row count - after a delete the two
     * differ, and numbering by count would collide with a row that is still there.
     */
    @Test
    void insertingAfterADeleteContinuesFromTheHighestIdNotTheCount() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));
        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        endpoints.cdc(at(), TABLE, CdcOp.INSERT, 1);

        assertThat(rowsReadBackIndependently()).containsExactly("3,3", "4,4", "5,5");
    }

    /**
     * Changing a table that was never seeded is an authoring mistake, not a change - the same refusal the
     * file driver makes, for the same reason: silently creating one would let a specification whose seed
     * and cdc name different tables pass by accident.
     */
    @Test
    void changingATableNoOneSeededRefusesRatherThanCreatingIt() {
        assertThatThrownBy(() -> endpoints.cdc(at(), "never_created", CdcOp.INSERT, 1))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("never_created")
                .hasMessageContaining("not been seeded");
    }

    /**
     * Two databases taken separately share nothing, which is the whole basis for one server serving every
     * specification in the JVM.
     *
     * <p>It is pinned here because this is the lane that always runs. The property is also exercised by the
     * two tiers of the real-connector witnesses - they would collide on the same table - but those need
     * connector jars and stay out of the default build, so on their own they would leave this unguarded
     * exactly where it is cheapest to break.
     */
    @Test
    void databasesTakenSeparatelyShareNoTables() {
        EndpointAddress mine = at();
        EndpointAddress theirs =
                new EndpointAddress("src_other", SharedMySql.settings("e2e_mysql_endpoints_other"));

        endpoints.seed(mine, TABLE, SeedRows.generated(3));
        endpoints.seed(theirs, TABLE, SeedRows.generated(5));

        assertThat(endpoints.count(mine, TABLE)).isEqualTo(3L);
        assertThat(endpoints.count(theirs, TABLE)).isEqualTo(5L);
    }

    /**
     * A JDBC store is addressed by several settings, so an absent one has to name itself: "cannot connect"
     * sends an author to their database, and the fault is in their resource.
     */
    @Test
    void anAddressMissingASettingRefusesAndNamesWhatItLacks() {
        EndpointAddress withoutPort = new EndpointAddress(
                "src_mysql", Map.of("host", settings.get("host"), "database", settings.get("database")));

        assertThatThrownBy(() -> endpoints.count(withoutPort, TABLE))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("src_mysql")
                .hasMessageContaining("port");
    }

    /** The address as a resource would carry it: five settings, no one of which is the whole address. */
    private static EndpointAddress at() {
        return new EndpointAddress("src_mysql", settings);
    }

    /**
     * Reads the table over a connection this test opens itself, so the driver cannot agree with itself:
     * a driver that reported what it meant to write rather than what the database holds would pass every
     * count assertion and fail here.
     */
    private List<String> rowsReadBackIndependently() {
        return rowsOf(TABLE);
    }

    private List<String> rowsOf(String table) {
        List<String> rows = new ArrayList<>();
        try (Connection connection = open();
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT id, seq FROM " + table + " ORDER BY id")) {
            while (results.next()) {
                rows.add(results.getLong("id") + "," + results.getLong("seq"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("cannot read " + table + " back", e);
        }
        return rows;
    }

    private void execute(String sql) throws SQLException {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static Connection open() throws SQLException {
        return SharedMySql.connect(settings);
    }
}
