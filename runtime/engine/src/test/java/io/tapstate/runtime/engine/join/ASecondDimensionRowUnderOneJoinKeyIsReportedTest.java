package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What is said when two dimension rows share the column a join matches them by.
 *
 * <p>The mirror a join looks dimension rows up in holds one row per key, so the second row to arrive
 * under a key replaces the first, and every fact row under that key joins to whichever arrived last.
 * The target then holds fewer rows than the query describes.
 *
 * <p><b>This case is not about that loss.</b> Publishing both rows moves dimension state, output
 * cardinality and row identity together, and none of that happens here -- the target is still short a
 * row afterwards. It is about the loss being said out loud. A target quietly short of rows is
 * indistinguishable from a correct one and every row it does hold looks entirely ordinary, so the
 * moment of replacement is the only chance anything has to report it.
 *
 * <p>The alert is observed at the port rather than in a log or in a target table, because a warning on
 * this project travels no further than a log: nothing downstream changes, and a case that read a target
 * would be asserting the repair this deliberately does not make.
 *
 * <p><b>The source row key is the discriminating half.</b> Changing the row already filed under a key
 * replaces it too, and that is the same row rather than a lost one even where its earlier image is
 * absent. Conversely, an edit of a row that was displaced earlier can replace a different row while
 * carrying an earlier image. The source's own key distinguishes both; event shape and row contents do
 * not.
 */
class ASecondDimensionRowUnderOneJoinKeyIsReportedTest {

    private static final String STREAM = "order_state";

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true))),
            new SourceTable("customers", List.of(
                    new SourceColumn("customer_id", TapstateType.INT64, false),
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    private static final String SQL =
            "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                    + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id";

    /** The one key both customers are filed under, spelled the way the mirror spells it. */
    private static final String SHARED_KEY = JoinKey.of(List.of(1L)).name();

    @Test
    @DisplayName("a second dimension row under an occupied join key is reported, naming the source and the key")
    void theDisplacedDimensionRowIsSaid() {
        Recording alert = new Recording();
        JoinDriver driver = driver(alert);

        apply(driver, "c", Envelope.insert(1L, "src", customer(101L, 1L, "Ada"), null));
        apply(driver, "o", Envelope.insert(2L, "src", order(10L, 1L), null));
        apply(driver, "c", Envelope.update(3L, "src", customer(101L, 1L, "Ada"),
                customer(101L, 1L, "Ada Lovelace"), null));

        assertThat(alert.said)
                .as("an edit of the row already filed under the key replaces it as well, and nothing "
                        + "was lost -- it is the same row")
                .isEmpty();

        apply(driver, "c", Envelope.insert(4L, "src", customer(102L, 1L, "Bo"), null));

        assertThat(alert.said)
                .as("Ada is now unreachable and the order under that key joins to Bo, which is the "
                        + "third option a query must not be answered with: accepted, wrong, and silent")
                .containsExactly(new Said("c", SHARED_KEY));
    }

    @Test
    void anUpdateWithoutABeforeImageDoesNotInventASecondSourceRow() {
        Recording alert = new Recording();
        JoinDriver driver = driver(alert);

        apply(driver, "c", Envelope.insert(1L, "src", customer(101L, 1L, "Ada"), null));
        apply(driver, "c", Envelope.update(2L, "src", null,
                customer(101L, 1L, "Ada Lovelace"), null));

        assertThat(alert.said)
                .as("one source row was edited, so no row was displaced")
                .isEmpty();
    }

    @Test
    void aChangedRowInARepeatedSnapshotDoesNotInventASecondSourceRow() {
        Recording alert = new Recording();
        JoinDriver driver = driver(alert);

        apply(driver, "c", Envelope.read(1L, "src", customer(101L, 1L, "Ada"), null));
        apply(driver, "c", Envelope.read(2L, "src",
                customer(101L, 1L, "Ada Lovelace"), null));

        assertThat(alert.said)
                .as("the repeated snapshot contains the newer image of the same source row")
                .isEmpty();
    }

    @Test
    void updatingAPreviouslyDisplacedRowStillReportsTheRowItDisplaces() {
        Recording alert = new Recording();
        JoinDriver driver = driver(alert);
        Map<String, Object> ada = customer(101L, 1L, "Ada");
        Map<String, Object> bo = customer(102L, 1L, "Bo");

        apply(driver, "c", Envelope.insert(1L, "src", ada, null));
        apply(driver, "c", Envelope.insert(2L, "src", bo, null));
        apply(driver, "c", Envelope.update(3L, "src", ada,
                customer(101L, 1L, "Ada Lovelace"), null));

        assertThat(alert.said)
                .as("Bo is replaced when the formerly displaced Ada row is updated")
                .containsExactly(new Said("c", SHARED_KEY), new Said("c", SHARED_KEY));
    }

    @Test
    void movingARowOntoAnOccupiedJoinKeyReportsTheRowItDisplaces() {
        Recording alert = new Recording();
        JoinDriver driver = driver(alert);
        Map<String, Object> ada = customer(101L, 2L, "Ada");

        apply(driver, "c", Envelope.insert(1L, "src", ada, null));
        apply(driver, "c", Envelope.insert(2L, "src", customer(102L, 1L, "Bo"), null));
        apply(driver, "c", Envelope.update(3L, "src", ada,
                customer(101L, 1L, "Ada"), null));

        assertThat(alert.said)
                .as("Bo is replaced even though the arriving Ada row carries an earlier image")
                .containsExactly(new Said("c", SHARED_KEY));
    }

    @Test
    void distinctRowsWithTheSamePublishedValuesAreStillDistinguishedByTheirSourceKeys() {
        Recording alert = new Recording();
        JoinDriver driver = driver(alert);

        apply(driver, "c", Envelope.insert(1L, "src", customer(101L, 1L, "Ada"), null));
        apply(driver, "c", Envelope.insert(2L, "src", customer(102L, 1L, "Ada"), null));

        assertThat(alert.said)
                .as("equal join and projected values do not make two source keys the same row")
                .containsExactly(new Said("c", SHARED_KEY));
    }

    private static JoinDriver driver(Recording alert) {
        return new JoinDriver(SqlFrontEnd.derive(SQL, TABLES), List.of("o_id"), STREAM,
                new MapJoinStores(), Map.of("c", List.of("customer_id")), alert);
    }

    /** The port, remembering what it was told. */
    private static final class Recording implements DimensionRowDisplacedAlert {

        private final List<Said> said = new ArrayList<>();

        @Override
        public void displaced(String source, String dimensionKey) {
            said.add(new Said(source, dimensionKey));
        }
    }

    /** One thing the port was told, in the two names a reader needs to find the rows behind it. */
    private record Said(String source, String dimensionKey) {
    }

    /**
     * One change in, and the driver run out to nothing pending. The published rows are taken and
     * dropped: what they hold is the other half of this report and no assertion here depends on it.
     */
    private static void apply(JoinDriver driver, String source, Envelope event) {
        JoinSink sink = change -> true;
        if (driver.apply(List.of(new SourceChange(source, event)), sink)) {
            return;
        }
        for (int offer = 0; offer < 10_000; offer++) {
            if (driver.apply(List.of(), sink)) {
                return;
            }
        }
        throw new AssertionError("the driver never finished with nothing arriving");
    }

    private static Map<String, Object> customer(long id, long joinKey, String name) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("customer_id", id);
        row.put("c_id", joinKey);
        row.put("c_name", name);
        return row;
    }

    private static Map<String, Object> order(long id, long customerId) {
        return Map.of("o_id", id, "o_cust_id", customerId);
    }
}
