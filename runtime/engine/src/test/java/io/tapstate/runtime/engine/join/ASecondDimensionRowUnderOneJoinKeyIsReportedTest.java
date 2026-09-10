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
 * <p><b>The edit in the middle is the discriminating half.</b> Changing the row already filed under a
 * key replaces it too, and that is the same row rather than a lost one. Only an arrival carrying no
 * before image can be a second row, so a case that only inserted would be satisfied by an
 * implementation that reported every dimension write it made.
 */
class ASecondDimensionRowUnderOneJoinKeyIsReportedTest {

    private static final String STREAM = "order_state";

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true))),
            new SourceTable("customers", List.of(
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
        JoinDriver driver = new JoinDriver(SqlFrontEnd.derive(SQL, TABLES), List.of("o_id"), STREAM,
                new MapJoinStores(), alert);

        apply(driver, "c", Envelope.insert(1L, "src", row("c_id", 1L, "c_name", "Ada"), null));
        apply(driver, "o", Envelope.insert(2L, "src", row("o_id", 10L, "o_cust_id", 1L), null));
        apply(driver, "c", Envelope.update(3L, "src", row("c_id", 1L, "c_name", "Ada"),
                row("c_id", 1L, "c_name", "Ada Lovelace"), null));

        assertThat(alert.said)
                .as("an edit of the row already filed under the key replaces it as well, and nothing "
                        + "was lost -- it is the same row")
                .isEmpty();

        apply(driver, "c", Envelope.insert(4L, "src", row("c_id", 1L, "c_name", "Bo"), null));

        assertThat(alert.said)
                .as("Ada is now unreachable and the order under that key joins to Bo, which is the "
                        + "third option a query must not be answered with: accepted, wrong, and silent")
                .containsExactly(new Said("c", SHARED_KEY));
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

    private static Map<String, Object> row(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(k1, v1);
        row.put(k2, v2);
        return row;
    }
}
