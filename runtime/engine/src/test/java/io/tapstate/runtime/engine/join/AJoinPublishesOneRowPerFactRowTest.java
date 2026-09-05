package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinPlan;
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
 * One fact row produces one output row, even where the SQL would produce several.
 *
 * <p>A dimension table need not be unique on the column a join matches it by, and where it is not,
 * SQL says a fact row matching two dimension rows yields two output rows. This release yields one:
 * the dimension mirror holds a single row per join key, so the later of two rows sharing a key
 * replaces the earlier, and every fact row under that key joins to whichever arrived last.
 *
 * <p><b>That is a boundary this release draws deliberately, and it is why the target key is derived
 * the way it is</b> -- one row per fact row is what makes the fact key sufficient to identify a
 * result row on its own. It is also a real limitation with a real cost, tracked as
 * tapstate/tapstate#233, and this case exists so the cost is visible here rather than only in a
 * decision record somebody has to go and find.
 *
 * <p><b>Read this as a boundary, not as an endorsement.</b> If the boundary moves -- if a fact row
 * starts producing one row per matching dimension row -- this case is supposed to go red, and the
 * right response is to replace it rather than to make it pass again. A test that pins a limitation
 * earns its place only while the limitation is deliberate; the moment it is lifted, this file is the
 * first thing that should be deleted.
 *
 * <p>What makes the assertion discriminating: it is a count, and only a count can see this. A
 * lookup for the published row finds one under either behaviour and cannot tell "one row, correctly"
 * from "one row, having dropped a match". The second half reads which of the two names survived, so
 * an implementation that published a row assembled from neither would fail as well.
 */
class AJoinPublishesOneRowPerFactRowTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    @Test
    @DisplayName("two dimension rows sharing a join key yield one output row, carrying the later one")
    void aFactRowMatchingTwoDimensionRowsStillPublishesOnce() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                        + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id", TABLES);
        JoinDriver driver = new JoinDriver(plan, List.of("o_id"), "order_state",
                new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE));

        List<Envelope> taken = new ArrayList<>();
        JoinSink sink = change -> {
            taken.add(change);
            return true;
        };

        // Two customers under one key. Legal input: nothing declares c_id unique.
        apply(driver, sink, new SourceChange("c", Envelope.insert(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), null)));
        apply(driver, sink, new SourceChange("c", Envelope.insert(1L, "src",
                row("c_id", 1L, "c_name", "Bo"), null)));
        apply(driver, sink, new SourceChange("o", Envelope.insert(1L, "src",
                row("o_id", 10L, "o_cust_id", 1L), null)));

        assertThat(taken)
                .as("one row per fact row is the boundary this release draws; SQL would give two "
                        + "here, and a lookup could not tell the difference")
                .hasSize(1);
        assertThat(taken.get(0).after())
                .as("and it carries the dimension row that replaced the other, not some third thing")
                .containsEntry("order_id", 10L)
                .containsEntry("customer_name", "Bo");
    }

    private static void apply(JoinDriver driver, JoinSink sink, SourceChange change) {
        if (driver.apply(List.of(change), sink)) {
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
