package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
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
 * A RIGHT JOIN is turned into a LEFT JOIN over swapped sides before anything runs it, and this
 * checks that the swap survives contact with the driver -- that the rows a person gets back are the
 * rows they would have got had they written the swap out by hand.
 *
 * <p>The rewrite is already checked as a plan shape elsewhere: the sides move, the key pair moves
 * with them, the preserved side becomes the one rows are driven from. What that cannot show is
 * whether the plan it produces still <em>runs</em> to the same answer, and the rewrite is exactly
 * the kind of change whose mistakes are invisible in a shape assertion -- a key pair that reads the
 * right columns in the wrong order probes the mirror under a key nobody stored, and the symptom is
 * rows that quietly do not match rather than anything that fails.
 *
 * <p>Two things have to hold together, and neither alone is worth much. The two runs must agree --
 * that is the claim. And the run must not be vacuous: an implementation publishing nothing at all,
 * or dropping every unmatched row, agrees with itself perfectly. So the final state is asserted
 * against values written out here, and it deliberately contains the three cases that separate a
 * preserved side from an inner join: a fact row that matches, one whose dimension row shows up only
 * later, and one whose join key is null and therefore never matches anything at all.
 */
class ARightJoinRunsAsItsSwappedLeftTest {

    private static final String STREAM = "order_state";

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    private static final String SELECT = "SELECT o.o_id AS order_id, c.c_name AS customer_name ";

    /** What a person wrote. The preserved side is orders, and it is named second. */
    private static final String AS_WRITTEN =
            SELECT + "FROM customers c RIGHT JOIN orders o ON o.o_cust_id = c.c_id";

    /** What they would have had to write instead. Same meaning, sides the other way round. */
    private static final String AS_SWAPPED =
            SELECT + "FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

    @Test
    @DisplayName("a RIGHT JOIN publishes what the swapped LEFT JOIN publishes, over one corpus")
    void theRewrittenPlanRunsToTheSameRows() {
        List<Map.Entry<Op, Map<String, Object>>> asWritten = run(AS_WRITTEN);
        List<Map.Entry<Op, Map<String, Object>>> asSwapped = run(AS_SWAPPED);

        assertThat(asWritten)
                .as("the rewrite is invisible to the author, so the rows must be too")
                .isEqualTo(asSwapped);
    }

    @Test
    @DisplayName("and those rows are the ones a preserved side owes, not an empty agreement")
    void theRunIsNotVacuous() {
        // The control for the case above. Two runs that publish nothing agree exactly, and so do two
        // that drop every unmatched row -- which is the inner-join answer, and the single most likely
        // way for a rewrite to be wrong.
        assertThat(finalNameByOrder(run(AS_WRITTEN)))
                .containsOnly(
                        // matched when it arrived
                        Map.entry(10L, "Ada"),
                        // unmatched on arrival; its dimension row turned up afterwards
                        Map.entry(11L, "Bo"),
                        // a null join key matches nothing, ever -- but the preserved side still owes
                        // the row, with the dimension half empty
                        Map.entry(12L, NONE));
    }

    /** Stands in for a null in the assertion above; AssertJ's map entries do not take one. */
    private static final String NONE = "<no dimension row>";

    /**
     * One driver over one corpus, returning what reached the sink.
     *
     * <p>The corpus is the same for both spellings and the aliases are the same in both, so a single
     * change stream drives either plan.
     */
    private static List<Map.Entry<Op, Map<String, Object>>> run(String sql) {
        JoinPlan plan = SqlFrontEnd.derive(sql, TABLES);
        CountingJoinStores stores = new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE);
        JoinDriver driver = new JoinDriver(plan, List.of("o_id"), STREAM, stores);

        List<Envelope> taken = new ArrayList<>();
        JoinSink sink = change -> {
            taken.add(change);
            return true;
        };

        apply(driver, sink, dimension(insert(row("c_id", 1L, "c_name", "Ada"))));
        apply(driver, sink, fact(insert(row("o_id", 10L, "o_cust_id", 1L))));
        apply(driver, sink, fact(insert(row("o_id", 11L, "o_cust_id", 99L))));
        apply(driver, sink, fact(insert(row("o_id", 12L, "o_cust_id", null))));
        // Last, so the row above it has already been published unmatched and has to be revisited.
        apply(driver, sink, dimension(insert(row("c_id", 99L, "c_name", "Bo"))));

        List<Map.Entry<Op, Map<String, Object>>> rows = new ArrayList<>();
        for (Envelope event : taken) {
            Map<String, Object> after = event.after() != null ? event.after() : event.before();
            rows.add(Map.entry(event.op(), new LinkedHashMap<>(after)));
        }
        return rows;
    }

    /** Folds the changelog into the state it leaves behind: the last word on each order. */
    private static Map<Long, String> finalNameByOrder(List<Map.Entry<Op, Map<String, Object>>> rows) {
        Map<Long, String> byOrder = new LinkedHashMap<>();
        for (Map.Entry<Op, Map<String, Object>> row : rows) {
            Long id = (Long) row.getValue().get("order_id");
            if (row.getKey() == Op.DELETE) {
                byOrder.remove(id);
                continue;
            }
            Object name = row.getValue().get("customer_name");
            byOrder.put(id, name == null ? NONE : (String) name);
        }
        return byOrder;
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

    private static SourceChange fact(Envelope event) {
        return new SourceChange("o", event);
    }

    private static SourceChange dimension(Envelope event) {
        return new SourceChange("c", event);
    }

    private static Envelope insert(Map<String, Object> after) {
        return Envelope.insert(1L, "src", after, null);
    }

    /** A row that may carry a null, which {@code Map.of} refuses. */
    private static Map<String, Object> row(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(k1, v1);
        row.put(k2, v2);
        return row;
    }
}
