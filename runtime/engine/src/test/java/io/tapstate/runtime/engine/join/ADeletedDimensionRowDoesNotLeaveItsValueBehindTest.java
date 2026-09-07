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
 * What becomes of a fact row when the dimension row it was joined to is deleted.
 *
 * <p>This is the one moment that separates a join which really re-reads its dimension from one that
 * copied the value in once and kept it. In every steady state the two are character-for-character
 * identical: the same target rows, the same values, no error either way. Only a delete asks the
 * question, and only if the answer is asserted -- an implementation that leaves the old name sitting
 * in the target looks completely normal afterwards, and the row it leaves behind names a customer
 * that no longer exists.
 *
 * <p>The two join kinds owe different answers and both are asserted, because a single-kind case
 * cannot tell "handled the delete" from "handled it the one way I happened to test". Under a left
 * join the fact row is preserved and only its dimension half empties; under an inner join the row
 * stops qualifying and is retracted.
 */
class ADeletedDimensionRowDoesNotLeaveItsValueBehindTest {

    private static final String STREAM = "order_state";

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    private static final String LEFT_SQL =
            "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                    + "FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

    private static final String INNER_SQL =
            "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                    + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id";

    @Test
    @DisplayName("under a left join the fact row stays and its dimension half empties")
    void aLeftJoinKeepsTheRowAndDropsTheValue() {
        Run run = new Run(LEFT_SQL);
        run.seedAndJoin();

        assertThat(run.lastRowFor(10L))
                .as("before the delete, the joined value is there -- otherwise the case below "
                        + "would be satisfied by a join that never produced it")
                .containsEntry("customer_name", "Ada");

        run.deleteTheCustomer();

        Map<String, Object> after = run.lastRowFor(10L);
        assertThat(after)
                .as("the order still exists: a left join preserves it whether or not the "
                        + "dimension row does")
                .isNotNull();
        assertThat(after.get("customer_name"))
                .as("and it must NOT still say Ada -- that is the whole failure this case is for")
                .isNull();
    }

    @Test
    @DisplayName("under an inner join the fact row stops qualifying and is retracted")
    void anInnerJoinRetractsTheRow() {
        Run run = new Run(INNER_SQL);
        run.seedAndJoin();

        assertThat(run.lastRowFor(10L)).isNotNull();

        run.deleteTheCustomer();

        assertThat(run.lastOpFor(10L))
                .as("the row no longer satisfies the join, so it has to be withdrawn rather "
                        + "than left in the target")
                .isEqualTo(Op.DELETE);
    }

    /** One driver, one order, one customer, and the delete that asks the question. */
    private static final class Run {

        private final JoinDriver driver;
        private final List<Envelope> taken = new ArrayList<>();
        private final JoinSink sink = change -> {
            taken.add(change);
            return true;
        };

        Run(String sql) {
            JoinPlan plan = SqlFrontEnd.derive(sql, TABLES);
            this.driver = new JoinDriver(plan, List.of("o_id"), STREAM,
                    new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE));
        }

        void seedAndJoin() {
            apply(new SourceChange("c", Envelope.insert(1L, "src", row("c_id", 1L, "c_name", "Ada"), null)));
            apply(new SourceChange("o", Envelope.insert(1L, "src", row("o_id", 10L, "o_cust_id", 1L), null)));
        }

        void deleteTheCustomer() {
            apply(new SourceChange("c", Envelope.delete(1L, "src", row("c_id", 1L, "c_name", "Ada"), null)));
        }

        /** The last row published for one order, or null where the last word was a retraction. */
        Map<String, Object> lastRowFor(long orderId) {
            Envelope last = lastFor(orderId);
            if (last == null || last.op() == Op.DELETE) {
                return null;
            }
            return last.after();
        }

        Op lastOpFor(long orderId) {
            Envelope last = lastFor(orderId);
            return last == null ? null : last.op();
        }

        private Envelope lastFor(long orderId) {
            Envelope found = null;
            for (Envelope event : taken) {
                Map<String, Object> row = event.after() != null ? event.after() : event.before();
                if (row != null && Long.valueOf(orderId).equals(row.get("order_id"))) {
                    found = event;
                }
            }
            return found;
        }

        private void apply(SourceChange change) {
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
    }

    private static Map<String, Object> row(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(k1, v1);
        row.put(k2, v2);
        return row;
    }
}
