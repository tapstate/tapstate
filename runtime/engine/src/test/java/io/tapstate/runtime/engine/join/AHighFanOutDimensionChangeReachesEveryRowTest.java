package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One dimension row with a hundred thousand fact rows hanging off it, changed once.
 *
 * <p>The ordinary cases prove the mechanism on three rows. Three rows prove that a dimension change
 * reaches its fact rows; they cannot say anything about the cliff, and the cliff is where this
 * feature is most fragile: one edit to one row becomes a hundred thousand rows to re-emit, and the
 * two ways of getting that wrong are both silent. Emitting them all inside one call holds the thread
 * for as long as the whole fan-out takes and everything else on it waits; stopping part way leaves
 * the target holding a mixture of old and new values with the job running and nothing reported.
 *
 * <p>So both are asserted: every row arrives and carries the new value, and it took more than one
 * offer to get them -- the operator handed back control and was able to pick up where it stopped.
 * The second is the one that needs a sink with a limit on it, because a sink that always accepts
 * lets a driver that never yields look identical to one that does.
 */
class AHighFanOutDimensionChangeReachesEveryRowTest {

    /** The plan asks for at least a hundred thousand rows under one key. */
    private static final int FACT_ROWS = 100_000;

    /** How many rows the sink takes before refusing, which is what forces the driver to yield. */
    private static final int ROOM_PER_ROUND = 4_096;

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    @Test
    @DisplayName("a hundred thousand rows under one key all get the new value, and in more than one go")
    void everyRowIsReachedAndTheOperatorYieldsAlongTheWay() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                        + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id", TABLES);
        JoinDriver driver = new JoinDriver(plan, List.of("o_id"), "order_state",
                new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE));
        BoundedSink sink = new BoundedSink();

        feed(driver, sink, new SourceChange("c", Envelope.insert(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), null)));
        for (int i = 0; i < FACT_ROWS; i++) {
            feed(driver, sink, new SourceChange("o", Envelope.insert(1L, "src",
                    row("o_id", (long) i, "o_cust_id", 1L), null)));
        }

        sink.forget();
        int roundsBefore = sink.rounds();

        // The one edit that owes a hundred thousand rows.
        feed(driver, sink, new SourceChange("c", Envelope.update(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), row("c_id", 1L, "c_name", "Bo"), null)));

        // Not "more than once": a driver that yielded a single time would satisfy that, and the
        // arithmetic says what to expect instead. A hundred thousand rows through a sink that takes
        // four thousand at a time cannot come out in fewer than twenty-four goes.
        assertThat(sink.rounds() - roundsBefore)
                .as("the recompute has to come out in pieces; done in one call it would hold the "
                        + "thread for the whole fan-out")
                .isGreaterThanOrEqualTo(FACT_ROWS / ROOM_PER_ROUND);

        Set<Long> reached = sink.ordersCarrying("Bo");
        assertThat(reached)
                .as("every row under the key owes the new value; a recompute that stops part way "
                        + "leaves old and new mixed in the target and reports nothing")
                .hasSize(FACT_ROWS);
        assertThat(sink.ordersCarrying("Ada"))
                .as("and none is left holding the old one")
                .isEmpty();
    }

    /**
     * A sink that takes a fixed number of rows and then refuses until it is offered to again, which
     * is how a real one behaves when what is downstream of it is full.
     */
    private static final class BoundedSink implements JoinSink {

        private final Map<Long, String> latest = new LinkedHashMap<>();
        private int roomLeft = ROOM_PER_ROUND;
        private int rounds;

        @Override
        public boolean offer(Envelope change) {
            if (roomLeft == 0) {
                return false;
            }
            roomLeft--;
            Map<String, Object> after = change.after();
            if (after != null) {
                latest.put((Long) after.get("order_id"), (String) after.get("customer_name"));
            }
            return true;
        }

        /** Opens the sink again, the way a drained queue does. */
        void reopen() {
            roomLeft = ROOM_PER_ROUND;
            rounds++;
        }

        int rounds() {
            return rounds;
        }

        /** Drops what was seen during the build-up, so the assertions read the recompute alone. */
        void forget() {
            latest.clear();
        }

        Set<Long> ordersCarrying(String name) {
            Set<Long> orders = new HashSet<>();
            latest.forEach((order, carried) -> {
                if (name.equals(carried)) {
                    orders.add(order);
                }
            });
            return orders;
        }
    }

    /** Offers until the driver says it is finished, reopening the sink whenever it fills up. */
    private static void feed(JoinDriver driver, BoundedSink sink, SourceChange change) {
        if (driver.apply(List.of(change), sink)) {
            return;
        }
        for (int offer = 0; offer < 1_000_000; offer++) {
            sink.reopen();
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
