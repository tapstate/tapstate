package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
 *
 * <p>The third case here is about the same wait seen from outside. Yielding is what makes the fan-out
 * survivable and it is also what makes it long, and for the whole of it the pipeline reads healthy: the
 * job runs, nothing errors, and the target holds half the old value and half the new one. What the
 * operator is told about that is asserted where it can be pinned down exactly -- a bounded sink makes
 * the number of steps arithmetic rather than timing -- and the reading reaching a real read face is
 * left to the end-to-end case, which is the only place the whole path exists.
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
     * Enough rows under one key to be worth reporting and to take several pages, and no more: what is
     * under test is the sequence of readings, and the sequence has the same shape at twelve pages as at
     * a hundred.
     */
    private static final int REPORTABLE_FACT_ROWS = 12_000;

    @Test
    @DisplayName("a rebuild says how far it has got while it is still going, not once it is over")
    void theRebuildReportsItsProgressWhileItIsStillGoing() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                        + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id", TABLES);
        RecordingGauge gauge = new RecordingGauge();
        JoinDriver driver = new JoinDriver(plan, List.of("o_id"), "order_state",
                new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE),
                JoinDriver.DEFAULT_KEYS_PER_READ, gauge);
        BoundedSink sink = new BoundedSink();

        feed(driver, sink, new SourceChange("c", Envelope.insert(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), null)));
        for (int i = 0; i < REPORTABLE_FACT_ROWS; i++) {
            feed(driver, sink, new SourceChange("o", Envelope.insert(1L, "src",
                    row("o_id", (long) i, "o_cust_id", 1L), null)));
        }

        gauge.forget();
        feed(driver, sink, new SourceChange("c", Envelope.update(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), row("c_id", 1L, "c_name", "Bo"), null)));

        assertThat(gauge.keysReported())
                .as("the reading is about one dimension key, and says which - as the driver holds it, "
                        + "which is the encoding everything else files that key under; rendering it for "
                        + "a person to read happens where it is reported and not here")
                .containsExactly(JoinKey.of(List.of(1L)).name());
        assertThat(gauge.sizes())
                .as("its size is read off the index as an upper bound, and is what a reporting "
                        + "threshold is applied to; a rebuild that understated it would be filtered "
                        + "out of every face by a threshold it should have passed")
                .allMatch(rows -> rows >= REPORTABLE_FACT_ROWS);
        assertThat(gauge.progress())
                .as("progress only ever moves forward, so two readings taken apart can be subtracted")
                .isSorted();
        assertThat(gauge.progress())
                .as("and it is reported part way, which is the whole point of it: a number that "
                        + "arrives when the rebuild ends describes a wait that is already over")
                .anyMatch(done -> done > 0 && done < REPORTABLE_FACT_ROWS);
        assertThat(gauge.progress().get(gauge.progress().size() - 1))
                .as("finishing at the size it was walking is what says the two numbers are about "
                        + "the same rebuild")
                .isEqualTo((long) REPORTABLE_FACT_ROWS);
    }

    /**
     * A fan-out that is not a whole number of pages. Every bucket is this shape except the one whose
     * last page happens to fill exactly, and the case above is that one - twelve thousand rows over a
     * thousand-row page - which is how it can assert the two numbers meet without the walk ever having
     * to make them meet.
     */
    private static final int UNEVEN_FACT_ROWS = 11_500;

    @Test
    @DisplayName("a rebuild that has ended reports the count it reached, not the estimate it started from")
    void theRebuildEndsOnTheCountItActuallyReached() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id AS order_id, c.c_name AS customer_name "
                        + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id", TABLES);
        RecordingGauge gauge = new RecordingGauge();
        JoinDriver driver = new JoinDriver(plan, List.of("o_id"), "order_state",
                new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE),
                JoinDriver.DEFAULT_KEYS_PER_READ, gauge);
        BoundedSink sink = new BoundedSink();

        feed(driver, sink, new SourceChange("c", Envelope.insert(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), null)));
        for (int i = 0; i < UNEVEN_FACT_ROWS; i++) {
            feed(driver, sink, new SourceChange("o", Envelope.insert(1L, "src",
                    row("o_id", (long) i, "o_cust_id", 1L), null)));
        }

        gauge.forget();
        feed(driver, sink, new SourceChange("c", Envelope.update(1L, "src",
                row("c_id", 1L, "c_name", "Ada"), row("c_id", 1L, "c_name", "Bo"), null)));

        List<Long> progress = gauge.progress();
        List<Long> sizes = gauge.sizes();
        assertThat(progress.get(progress.size() - 1))
                .as("the walk reached every row under the key, so that is what it finishes on")
                .isEqualTo((long) UNEVEN_FACT_ROWS);
        assertThat(sizes.get(sizes.size() - 1))
                .as("and the estimate it started from does not outlive the walk: it is an upper bound, "
                        + "a page's worth high wherever the last page is partial, so a rebuild that has "
                        + "ended and left it standing reads for ever as one that stopped just short - "
                        + "which is the reading this pair exists to rule out")
                .isEqualTo((long) UNEVEN_FACT_ROWS);
    }

    /** Keeps every rebuild reading in the order it was made, which is what the sequence is read from. */
    private static final class RecordingGauge implements JoinGauge {

        private final List<String> keys = new ArrayList<>();
        private final List<Long> progress = new ArrayList<>();
        private final List<Long> sizes = new ArrayList<>();

        @Override
        public void bucketWalked(String source, String dimensionKey, int pages) {
        }

        @Override
        public void recomputing(String source, String dimensionKey, long rowsDone, long rowsExpected) {
            if (!keys.contains(dimensionKey)) {
                keys.add(dimensionKey);
            }
            progress.add(rowsDone);
            sizes.add(rowsExpected);
        }

        /** Drops what the build-up reported, so the assertions read the one edit under test. */
        void forget() {
            keys.clear();
            progress.clear();
            sizes.clear();
        }

        List<String> keysReported() {
            return keys;
        }

        List<Long> progress() {
            return progress;
        }

        List<Long> sizes() {
            return sizes;
        }
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
