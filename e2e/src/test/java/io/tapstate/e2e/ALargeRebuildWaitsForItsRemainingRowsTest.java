package io.tapstate.e2e;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.runtime.engine.join.JoinDriver;
import io.tapstate.runtime.engine.join.JoinGauge;
import io.tapstate.runtime.engine.join.JoinSink;
import io.tapstate.runtime.engine.join.MapJoinStores;
import io.tapstate.runtime.engine.join.SourceChange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Samples the lane witness's completion wait between two deliveries of a real rebuild. Equal
 * progress and target counts at that boundary must not let the wait return with rows still owed.
 * No database timing is needed: the sink's refusal holds the exact interleaving open.
 */
class ALargeRebuildWaitsForItsRemainingRowsTest {
    private static final int ROWS = 12_000;
    private static final int FIRST_DELIVERY = 2_048;

    @Test
    void theCompletionWaitDoesNotReturnAfterOnePartialDelivery() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.id AS order_id, c.name AS customer_name "
                        + "FROM orders o JOIN customers c ON o.customer_id = c.id",
                List.of(
                        new SourceTable("orders", List.of(
                                new SourceColumn("id", TapstateType.INT64, false),
                                new SourceColumn("customer_id", TapstateType.INT64, true))),
                        new SourceTable("customers", List.of(
                                new SourceColumn("id", TapstateType.INT64, false),
                                new SourceColumn("name", TapstateType.STRING, false)))));
        Progress progress = new Progress();
        JoinDriver driver = new JoinDriver(plan, List.of("id"), "order_state",
                new MapJoinStores(), 8_000, progress);
        Map<Long, Map<String, Object>> target = new HashMap<>();
        JoinSink accepting = event -> {
            target.put((Long) event.after().get("order_id"), event.after());
            return true;
        };
        Map<String, Object> before = Map.of("id", 1L, "name", "ada");
        Map<String, Object> after = Map.of("id", 1L, "name", "adelaide");
        assertThat(driver.apply(List.of(new SourceChange("c",
                Envelope.insert(1L, "src", before, null))), accepting)).isTrue();
        List<SourceChange> orders = new ArrayList<>();
        for (long id = 1; id <= ROWS; id++) {
            orders.add(new SourceChange("o", Envelope.insert(1L, "src",
                    Map.of("id", id, "customer_id", 1L), null)));
        }
        assertThat(driver.apply(orders, accepting)).isTrue();
        assertThat(target).hasSize(ROWS);
        assertThat(carried(target, "ada")).isEqualTo(ROWS);

        int[] room = {FIRST_DELIVERY};
        assertThat(driver.apply(List.of(new SourceChange("c",
                Envelope.update(2L, "src", before, after, null))), event -> {
                    if (room[0] == 0) {
                        return false;
                    }
                    room[0]--;
                    return accepting.offer(event);
                })).isFalse();
        // An idle retry reports the current position before finding the outbox still full.
        assertThat(driver.drain(event -> false)).isFalse();
        assertThat(driver.hasPending()).isTrue();
        assertThat(progress.done).isEqualTo(FIRST_DELIVERY);
        assertThat(progress.expected).isEqualTo(ROWS);
        assertThat(carried(target, "adelaide")).isEqualTo(FIRST_DELIVERY);

        int[] polls = {0};
        long carriedWhenWaitReturned;
        try {
            ALargeRebuildIsVisibleWhileItRunsIT.awaitRebuildToStopAdvancing(
                    () -> {
                        // The next observation lets the next delivery run. Returning on the first
                        // observation is the lane's race, not a rebuild that cannot make progress.
                        if (polls[0]++ > 0) {
                            assertThat(driver.drain(accepting)).isTrue();
                        }
                        return progress.done;
                    },
                    () -> carried(target, "adelaide"),
                    () -> "done=" + progress.done + ", expected=" + progress.expected
                            + ", target=" + carried(target, "adelaide"));
            carriedWhenWaitReturned = carried(target, "adelaide");
        } finally {
            // The control proves the walk retains and can deliver all the work the wait missed.
            assertThat(driver.drain(accepting)).isTrue();
            assertThat(driver.hasPending()).isFalse();
            assertThat(carried(target, "adelaide")).isEqualTo(ROWS);
            assertThat(progress.done).isEqualTo(ROWS);
            assertThat(progress.expected).isEqualTo(ROWS);
        }

        assertThat(carriedWhenWaitReturned)
                .as("the completion wait must not return at 2048 of 12000 rows: equal rows-sent "
                        + "and target counts can describe a partial delivery with work still pending")
                .isEqualTo(ROWS);
    }

    private static long carried(Map<Long, Map<String, Object>> target, String name) {
        return target.values().stream()
                .filter(row -> name.equals(row.get("customer_name"))).count();
    }

    private static final class Progress implements JoinGauge {
        private Long done;
        private long expected;

        @Override
        public void bucketWalked(String source, String key, int pages) {
        }

        @Override
        public void recomputing(String source, String key, long rowsDone, long rowsExpected) {
            done = rowsDone;
            expected = rowsExpected;
        }
    }
}
