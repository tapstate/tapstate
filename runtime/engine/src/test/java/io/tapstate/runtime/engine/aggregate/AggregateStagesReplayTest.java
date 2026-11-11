package io.tapstate.runtime.engine.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.OrderedBytes;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.runtime.engine.aggregate.GroupChange.FieldInput;
import io.tapstate.runtime.engine.aggregate.GroupStage.GroupOutcome;
import io.tapstate.runtime.engine.aggregate.MembershipStage.RowEvent;
import io.tapstate.spi.store.OrderedStateStore;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The two stages together, through lost sends and replays: what reaches the target is a GROUP BY over the
 * source rows, with no row counted twice, none left behind in a group it moved out of, and no group
 * surviving its last row.
 */
class AggregateStagesReplayTest {

    private static final ValueType MONEY = ValueType.decimal(2);
    private static final ValueType TEXT = ValueType.of(TapstateType.STRING);

    private static RollupSpec field(String name, RollupFunction function, ValueType argument) {
        return new RollupSpec(name, function, "x", List.of(), null, argument,
                Aggregates.of(function).resultType(argument), null, null, Set.of());
    }

    private static final AggregateStepSpec SPEC = new AggregateStepSpec("sales", List.of("id"), null,
            Map.of("region", "after.region"), Map.of("region", TEXT),
            List.of(field("orders", RollupFunction.COUNT, ValueType.INT64), field("revenue", RollupFunction.SUM, MONEY),
                    field("most", RollupFunction.MAX, MONEY)));

    /** A source row: its region, or null when it does not pass the step's condition, and its amount. */
    private record Row(String region, BigDecimal amount) {
    }

    private static RowEvent event(long id, long seq, Row row) {
        byte[] key = OrderedBytes.ofKey(List.of(id));
        SourceOrder order = new SourceOrder(1, seq);
        if (row == null || row.region() == null) {
            return RowEvent.gone(key, "orders", order);
        }
        FieldInput amount = FieldInput.of(row.amount());
        return RowEvent.member(key, "orders", order, List.of(row.region()), List.of(FieldInput.of(null), amount, amount));
    }

    private static Map<List<Object>, Map<String, Object>> groupBy(Map<Long, Row> source) {
        Map<List<Object>, Map<String, Object>> groups = new HashMap<>();
        source.values().forEach(row -> {
            if (row.region() == null) {
                return;
            }
            Map<String, Object> group = groups.computeIfAbsent(List.of(row.region()), key -> {
                Map<String, Object> fresh = new LinkedHashMap<>();
                fresh.put("orders", 0L);
                fresh.put("revenue", new BigDecimal("0.00"));
                fresh.put("most", row.amount());
                return fresh;
            });
            group.put("orders", (Long) group.get("orders") + 1);
            group.put("revenue", ((BigDecimal) group.get("revenue")).add(row.amount()));
            group.put("most", row.amount().max((BigDecimal) group.get("most")));
        });
        return groups;
    }

    @Test
    void theTargetIsAGroupByOfTheSourceThroughLostSendsAndReplays() {
        for (int seed = 0; seed < 40; seed++) {
            Random random = new Random(seed);
            GroupStageTest.CountingStore memberStore = new GroupStageTest.CountingStore(new OrderedStateStore.Limit(7, 1 << 20)).swept();
            GroupStageTest.CountingStore groupStore = new GroupStageTest.CountingStore(new OrderedStateStore.Limit(30, 1 << 20));
            MembershipStage membership = new MembershipStage(memberStore);
            GroupStage groups = new GroupStage(SPEC, groupStore);
            Map<Long, Row> source = new HashMap<>();
            Map<List<Object>, Map<String, Object>> target = new HashMap<>();
            List<RowEvent> unsettled = new ArrayList<>();
            List<RowEvent> settled = new ArrayList<>();
            String[] regions = {"EU", "US", "APAC", null};
            long seq = 0;
            for (int drain = 0; drain < 120; drain++) {
                if (random.nextInt(3) == 0) {
                    // Between drains, whatever the last one came to: forget what lies below everything
                    // that can still be delivered.
                    long resumesAt = seq;
                    for (RowEvent event : settled) {
                        resumesAt = Math.min(resumesAt, event.order().seq());
                    }
                    for (RowEvent event : unsettled) {
                        resumesAt = Math.min(resumesAt, event.order().seq());
                    }
                    sweep(membership, groups, new SourceOrder(1, resumesAt), random.nextInt(1, 6));
                }
                List<RowEvent> delivered = new ArrayList<>();
                // A frontier that lags redelivers events whose effects already reached the target.
                if (random.nextInt(5) == 0 && !settled.isEmpty()) {
                    delivered.addAll(settled.subList(random.nextInt(settled.size()), settled.size()));
                }
                delivered.addAll(unsettled);
                List<RowEvent> fresh = new ArrayList<>();
                for (int i = random.nextInt(1, 12); i > 0; i--) {
                    long id = random.nextInt(12);
                    Row row = random.nextInt(5) == 0 ? null
                            : new Row(regions[random.nextInt(4)], BigDecimal.valueOf(random.nextInt(1, 9000), 2));
                    if (row == null) {
                        source.remove(id);
                    } else {
                        source.put(id, row);
                    }
                    fresh.add(event(id, seq++, row));
                }
                delivered.addAll(fresh);
                unsettled.addAll(fresh);

                MembershipStage.Outcome told = membership.apply(delivered);
                if (random.nextInt(6) == 0) {
                    continue; // stopped after the membership commit: nothing was told
                }
                List<GroupOutcome> outcomes = groups.apply(told.changes());
                if (random.nextInt(6) == 0) {
                    continue; // stopped after the group commit: nothing was sent
                }
                for (GroupOutcome outcome : outcomes) {
                    assertThat(outcome.failure()).isNull();
                    if (!outcome.send()) {
                        continue;
                    }
                    if (outcome.members() == 0) {
                        target.remove(outcome.group());
                    } else {
                        target.put(outcome.group(), outcome.values());
                    }
                }
                settled = new ArrayList<>(delivered.subList(Math.max(0, delivered.size() - 20), delivered.size()));
                unsettled.clear();
                assertThat(target).as("seed %d after drain %d", seed, drain).isEqualTo(groupBy(source));
            }
            // Bring both sides to rest, then let nothing be delivered again: what remains is one entry
            // for each row the source holds, and for each group its totals and its members' entries.
            List<RowEvent> rest = new ArrayList<>(settled);
            rest.addAll(unsettled);
            for (GroupOutcome outcome : groups.apply(membership.apply(rest).changes())) {
                if (!outcome.send()) {
                    continue;
                }
                if (outcome.members() == 0) {
                    target.remove(outcome.group());
                } else {
                    target.put(outcome.group(), outcome.values());
                }
            }
            sweep(membership, groups, new SourceOrder(1, seq), 4);
            long members = source.values().stream().filter(row -> row.region() != null).count();
            Map<List<Object>, Map<String, Object>> expected = groupBy(source);
            assertThat(target).as("seed %d: groups at rest", seed).isEqualTo(expected);
            // A row that is in no group is remembered only to answer a second delivery.
            assertThat((long) memberStore.size()).as("seed %d: rows kept", seed).isEqualTo(members);
            // Per group: its totals and the ordered entries behind "most", one for each member.
            assertThat((long) groupStore.size()).as("seed %d: group entries kept", seed)
                    .isEqualTo(expected.size() + 2 * members);
        }
    }

    private static void sweep(MembershipStage membership, GroupStage groups, SourceOrder resumesAt, int rows) {
        byte[] after = null;
        do {
            after = membership.reclaim(chain -> Optional.of(resumesAt), groups, after, rows).next();
        } while (after != null);
    }

    @Test
    void anEventFromAnotherChainForAStoredRowIsNotApplied() {
        GroupStageTest.CountingStore store = new GroupStageTest.CountingStore(new OrderedStateStore.Limit(100, 1 << 20));
        MembershipStage membership = new MembershipStage(store);
        byte[] key = OrderedBytes.ofKey(List.of(1L));
        membership.apply(List.of(RowEvent.member(key, "sessions_0", new SourceOrder(1, 5), List.of("p1"), List.of())));

        MembershipStage.Outcome other = membership.apply(List.of(
                RowEvent.member(key, "sessions_1", new SourceOrder(9, 9), List.of("p2"), List.of())));

        assertThat(other.spansInputs()).isEqualTo(1);
        assertThat(other.changes()).isEmpty();
    }

    @Test
    void severalEventsOfOneRowInADrainAreToldAsTheirNetEffect() {
        GroupStageTest.CountingStore store = new GroupStageTest.CountingStore(new OrderedStateStore.Limit(100, 1 << 20));
        MembershipStage membership = new MembershipStage(store);
        membership.apply(List.of(event(1, 1, new Row("EU", new BigDecimal("1.00")))));

        MembershipStage.Outcome told = membership.apply(List.of(
                event(1, 2, new Row("US", new BigDecimal("2.00"))), event(1, 3, new Row("APAC", new BigDecimal("3.00")))));

        assertThat(told.changes()).hasSize(2);
        assertThat(told.changes().get(0).retracts()).isTrue();
        assertThat(told.changes().get(0).group()).containsExactly("EU");
        assertThat(told.changes().get(1).group()).containsExactly("APAC");
        assertThat(told.changes()).allSatisfy(change -> assertThat(change.order()).isEqualTo(new SourceOrder(1, 3)));

        // Delivered again after the commit, the last event ties and the same two things are told again:
        // the row still leaves the group it was in before the drain.
        MembershipStage.Outcome again = membership.apply(List.of(
                event(1, 2, new Row("US", new BigDecimal("2.00"))), event(1, 3, new Row("APAC", new BigDecimal("3.00")))));
        assertThat(again.dropped()).isEqualTo(1);
        assertThat(again.changes()).hasSize(2);
        assertThat(again.changes().get(0).group()).containsExactly("EU");
        assertThat(again.changes().get(1).group()).containsExactly("APAC");
    }
}
