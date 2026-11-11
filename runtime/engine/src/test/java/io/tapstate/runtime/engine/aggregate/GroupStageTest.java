package io.tapstate.runtime.engine.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.dsl.aggregate.Accumulator;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.Contribution;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.runtime.engine.aggregate.GroupChange.FieldInput;
import io.tapstate.runtime.engine.aggregate.GroupStage.GroupOutcome;
import io.tapstate.spi.store.OrderedStateStore;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** A group's values are what the function library gives for the rows still in it, whatever came and went. */
class GroupStageTest {

    /** A sorted map under the store's contract, counting what the stage asks of it. */
    static final class CountingStore implements OrderedStateStore {
        final TreeMap<byte[], byte[]> entries = new TreeMap<>(Arrays::compareUnsigned);
        final Limit limit;
        int commits;
        int firstReads;

        CountingStore(Limit limit) {
            this.limit = limit;
        }

        @Override
        public List<byte[]> loadAll(List<byte[]> keys) {
            List<byte[]> states = new ArrayList<>();
            keys.forEach(key -> states.add(entries.get(key)));
            return states;
        }

        @Override
        public void commit(List<Entry> saves, List<byte[]> deletes) {
            long bytes = 0;
            TreeMap<byte[], Boolean> seen = new TreeMap<>(Arrays::compareUnsigned);
            for (Entry save : saves) {
                bytes += save.key().length + save.state().length;
                assertThat(seen.put(save.key(), true)).as("a key once in a commit").isNull();
            }
            for (byte[] delete : deletes) {
                bytes += delete.length;
                assertThat(seen.put(delete, true)).as("a key once in a commit").isNull();
            }
            assertThat(saves.size() + deletes.size()).isLessThanOrEqualTo(limit.operations());
            assertThat(bytes).isLessThanOrEqualTo(limit.bytes());
            saves.forEach(save -> entries.put(save.key(), save.state()));
            deletes.forEach(entries::remove);
            commits++;
        }

        @Override
        public Limit commitLimit() {
            return limit;
        }

        @Override
        public Optional<Entry> first(byte[] from, byte[] to, End end) {
            firstReads++;
            var range = entries.subMap(from, true, to, false);
            var found = end == End.LOWEST ? range.firstEntry() : range.lastEntry();
            return Optional.ofNullable(found).map(entry -> new Entry(entry.getKey(), entry.getValue()));
        }

        @Override
        public List<Entry> scan(byte[] from, byte[] to, byte[] after, int limit) {
            throw new UnsupportedOperationException("the data path never scans");
        }
    }

    private static final ValueType MONEY = ValueType.decimal(2);
    private static final ValueType TEXT = ValueType.of(TapstateType.STRING);

    private static RollupSpec field(String name, RollupFunction function, ValueType argument) {
        ValueType result = Aggregates.of(function).resultType(argument);
        return new RollupSpec(name, function, "x", List.of(), null, argument, result, null, null, Set.of());
    }

    private static final List<RollupSpec> FIELDS = List.of(
            field("orders", RollupFunction.COUNT, ValueType.INT64),
            field("revenue", RollupFunction.SUM, MONEY),
            field("average", RollupFunction.AVG, MONEY),
            field("least", RollupFunction.MIN, MONEY),
            field("most", RollupFunction.MAX, MONEY),
            field("buyers", RollupFunction.COUNT_DISTINCT, TEXT),
            field("first_buyer", RollupFunction.FIRST, TEXT),
            field("last_buyer", RollupFunction.LAST, TEXT));

    private static final AggregateStepSpec SPEC = new AggregateStepSpec("sales", List.of("id"), null,
            Map.of("region", "after.region"), Map.of("region", TEXT), FIELDS);

    /** One order as the membership side would describe it. */
    private record Order(String region, BigDecimal amount, String buyer, Long at) {

        List<FieldInput> inputs() {
            FieldInput amountIn = amount == null ? FieldInput.OUT : FieldInput.of(amount);
            FieldInput buyerIn = buyer == null ? FieldInput.OUT : FieldInput.of(buyer);
            FieldInput pick = at == null ? FieldInput.OUT : FieldInput.of(buyer, List.of(at));
            return List.of(FieldInput.of(null), amountIn, amountIn, amountIn, amountIn, buyerIn, pick, pick);
        }
    }

    private static byte[] id(long id) {
        return io.tapstate.core.dsl.aggregate.OrderedBytes.ofKey(List.of(id));
    }

    /** What the library gives for the rows of one group: the answer the stage has to match. */
    private static Map<String, Object> expected(Map<Long, Order> rows) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int f = 0; f < FIELDS.size(); f++) {
            RollupSpec spec = FIELDS.get(f);
            Accumulator fold = Aggregates.of(spec.function()).init(spec.argumentType());
            int field = f;
            rows.forEach((id, order) -> {
                FieldInput input = order.inputs().get(field);
                if (input.in()) {
                    fold.add(new Contribution(input.value(), input.by(), List.of(id)));
                }
            });
            values.put(spec.field(), fold.result());
        }
        return values;
    }

    @Test
    void everyGroupMatchesTheLibraryOverTheRowsStillInIt() {
        for (OrderedStateStore.Limit limit : List.of(new OrderedStateStore.Limit(1000, 1 << 20),
                new OrderedStateStore.Limit(40, 1 << 20), new OrderedStateStore.Limit(1000, 2000))) {
            Random random = new Random(42 + limit.operations());
            CountingStore store = new CountingStore(limit);
            GroupStage stage = new GroupStage(SPEC, store);
            Map<Long, Order> live = new HashMap<>();
            long seq = 0;
            String[] regions = {"EU", "US", "APAC"};
            String[] buyers = {"ann", "bob", "cy", null};
            for (int drain = 0; drain < 150; drain++) {
                List<GroupChange> changes = new ArrayList<>();
                Map<Long, Order> after = new HashMap<>(live);
                for (int i = random.nextInt(1, 25); i > 0; i--) {
                    long id = random.nextInt(30);
                    SourceOrder order = new SourceOrder(1, seq++);
                    Order before = after.get(id);
                    if (before != null && random.nextInt(3) == 0) {
                        changes.add(GroupChange.retract(List.of(before.region()), id(id), order));
                        after.remove(id);
                        continue;
                    }
                    Order now = new Order(regions[random.nextInt(3)],
                            random.nextInt(8) == 0 ? null : BigDecimal.valueOf(random.nextInt(-500, 5000), 2),
                            buyers[random.nextInt(4)], random.nextInt(6) == 0 ? null : (long) random.nextInt(5));
                    if (before != null && !before.region().equals(now.region())) {
                        changes.add(GroupChange.retract(List.of(before.region()), id(id), order));
                    }
                    changes.add(GroupChange.set(List.of(now.region()), id(id), order, now.inputs()));
                    after.put(id, now);
                }
                List<GroupOutcome> outcomes = stage.apply(changes);
                live = after;
                for (GroupOutcome outcome : outcomes) {
                    assertThat(outcome.send()).isTrue();
                    assertThat(outcome.failure()).isNull();
                    Map<Long, Order> rows = new TreeMap<>();
                    live.forEach((id, order) -> {
                        if (List.of(order.region()).equals(outcome.group())) {
                            rows.put(id, order);
                        }
                    });
                    assertThat(outcome.members()).as("members of %s after drain %d", outcome.group(), drain)
                            .isEqualTo(rows.size());
                    if (!rows.isEmpty()) {
                        assertThat(outcome.values()).as("%s after drain %d", outcome.group(), drain)
                                .isEqualTo(expected(rows));
                    }
                }
            }
            assertThat(store.commits).isPositive();
        }
    }

    private static Order order(String region, String amount) {
        return new Order(region, new BigDecimal(amount), "ann", 1L);
    }

    private GroupOutcome only(List<GroupOutcome> outcomes) {
        assertThat(outcomes).hasSize(1);
        return outcomes.get(0);
    }

    @Test
    void anEventDeliveredAgainChangesNothingAndTheGroupIsSentAgain() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20));
        GroupStage stage = new GroupStage(SPEC, store);
        GroupChange change = GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 10), order("EU", "5.00").inputs());
        GroupOutcome first = only(stage.apply(List.of(change)));
        int commits = store.commits;

        GroupOutcome again = only(stage.apply(List.of(change)));

        assertThat(again.send()).isTrue();
        assertThat(again.values()).isEqualTo(first.values());
        assertThat(again.values().get("revenue")).isEqualTo(new BigDecimal("5.00"));
        assertThat(again.conflicts()).isZero();
        assertThat(store.commits).as("nothing was written for a delivery already applied").isEqualTo(commits);
    }

    @Test
    void anOlderEventIsDroppedAndNothingIsSent() {
        GroupStage stage = new GroupStage(SPEC, new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)));
        stage.apply(List.of(GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 10), order("EU", "5.00").inputs())));

        GroupOutcome older = only(stage.apply(List.of(
                GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 9), order("EU", "7.00").inputs()))));

        assertThat(older.send()).isFalse();
        assertThat(older.dropped()).isEqualTo(1);
        assertThat(older.values().get("revenue")).isEqualTo(new BigDecimal("5.00"));
    }

    @Test
    void theSameOrderWithOtherContentKeepsWhatIsStoredAndIsCounted() {
        GroupStage stage = new GroupStage(SPEC, new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)));
        stage.apply(List.of(GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 10), order("EU", "5.00").inputs())));

        GroupOutcome conflict = only(stage.apply(List.of(
                GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 10), order("EU", "7.00").inputs()))));

        assertThat(conflict.conflicts()).isEqualTo(1);
        assertThat(conflict.send()).isTrue();
        assertThat(conflict.values().get("revenue")).isEqualTo(new BigDecimal("5.00"));
    }

    @Test
    void aGroupThatEmptiesIsSentWithNoMembersAndItsDeleteCanBeSentAgain() {
        GroupStage stage = new GroupStage(SPEC, new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)));
        stage.apply(List.of(GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 10), order("EU", "5.00").inputs())));
        GroupChange leave = GroupChange.retract(List.of("EU"), id(1), new SourceOrder(1, 11));

        GroupOutcome emptied = only(stage.apply(List.of(leave)));
        GroupOutcome again = only(stage.apply(List.of(leave)));

        assertThat(emptied.send()).isTrue();
        assertThat(emptied.members()).isZero();
        assertThat(again.send()).isTrue();
        assertThat(again.members()).isZero();
        // The row that left is remembered, so a replay of the event that wrote it does not bring it back.
        GroupOutcome replayed = only(stage.apply(List.of(
                GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 10), order("EU", "5.00").inputs()))));
        assertThat(replayed.send()).isFalse();
        assertThat(replayed.members()).isZero();
    }

    @Test
    void aGreatestValueThatLeavesIsReplacedWithOneRead() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20));
        GroupStage stage = new GroupStage(SPEC, store);
        List<GroupChange> seed = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            seed.add(GroupChange.set(List.of("EU"), id(i), new SourceOrder(1, i), order("EU", i + ".00").inputs()));
        }
        stage.apply(seed);
        assertThat(store.firstReads).as("adding never reads the order").isZero();

        // Row 20 holds the greatest amount, and with it the last buyer's place among equal times.
        GroupOutcome after = only(stage.apply(List.of(GroupChange.retract(List.of("EU"), id(20), new SourceOrder(1, 99)))));

        assertThat(after.values().get("most")).isEqualTo(new BigDecimal("19.00"));
        assertThat(after.values().get("least")).isEqualTo(new BigDecimal("1.00"));
        assertThat(store.firstReads).as("one read for max, one for last; min and first kept their winners").isEqualTo(2);
    }

    @Test
    void aValueOfAnotherTypeWithholdsTheGroupUntilItsRowIsFixed() {
        GroupStage stage = new GroupStage(SPEC, new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)));
        stage.apply(List.of(GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 1), order("EU", "5.00").inputs())));
        Order wrong = new Order("EU", new BigDecimal("1.005"), "bob", 2L);

        GroupOutcome failed = only(stage.apply(List.of(
                GroupChange.set(List.of("EU"), id(2), new SourceOrder(1, 2), wrong.inputs()))));

        assertThat(failed.failure()).isNotNull();
        assertThat(failed.failure()).isEqualTo(RollupError.VALUE_TYPE_MISMATCH);
        assertThat(failed.failedField()).isEqualTo("revenue");
        assertThat(failed.members()).isEqualTo(2);

        GroupOutcome fixed = only(stage.apply(List.of(
                GroupChange.set(List.of("EU"), id(2), new SourceOrder(1, 3), order("EU", "1.00").inputs()))));
        assertThat(fixed.failure()).isNull();
        assertThat(fixed.values().get("revenue")).isEqualTo(new BigDecimal("6.00"));
        assertThat(fixed.values().get("most")).isEqualTo(new BigDecimal("5.00"));
    }

    @Test
    void anIntegerSumPastSixtyFourBitsWithholdsOnlyItsGroupAndComesBack() {
        AggregateStepSpec spec = new AggregateStepSpec("totals", List.of("id"), null, Map.of("g", "after.g"),
                Map.of("g", TEXT), List.of(field("total", RollupFunction.SUM, ValueType.INT64)));
        GroupStage stage = new GroupStage(spec, new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)));
        List<GroupOutcome> outcomes = stage.apply(List.of(
                GroupChange.set(List.of("a"), id(1), new SourceOrder(1, 1), List.of(FieldInput.of(Long.MAX_VALUE))),
                GroupChange.set(List.of("a"), id(2), new SourceOrder(1, 2), List.of(FieldInput.of(1L))),
                GroupChange.set(List.of("b"), id(3), new SourceOrder(1, 3), List.of(FieldInput.of(7L)))));

        assertThat(outcomes).hasSize(2);
        assertThat(outcomes.get(0).failure()).isEqualTo(RollupError.OVERFLOW);
        assertThat(outcomes.get(1).values()).containsEntry("total", 7L);

        GroupOutcome back = only(stage.apply(List.of(GroupChange.retract(List.of("a"), id(2), new SourceOrder(1, 4)))));
        assertThat(back.failure()).isNull();
        assertThat(back.values()).containsEntry("total", Long.MAX_VALUE);
    }

    @Test
    void theOneGlobalGroupIsAGroupLikeAnyOther() {
        AggregateStepSpec spec = new AggregateStepSpec("all", List.of("id"), null, Map.of(), Map.of(),
                List.of(field("rows", RollupFunction.COUNT, ValueType.INT64), field("most", RollupFunction.MAX, ValueType.INT64)));
        GroupStage stage = new GroupStage(spec, new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)));

        GroupOutcome outcome = only(stage.apply(List.of(
                GroupChange.set(List.of(), id(1), new SourceOrder(1, 1), List.of(FieldInput.of(null), FieldInput.of(4L))),
                GroupChange.set(List.of(), id(2), new SourceOrder(1, 2), List.of(FieldInput.of(null), FieldInput.of(9L))))));

        assertThat(outcome.group()).isEmpty();
        assertThat(outcome.values()).containsEntry("rows", 2L).containsEntry("most", 9L);
    }
}
