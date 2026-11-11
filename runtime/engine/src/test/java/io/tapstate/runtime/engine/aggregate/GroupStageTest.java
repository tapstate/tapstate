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

        private boolean swept;

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
            if (!swept) {
                throw new UnsupportedOperationException("the data path never scans");
            }
            var range = after == null ? entries.subMap(from, true, to, false) : entries.subMap(after, false, to, false);
            List<Entry> page = new ArrayList<>();
            for (var entry : range.entrySet()) {
                if (page.size() == limit) {
                    break;
                }
                page.add(new Entry(entry.getKey(), entry.getValue()));
            }
            return page;
        }

        /** A store whose rows a sweep may page through; applying events still must not. */
        CountingStore swept() {
            swept = true;
            return this;
        }

        int size() {
            return entries.size();
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
            // Whatever came and went, every group still matches its own rows, and the groups listed are
            // the ones that hold rows.
            store.swept();
            List<List<Object>> listed = new ArrayList<>();
            byte[] after = null;
            do {
                GroupStage.Groups page = stage.groups(after, 2);
                listed.addAll(page.groups());
                after = page.next();
            } while (after != null);
            assertThat(listed).as("groups listed under %s", limit)
                    .containsExactlyInAnyOrderElementsOf(
                            live.values().stream().map(order -> List.<Object>of(order.region())).distinct().toList());
            for (List<Object> group : listed) {
                GroupStage.Audit audit = stage.check(group, 10_000);
                assertThat(audit.verdict()).as("%s under %s: %s", group, limit, audit.fields())
                        .isEqualTo(GroupStage.Verdict.CLEAN);
            }
        }
    }

    private static byte[] totalsKey(String region) {
        byte[] group = io.tapstate.core.dsl.aggregate.OrderedBytes.ofKey(List.of(region));
        byte[] key = new byte[group.length + 1];
        key[0] = 0x01;
        System.arraycopy(group, 0, key, 1, group.length);
        return key;
    }

    private static List<GroupChange> threeOrders() {
        return List.of(
                GroupChange.set(List.of("EU"), id(1), new SourceOrder(1, 1), new Order("EU", new BigDecimal("5.00"), "ann", 1L).inputs()),
                GroupChange.set(List.of("EU"), id(2), new SourceOrder(1, 2), new Order("EU", new BigDecimal("9.00"), "bob", 2L).inputs()),
                GroupChange.set(List.of("US"), id(3), new SourceOrder(1, 3), new Order("US", new BigDecimal("1.00"), "cy", 3L).inputs()));
    }

    @Test
    void totalsThatDoNotMatchTheirRowsAreNamedByFieldAndARepairWritesThemAgain() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)).swept();
        GroupStage stage = new GroupStage(SPEC, store);
        stage.apply(threeOrders().subList(0, 1));
        byte[] stale = store.entries.get(totalsKey("EU"));
        stage.apply(threeOrders().subList(1, 3));
        assertThat(stage.check(List.of("EU"), 1000).verdict()).isEqualTo(GroupStage.Verdict.CLEAN);

        // The totals of when the group held one order, beside the entries of two.
        store.entries.put(totalsKey("EU"), stale);

        GroupStage.Audit found = stage.check(List.of("EU"), 1000);
        assertThat(found.verdict()).isEqualTo(GroupStage.Verdict.MISMATCH);
        assertThat(found.fields()).as("the least is 5.00 either way and is not named")
                .containsExactly(GroupStage.MEMBERS, "orders", "revenue", "average", "most", "buyers", "last_buyer");
        assertThat(found.outcome()).isNull();
        assertThat(stage.check(List.of("US"), 1000).verdict()).as("another group is not touched by it")
                .isEqualTo(GroupStage.Verdict.CLEAN);

        GroupStage.Audit repaired = stage.repair(List.of("EU"), 1000);

        assertThat(repaired.verdict()).isEqualTo(GroupStage.Verdict.MISMATCH);
        assertThat(repaired.outcome().send()).isTrue();
        assertThat(repaired.outcome().members()).isEqualTo(2);
        assertThat(repaired.outcome().values()).containsEntry("orders", 2L)
                .containsEntry("revenue", new BigDecimal("14.00")).containsEntry("most", new BigDecimal("9.00"));
        assertThat(stage.check(List.of("EU"), 1000).verdict()).isEqualTo(GroupStage.Verdict.CLEAN);
    }

    @Test
    void aLostOrderedEntryIsFoundUnderItsFieldAndPutBack() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)).swept();
        GroupStage stage = new GroupStage(SPEC, store);
        stage.apply(threeOrders());
        byte[] group = io.tapstate.core.dsl.aggregate.OrderedBytes.ofKey(List.of("EU"));
        // Field 3 is "least": drop the entry of its smallest value.
        byte[] prefix = Arrays.copyOf(group, group.length + 3);
        prefix[group.length] = 0x03;
        prefix[group.length + 2] = 3;
        byte[] lost = store.entries.ceilingKey(prefix);
        assertThat(Arrays.copyOf(lost, prefix.length)).isEqualTo(prefix);
        store.entries.remove(lost);

        assertThat(stage.check(List.of("EU"), 1000).fields()).containsExactly("least");
        GroupStage.Audit repaired = stage.repair(List.of("EU"), 1000);

        assertThat(repaired.fields()).containsExactly("least");
        assertThat(store.entries).containsKey(lost);
        assertThat(stage.check(List.of("EU"), 1000).verdict()).isEqualTo(GroupStage.Verdict.CLEAN);
    }

    @Test
    void aRepairOfAGroupThatMatchesWritesItsTotalsAndSendsTheGroupAgain() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)).swept();
        GroupStage stage = new GroupStage(SPEC, store);
        stage.apply(threeOrders());

        GroupStage.Audit repaired = stage.repair(List.of("US"), 1000);

        assertThat(repaired.verdict()).isEqualTo(GroupStage.Verdict.CLEAN);
        assertThat(repaired.outcome().send()).isTrue();
        assertThat(repaired.outcome().values()).containsEntry("revenue", new BigDecimal("1.00"));
    }

    @Test
    void aGroupTooLargeToHoldIsInconclusiveAndNothingIsWritten() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)).swept();
        GroupStage stage = new GroupStage(SPEC, store);
        stage.apply(threeOrders());
        store.entries.put(totalsKey("EU"), store.entries.get(totalsKey("US")));
        int commits = store.commits;

        assertThat(stage.check(List.of("EU"), 3).verdict()).isEqualTo(GroupStage.Verdict.INCONCLUSIVE);
        GroupStage.Audit repair = stage.repair(List.of("EU"), 3);

        assertThat(repair.verdict()).isEqualTo(GroupStage.Verdict.INCONCLUSIVE);
        assertThat(repair.outcome()).isNull();
        assertThat(store.commits).isEqualTo(commits);
    }

    @Test
    void aRepairThatDoesNotFitOneCommitChangesNothing() {
        CountingStore store = new CountingStore(new OrderedStateStore.Limit(100, 1 << 20)).swept();
        GroupStage stage = new GroupStage(SPEC, store);
        stage.apply(threeOrders());
        byte[] group = io.tapstate.core.dsl.aggregate.OrderedBytes.ofKey(List.of("EU"));
        // Drop every ordered entry of the group: putting them back takes more than two writes.
        store.entries.subMap(Arrays.copyOf(group, group.length + 1), true, concat(group, 0x05), false)
                .keySet().removeIf(key -> key[group.length] == 0x03);
        GroupStage tight = new GroupStage(SPEC, new Narrowed(store, new OrderedStateStore.Limit(2, 1 << 20)));
        int before = store.entries.size();

        assertThat(tight.repair(List.of("EU"), 1000).verdict()).isEqualTo(GroupStage.Verdict.INCONCLUSIVE);

        assertThat(store.entries).hasSize(before);
    }

    private static byte[] concat(byte[] group, int kind) {
        byte[] key = Arrays.copyOf(group, group.length + 1);
        key[group.length] = (byte) kind;
        return key;
    }

    /** The same entries behind a smaller commit. */
    private record Narrowed(CountingStore store, Limit limit) implements OrderedStateStore {

        @Override
        public List<byte[]> loadAll(List<byte[]> keys) {
            return store.loadAll(keys);
        }

        @Override
        public void commit(List<Entry> saves, List<byte[]> deletes) {
            throw new AssertionError("a repair that does not fit one commit writes nothing");
        }

        @Override
        public Limit commitLimit() {
            return limit;
        }

        @Override
        public Optional<Entry> first(byte[] from, byte[] to, End end) {
            return store.first(from, to, end);
        }

        @Override
        public List<Entry> scan(byte[] from, byte[] to, byte[] after, int limit) {
            return store.scan(from, to, after, limit);
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
