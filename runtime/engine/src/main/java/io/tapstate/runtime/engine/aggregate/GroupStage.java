package io.tapstate.runtime.engine.aggregate;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.AggregateFailure;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.OrderedBytes;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.RunningValue;
import io.tapstate.core.dsl.aggregate.StateKind;
import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.dsl.aggregate.Values;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.runtime.engine.aggregate.GroupChange.FieldInput;
import io.tapstate.spi.store.OrderedStateStore;
import io.tapstate.spi.store.OrderedStateStore.End;
import io.tapstate.spi.store.OrderedStateStore.Entry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The group side of an aggregate step: takes what the membership side says about input rows, keeps each
 * group's stored rows and totals in step, and says which groups to send on.
 *
 * <p><b>Every value of a group is a function of the rows stored for it.</b> Each input row has one stored
 * entry in its group, carrying what the row gives every field and the order of the event that wrote it. A
 * change is applied only when its order is newer than the entry's, so an event delivered again changes
 * nothing; one delivered again with the very order already stored only marks the group to be sent again,
 * since the first sending may have been lost. The totals beside the entries are a convenience that is
 * always written in the same commit as the entries it was computed from.
 *
 * <p>A group's changes go to the store whole: its entries, its totals, and the ordered entries that let a
 * least or greatest value be found again after the current one leaves. Several groups share a commit when
 * they fit. A group with more changes than one commit holds is committed in parts, and each part leaves
 * the group consistent. Nothing is reported for a group until the commit holding it has returned.
 */
public final class GroupStage {

    private static final int TOTALS = 0x01;
    private static final int ROW = 0x02;
    private static final int ORDERED = 0x03;
    private static final int DISTINCT = 0x04;

    private final List<RollupSpec> fields;
    private final OrderedStateStore store;
    private final java.util.function.LongSupplier clock;

    public GroupStage(AggregateStepSpec spec, OrderedStateStore store) {
        this(spec, store, System::currentTimeMillis);
    }

    /** As above, with the clock a group's last good values and the start of a failure are dated by. */
    public GroupStage(AggregateStepSpec spec, OrderedStateStore store, java.util.function.LongSupplier clock) {
        this.clock = clock;
        this.fields = spec.fields();
        this.store = store;
        if (fields.size() > 0xFFFF) {
            throw new IllegalArgumentException("more fields than a group's keys can number");
        }
    }

    /**
     * What became of one group.
     *
     * @param group     the values of the group's key
     * @param send      whether the group is to be sent on: something changed, or an event already applied
     *                  arrived again
     * @param members   how many input rows the group holds; a group with none is sent as a delete
     * @param values    field name to value, when every field can be given exactly
     * @param failure   why the group cannot be given, or null; {@code values} is then empty
     * @param failureArgs what the failure knows about itself
     * @param failedField the field the failure belongs to
     * @param conflicts events that carried an order already stored with different content, and were not applied
     * @param dropped   events older than what is stored
     * @param lastGood  the values the group last had when every field could be given, or null when it never had any
     * @param lastGoodAt when those values were first reached, in epoch milliseconds
     * @param failedSince when the group stopped being givable, in epoch milliseconds; zero while it is
     */
    public record GroupOutcome(List<Object> group, boolean send, long members, Map<String, Object> values,
            RollupError failure, Map<String, Object> failureArgs, String failedField, int conflicts, int dropped,
            Map<String, Object> lastGood, long lastGoodAt, long failedSince) {

        GroupOutcome(List<Object> group, boolean send, long members, Map<String, Object> values,
                RollupError failure, Map<String, Object> failureArgs, String failedField, int conflicts, int dropped) {
            this(group, send, members, values, failure, failureArgs, failedField, conflicts, dropped, null, 0, 0);
        }
    }

    /** Applies one drain's changes and returns the outcome of every group they named, after committing. */
    public List<GroupOutcome> apply(List<GroupChange> changes) {
        TreeMap<byte[], List<GroupChange>> byGroup = new TreeMap<>(Arrays::compareUnsigned);
        for (GroupChange change : changes) {
            byGroup.computeIfAbsent(OrderedBytes.ofKey(change.group()), key -> new ArrayList<>()).add(change);
        }
        Deque<Map.Entry<byte[], List<GroupChange>>> waiting = new ArrayDeque<>(byGroup.entrySet());
        Map<Key, GroupOutcome> outcomes = new LinkedHashMap<>();
        List<Fold> batch = new ArrayList<>();
        Budget used = new Budget();
        while (!waiting.isEmpty()) {
            Map.Entry<byte[], List<GroupChange>> next = waiting.pollFirst();
            List<GroupChange> all = next.getValue();
            int take = all.size();
            Fold fold = fold(next.getKey(), all);
            while (!fold.fits(new Budget(), store.commitLimit())) {
                if (take == 1) {
                    throw new IllegalStateException("one input row of an aggregate group needs more than a "
                            + "whole commit of " + store.commitLimit());
                }
                take = take / 2;
                fold = fold(next.getKey(), all.subList(0, take));
            }
            if (!fold.fits(used, store.commitLimit())) {
                commit(batch, outcomes);
                used = new Budget();
            }
            batch.add(fold);
            used.add(fold);
            if (take < all.size()) {
                // The rest of this group is read only once this part of it is stored.
                commit(batch, outcomes);
                used = new Budget();
                waiting.addFirst(Map.entry(next.getKey(), all.subList(take, all.size())));
            }
        }
        commit(batch, outcomes);
        return List.copyOf(outcomes.values());
    }

    /**
     * A row's leaving of a group that cannot be delivered again.
     *
     * @param group    the values of the group the row left
     * @param inputKey the row
     * @param order    the order of the event that took the row out
     */
    public record Leaving(List<Object> group, byte[] inputKey, SourceOrder order) {
    }

    /**
     * Drops the record that a row left a group, for leavings nothing can deliver again. The record exists
     * to recognise that event and the ones before it when they are delivered twice; once none of them can
     * be, it recognises nothing. A row that has since come back, or left again later, is not touched.
     *
     * @return how many records were dropped
     */
    public int forget(List<Leaving> leavings) {
        List<byte[]> keys = new ArrayList<>();
        for (Leaving leaving : leavings) {
            keys.add(rowKey(OrderedBytes.ofKey(leaving.group()), leaving.inputKey()));
        }
        List<byte[]> loaded = keys.isEmpty() ? List.of() : store.loadAll(keys);
        TreeSet<byte[]> deletes = new TreeSet<>(Arrays::compareUnsigned);
        for (int i = 0; i < keys.size(); i++) {
            if (loaded.get(i) == null) {
                continue;
            }
            StoredRow stored = readRow(loaded.get(i));
            if (stored.payload == null && stored.order.compareTo(leavings.get(i).order()) <= 0) {
                deletes.add(keys.get(i));
            }
        }
        List<byte[]> part = new ArrayList<>();
        for (byte[] key : deletes) {
            part.add(key);
            if (part.size() == store.commitLimit().operations()) {
                store.commit(List.of(), part);
                part = new ArrayList<>();
            }
        }
        if (!part.isEmpty()) {
            store.commit(List.of(), part);
        }
        return deletes.size();
    }

    // ---- checking a group against its own rows --------------------------------------------------

    /** What a check or a repair of one group found. */
    public enum Verdict {
        /** Everything derived from the group's rows is what those rows give. */
        CLEAN,
        /** Something derived differs from what the rows give; a repair has rewritten it. */
        MISMATCH,
        /** The group holds more than could be read or rewritten in one go; nothing is claimed or changed. */
        INCONCLUSIVE
    }

    /** The name a difference in how many rows a group counts is reported under. */
    public static final String MEMBERS = "(rows)";

    /**
     * @param group   the group's key values
     * @param verdict what was found
     * @param fields  the fields whose derived state differed, in declared order, with {@link #MEMBERS}
     *                first when the count of rows itself differed
     * @param outcome after a repair, the group as it is now, to be sent on; null after a check
     * @param failure why the group, computed from its rows, cannot be given exactly; null when it can
     * @param failedField the field that failure is about
     */
    public record Audit(List<Object> group, Verdict verdict, List<String> fields, GroupOutcome outcome,
            RollupError failure, String failedField) {

        Audit(List<Object> group, Verdict verdict, List<String> fields, GroupOutcome outcome) {
            this(group, verdict, fields, outcome, null, null);
        }
    }

    /**
     * A page of the groups that hold rows.
     *
     * @param next where the next page starts, or null when this page was the last
     */
    public record Groups(List<List<Object>> groups, byte[] next) {
    }

    /** The groups holding at least one row, in key order, {@code limit} at a time. */
    public Groups groups(byte[] after, int limit) {
        List<Entry> page = store.scan(new byte[] {(byte) TOTALS}, new byte[] {(byte) ROW}, after, limit);
        List<List<Object>> groups = new ArrayList<>();
        for (Entry entry : page) {
            groups.add(readTotals(entry.state()).group);
        }
        return new Groups(groups, page.size() < limit ? null : page.getLast().key());
    }

    /**
     * Compares everything derived for {@code group} with what its stored rows give, and changes nothing.
     * It says whether the totals match the rows the step holds, and nothing about whether the step holds
     * the rows the source has.
     *
     * <p>It must run on the thread that applies events, between drains: nothing then writes the group
     * while it is read, so a difference is a defect and never a matter of timing.
     *
     * @param most how many entries of the group may be held in memory; a larger group is inconclusive
     */
    public Audit check(List<Object> group, int most) {
        Rebuilt rebuilt = rebuild(group, most);
        if (rebuilt == null) {
            return new Audit(group, Verdict.INCONCLUSIVE, List.of(), null);
        }
        GroupOutcome now = outcome(rebuilt.totals, false, 0, 0);
        return new Audit(group, rebuilt.differing.isEmpty() ? Verdict.CLEAN : Verdict.MISMATCH, rebuilt.differing, null,
                now.failure(), now.failedField());
    }

    /**
     * Writes everything derived for {@code group} again from its stored rows, in one commit, and returns
     * the group to be sent on. A row that is wrong or missing is not something this can mend: only the
     * source row changing, or a rebuild, does that.
     */
    public Audit repair(List<Object> group, int most) {
        Rebuilt rebuilt = rebuild(group, most);
        if (rebuilt == null) {
            return new Audit(group, Verdict.INCONCLUSIVE, List.of(), null);
        }
        List<Entry> saves = new ArrayList<>();
        List<byte[]> deletes = new ArrayList<>();
        long bytes = 0;
        for (Map.Entry<byte[], byte[]> wanted : rebuilt.wanted.entrySet()) {
            if (!Arrays.equals(wanted.getValue(), rebuilt.held.get(wanted.getKey()))) {
                saves.add(new Entry(wanted.getKey(), wanted.getValue()));
                bytes += wanted.getKey().length + wanted.getValue().length;
            }
        }
        for (byte[] held : rebuilt.held.keySet()) {
            if (!rebuilt.wanted.containsKey(held)) {
                deletes.add(held);
                bytes += held.length;
            }
        }
        byte[] totalsKey = totalsKey(rebuilt.fold.group);
        if (rebuilt.totals.members == 0) {
            if (rebuilt.storedTotals != null) {
                deletes.add(totalsKey);
                bytes += totalsKey.length;
            }
        } else {
            remember(rebuilt.totals, outcome(rebuilt.totals, true, 0, 0));
            byte[] written = writeTotals(rebuilt.totals);
            saves.add(new Entry(totalsKey, written));
            bytes += totalsKey.length + written.length;
        }
        OrderedStateStore.Limit limit = store.commitLimit();
        if (saves.size() + deletes.size() > limit.operations() || bytes > limit.bytes()) {
            return new Audit(group, Verdict.INCONCLUSIVE, List.of(), null);
        }
        if (!saves.isEmpty() || !deletes.isEmpty()) {
            store.commit(saves, deletes);
        }
        GroupOutcome now = outcome(rebuilt.totals, true, 0, 0);
        GroupOutcome shown = new GroupOutcome(now.group(), true, now.members(), now.values(), now.failure(),
                now.failureArgs(), now.failedField(), 0, 0,
                rebuilt.totals.lastGood, rebuilt.totals.lastGoodAt, rebuilt.totals.failedSince);
        return new Audit(group, rebuilt.differing.isEmpty() ? Verdict.CLEAN : Verdict.MISMATCH, rebuilt.differing, shown,
                now.failure(), now.failedField());
    }

    /**
     * A group computed again from its rows.
     *
     * @param held    the derived entries the store holds for the group, apart from its totals
     * @param wanted  the derived entries its rows give
     */
    private record Rebuilt(Fold fold, Totals totals, byte[] storedTotals, TreeMap<byte[], byte[]> held,
            TreeMap<byte[], byte[]> wanted, List<String> differing) {
    }

    private Rebuilt rebuild(List<Object> values, int most) {
        byte[] group = OrderedBytes.ofKey(values);
        byte[] storedTotals = store.loadAll(List.of(totalsKey(group))).getFirst();
        byte[] from = key(group, ROW);
        byte[] to = key(group, DISTINCT + 1);
        TreeMap<byte[], byte[]> all = new TreeMap<>(Arrays::compareUnsigned);
        byte[] after = null;
        while (true) {
            List<Entry> page = store.scan(from, to, after, AUDIT_PAGE);
            page.forEach(entry -> all.put(entry.key(), entry.state()));
            if (all.size() > most) {
                return null;
            }
            if (page.size() < AUDIT_PAGE) {
                break;
            }
            after = page.getLast().key();
        }
        byte[] derivedFrom = key(group, ORDERED);
        TreeMap<byte[], byte[]> held = new TreeMap<>(all.subMap(derivedFrom, true, to, false));
        Fold fold = new Fold(group);
        fold.counts = new TreeMap<>(Arrays::compareUnsigned);
        all.subMap(key(group, DISTINCT), true, to, false)
                .forEach((key, state) -> fold.counts.put(key, Count.again(longOf(state))));
        Totals totals = new Totals(values);
        TreeMap<byte[], byte[]> wanted = new TreeMap<>(Arrays::compareUnsigned);
        long unusableDiffers = 0;
        for (Map.Entry<byte[], byte[]> entry : all.subMap(from, true, derivedFrom, false).entrySet()) {
            StoredRow row = readRow(entry.getValue());
            if (row.payload == null) {
                continue;
            }
            List<FieldInput> inputs = inputs(row.payload);
            for (int field = 0; field < fields.size(); field++) {
                if (fields.get(field).function() == RollupFunction.COUNT_DISTINCT && inputs.get(field).in()) {
                    fold.counts.computeIfAbsent(distinctKey(group, field, inputs.get(field).value()),
                            key -> Count.again(0));
                }
            }
            byte[] inputKey = Arrays.copyOfRange(entry.getKey(), from.length, entry.getKey().length);
            long unusable = putIn(fold, totals, inputKey, inputs);
            totals.members++;
            if (unusable != row.unusable) {
                unusableDiffers |= unusable ^ row.unusable;
                held.put(entry.getKey(), entry.getValue());
                wanted.put(entry.getKey(), writeRow(new StoredRow(row.order, row.payload, unusable)));
            }
        }
        wanted.putAll(fold.saves);
        fold.counts.forEach((key, count) -> {
            if (count.now > 0) {
                wanted.put(key, longBytes(count.now));
            }
        });
        Totals stored = storedTotals == null ? null : readTotals(storedTotals);
        if (stored != null) {
            // When the group was last good is history, not something its rows give.
            totals.lastGood = stored.lastGood;
            totals.lastGoodAt = stored.lastGoodAt;
            totals.failedSince = stored.failedSince;
        }
        List<String> differing = new ArrayList<>();
        if ((stored == null ? 0 : stored.members) != totals.members) {
            differing.add(MEMBERS);
        }
        for (int field = 0; field < fields.size(); field++) {
            boolean same = (unusableDiffers & bit(field)) == 0
                    && sameEntries(held, wanted, orderedPrefix(group, field), orderedPrefix(group, field + 1))
                    && sameEntries(held, wanted, distinctPrefix(group, field), distinctPrefix(group, field + 1));
            if (same && totals.members > 0) {
                same = stored != null && result(stored, field).equals(result(totals, field));
            }
            if (!same) {
                differing.add(fields.get(field).field());
            }
        }
        return new Rebuilt(fold, totals, storedTotals, held, wanted, List.copyOf(differing));
    }

    private static boolean sameEntries(TreeMap<byte[], byte[]> held, TreeMap<byte[], byte[]> wanted,
            byte[] from, byte[] to) {
        var a = held.subMap(from, true, to, false);
        var b = wanted.subMap(from, true, to, false);
        if (a.size() != b.size()) {
            return false;
        }
        var other = b.entrySet().iterator();
        for (Map.Entry<byte[], byte[]> entry : a.entrySet()) {
            Map.Entry<byte[], byte[]> match = other.next();
            if (!Arrays.equals(entry.getKey(), match.getKey()) || !Arrays.equals(entry.getValue(), match.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static final int AUDIT_PAGE = 256;

    private void commit(List<Fold> batch, Map<Key, GroupOutcome> outcomes) {
        List<Entry> saves = new ArrayList<>();
        List<byte[]> deletes = new ArrayList<>();
        for (Fold fold : batch) {
            fold.saves.forEach((key, state) -> saves.add(new Entry(key, state)));
            deletes.addAll(fold.deletes);
        }
        if (!saves.isEmpty() || !deletes.isEmpty()) {
            store.commit(saves, deletes);
        }
        for (Fold fold : batch) {
            outcomes.merge(new Key(fold.group), fold.outcome, (before, now) -> new GroupOutcome(now.group(),
                    before.send() || now.send(), now.members(), now.values(), now.failure(), now.failureArgs(),
                    now.failedField(),
                    before.conflicts() + now.conflicts(), before.dropped() + now.dropped(),
                    now.lastGood(), now.lastGoodAt(), now.failedSince()));
        }
        batch.clear();
    }

    // ---- one group ----------------------------------------------------------------------------

    private Fold fold(byte[] group, List<GroupChange> changes) {
        Fold fold = new Fold(group);
        List<byte[]> keys = new ArrayList<>();
        keys.add(totalsKey(group));
        TreeMap<byte[], StoredRow> rows = new TreeMap<>(Arrays::compareUnsigned);
        for (GroupChange change : changes) {
            rows.put(change.inputKey(), null);
        }
        rows.keySet().forEach(inputKey -> keys.add(rowKey(group, inputKey)));
        List<byte[]> loaded = store.loadAll(keys);
        Totals totals = loaded.get(0) == null ? new Totals(changes.get(0).group()) : readTotals(loaded.get(0));
        // The map is sorted, so its order is also the order the rows were asked for in.
        int at = 1;
        for (Map.Entry<byte[], StoredRow> row : rows.entrySet()) {
            byte[] state = loaded.get(at++);
            row.setValue(state == null ? null : readRow(state));
        }
        fold.counts = distinctCounts(group, rows, changes);

        boolean touched = false;
        boolean again = false;
        int conflicts = 0;
        int dropped = 0;
        SourceOrder last = null;
        for (GroupChange change : changes) {
            StoredRow stored = rows.get(change.inputKey());
            byte[] payload = change.retracts() ? null : payload(change.inputs());
            int order = stored == null ? 1 : change.order().compareTo(stored.order);
            if (order < 0) {
                dropped++;
                continue;
            }
            if (order == 0) {
                again = true;
                if (!Arrays.equals(payload, stored.payload)) {
                    conflicts++;
                }
                continue;
            }
            if (stored != null && stored.payload != null) {
                takeOut(fold, totals, change.inputKey(), inputs(stored.payload), stored.unusable);
                totals.members--;
            }
            StoredRow now = new StoredRow(change.order(), payload, 0L);
            if (!change.retracts()) {
                now = new StoredRow(change.order(), payload, putIn(fold, totals, change.inputKey(), change.inputs()));
                totals.members++;
            }
            rows.put(change.inputKey(), now);
            fold.saves.put(rowKey(group, change.inputKey()), writeRow(now));
            touched = true;
            last = change.order();
        }
        if (touched) {
            for (int field = 0; field < fields.size(); field++) {
                if (totals.fields[field].stale) {
                    findBest(fold, totals.fields[field], field);
                }
            }
            totals.emptiedAt = totals.members == 0 ? last : null;
            remember(totals, outcome(totals, true, 0, 0));
            if (totals.members == 0) {
                // A group with no rows has nothing a later fold needs: one that finds no totals starts
                // from none, and reports the group as empty again if its last leaving is delivered twice.
                fold.deletes.add(totalsKey(group));
            } else {
                fold.saves.put(totalsKey(group), writeTotals(totals));
            }
            fold.counts.forEach((key, count) -> {
                if (count.now != count.before) {
                    if (count.now == 0) {
                        fold.deletes.add(key);
                    } else {
                        fold.saves.put(key, longBytes(count.now));
                    }
                }
            });
        }
        GroupOutcome now = outcome(totals, touched || again, conflicts, dropped);
        fold.outcome = new GroupOutcome(now.group(), now.send(), now.members(), now.values(), now.failure(),
                now.failureArgs(), now.failedField(), now.conflicts(), now.dropped(),
                totals.lastGood, totals.lastGoodAt, totals.failedSince);
        return fold;
    }

    /**
     * Keeps what a failed group is shown as. Values every field could give replace the last good ones,
     * dated when they were first reached, so storing the same values again changes nothing; the first
     * fold that cannot give the group dates the failure, and later ones leave that date alone.
     */
    private void remember(Totals totals, GroupOutcome now) {
        if (totals.members == 0) {
            totals.lastGood = null;
            totals.lastGoodAt = 0;
            totals.failedSince = 0;
        } else if (now.failure() == null) {
            if (!now.values().equals(totals.lastGood)) {
                totals.lastGood = now.values();
                totals.lastGoodAt = clock.getAsLong();
            }
            totals.failedSince = 0;
        } else if (totals.failedSince == 0) {
            totals.failedSince = clock.getAsLong();
        }
    }

    /** The distinct-value counts this fold can touch, read in one go: those of rows leaving and of rows arriving. */
    private TreeMap<byte[], Count> distinctCounts(byte[] group, Map<byte[], StoredRow> rows, List<GroupChange> changes) {
        TreeMap<byte[], Count> counts = new TreeMap<>(Arrays::compareUnsigned);
        for (int field = 0; field < fields.size(); field++) {
            if (fields.get(field).function() != RollupFunction.COUNT_DISTINCT) {
                continue;
            }
            for (StoredRow row : rows.values()) {
                if (row != null && row.payload != null) {
                    FieldInput input = inputs(row.payload).get(field);
                    if (input.in()) {
                        counts.put(distinctKey(group, field, input.value()), null);
                    }
                }
            }
            for (GroupChange change : changes) {
                if (!change.retracts() && change.inputs().get(field).in()) {
                    counts.put(distinctKey(group, field, change.inputs().get(field).value()), null);
                }
            }
        }
        if (!counts.isEmpty()) {
            List<byte[]> keys = new ArrayList<>(counts.keySet());
            List<byte[]> states = store.loadAll(keys);
            for (int i = 0; i < keys.size(); i++) {
                long held = states.get(i) == null ? 0 : longOf(states.get(i));
                counts.put(keys.get(i), new Count(held));
            }
        }
        return counts;
    }

    /** Adds one row's inputs to the group and returns which fields could not take theirs, one bit a field. */
    private long putIn(Fold fold, Totals totals, byte[] inputKey, List<FieldInput> inputs) {
        if (inputs.size() != fields.size()) {
            throw new IllegalArgumentException("a row gives " + inputs.size() + " inputs to " + fields.size() + " fields");
        }
        long unusable = 0;
        for (int field = 0; field < fields.size(); field++) {
            FieldInput input = inputs.get(field);
            if (!input.in()) {
                continue;
            }
            RollupSpec spec = fields.get(field);
            FieldTotals state = totals.fields[field];
            switch (spec.function()) {
                case COUNT, SUM, AVG -> {
                    try {
                        state.running.add(input.value());
                    } catch (AggregateFailure notOfTheDeclaredType) {
                        unusable |= bit(field);
                        state.unusable++;
                    }
                }
                case COUNT_DISTINCT -> {
                    Count count = fold.counts.get(distinctKey(fold.group, field, input.value()));
                    if (count.now++ == 0) {
                        state.distinct++;
                    }
                    state.inputs++;
                }
                case MIN, MAX, FIRST, LAST -> {
                    if (ordersByValue(spec) && !admits(spec.argumentType(), input.value())) {
                        unusable |= bit(field);
                        state.unusable++;
                        continue;
                    }
                    byte[] key = orderedKey(fold.group, field, sortBytes(spec, input), inputKey);
                    fold.deletes.remove(key);
                    fold.saves.put(key, value(input.value()));
                    state.inputs++;
                    if (!state.stale && (state.bestKey == null || better(spec, key, state.bestKey))) {
                        state.bestKey = key;
                        state.bestValue = input.value();
                    }
                }
            }
        }
        return unusable;
    }

    private void takeOut(Fold fold, Totals totals, byte[] inputKey, List<FieldInput> inputs, long unusable) {
        for (int field = 0; field < fields.size(); field++) {
            FieldInput input = inputs.get(field);
            if (!input.in()) {
                continue;
            }
            RollupSpec spec = fields.get(field);
            FieldTotals state = totals.fields[field];
            if ((unusable & bit(field)) != 0) {
                state.unusable--;
                continue;
            }
            switch (spec.function()) {
                case COUNT, SUM, AVG -> state.running.retract(input.value());
                case COUNT_DISTINCT -> {
                    Count count = fold.counts.get(distinctKey(fold.group, field, input.value()));
                    if (--count.now == 0) {
                        state.distinct--;
                    }
                    state.inputs--;
                }
                case MIN, MAX, FIRST, LAST -> {
                    byte[] key = orderedKey(fold.group, field, sortBytes(spec, input), inputKey);
                    fold.saves.remove(key);
                    fold.deletes.add(key);
                    state.inputs--;
                    if (state.bestKey != null && Arrays.equals(key, state.bestKey)) {
                        state.lost = state.bestKey;
                        state.bestKey = null;
                        state.bestValue = null;
                        state.stale = true;
                    }
                }
            }
        }
    }

    /**
     * Finds a field's winner again after the one it had left: the best of what the store holds, passing
     * over entries this fold is removing, and of what this fold is adding.
     */
    private void findBest(Fold fold, FieldTotals state, int field) {
        state.stale = false;
        if (state.inputs == 0) {
            return;
        }
        RollupSpec spec = fields.get(field);
        End end = highest(spec) ? End.HIGHEST : End.LOWEST;
        byte[] from = orderedPrefix(fold.group, field);
        byte[] to = orderedPrefix(fold.group, field + 1);
        byte[] bestKey = null;
        byte[] bestState = null;
        var added = fold.saves.subMap(from, true, to, false);
        if (!added.isEmpty()) {
            bestKey = end == End.HIGHEST ? added.lastKey() : added.firstKey();
            bestState = added.get(bestKey);
        }
        // The winner that left is still in the store until this fold is committed. Nothing sorted beyond
        // it, so the search starts just short of it and usually ends with its first read.
        byte[] low = end == End.LOWEST && state.lost != null ? Arrays.copyOf(state.lost, state.lost.length + 1) : from;
        byte[] high = end == End.HIGHEST && state.lost != null ? state.lost : to;
        state.lost = null;
        while (true) {
            Optional<Entry> found = store.first(low, high, end);
            if (found.isEmpty()) {
                break;
            }
            byte[] key = found.get().key();
            if (!fold.deletes.contains(key)) {
                if (bestKey == null || better(spec, key, bestKey)) {
                    bestKey = key;
                    bestState = found.get().state();
                }
                break;
            }
            if (end == End.HIGHEST) {
                high = key;
            } else {
                low = Arrays.copyOf(key, key.length + 1);
            }
        }
        if (bestKey == null) {
            throw new IllegalStateException("an aggregate group counts " + state.inputs + " inputs of field "
                    + spec.field() + " and holds no ordered entry for any");
        }
        state.bestKey = bestKey;
        state.bestValue = valueOf(bestState);
    }

    private GroupOutcome outcome(Totals totals, boolean send, int conflicts, int dropped) {
        if (totals.members == 0) {
            return new GroupOutcome(totals.group, send, 0, Map.of(), null, Map.of(), null, conflicts, dropped);
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (int field = 0; field < fields.size(); field++) {
            FieldResult result = result(totals, field);
            if (result.failure != null) {
                return new GroupOutcome(totals.group, send, totals.members, Map.of(), result.failure,
                        result.args, fields.get(field).field(), conflicts, dropped);
            }
            values.put(fields.get(field).field(), result.value);
        }
        return new GroupOutcome(totals.group, send, totals.members, java.util.Collections.unmodifiableMap(values),
                null, Map.of(), null, conflicts, dropped);
    }

    /** What one field of a group comes to: its value, or why it cannot be given. */
    private record FieldResult(Object value, RollupError failure, Map<String, Object> args) {
    }

    private FieldResult result(Totals totals, int field) {
        RollupSpec spec = fields.get(field);
        FieldTotals state = totals.fields[field];
        try {
            if (state.unusable > 0) {
                return new FieldResult(null, RollupError.VALUE_TYPE_MISMATCH,
                        Map.of("declared", spec.argumentType().spelled()));
            }
            boolean contributed;
            Object value;
            if (state.running != null) {
                contributed = state.running.contributed();
                value = state.running.result();
            } else if (spec.function() == RollupFunction.COUNT_DISTINCT) {
                contributed = state.inputs > 0;
                value = state.distinct;
            } else {
                contributed = state.inputs > 0;
                value = state.bestValue;
            }
            if (!contributed && spec.defaultValue() != null) {
                value = spec.defaultValue();
            } else if (spec.convert() != null) {
                value = Values.convert(value, spec.convert());
            }
            return new FieldResult(value, null, Map.of());
        } catch (AggregateFailure failure) {
            return new FieldResult(null, failure.code(), failure.args());
        }
    }

    // ---- what a field orders by and admits ----------------------------------------------------

    private static boolean ordersByValue(RollupSpec spec) {
        return spec.function() == RollupFunction.MIN || spec.function() == RollupFunction.MAX;
    }

    private static boolean highest(RollupSpec spec) {
        return spec.function() == RollupFunction.MAX || spec.function() == RollupFunction.LAST;
    }

    private static byte[] sortBytes(RollupSpec spec, FieldInput input) {
        return ordersByValue(spec) ? OrderedBytes.of(input.value()) : OrderedBytes.ofAll(input.by());
    }

    /** Whether {@code candidate} beats {@code held}: later for max and last, earlier for min and first. */
    private static boolean better(RollupSpec spec, byte[] candidate, byte[] held) {
        int order = Arrays.compareUnsigned(candidate, held);
        return highest(spec) ? order > 0 : order < 0;
    }

    /** Whether a value is of the type its column was declared with; a type nobody declared admits anything. */
    private static boolean admits(ValueType declared, Object value) {
        return switch (declared.type()) {
            case INT64 -> Values.numericKind(value) == TapstateType.INT64;
            case DOUBLE -> Values.numericKind(value) == TapstateType.DOUBLE;
            case DECIMAL -> (value instanceof BigDecimal d
                    && (declared.scale() == null || d.stripTrailingZeros().scale() <= declared.scale()))
                    || value instanceof BigInteger;
            case STRING -> value instanceof CharSequence;
            case BOOLEAN -> value instanceof Boolean;
            default -> true;
        };
    }

    private static long bit(int field) {
        if (field >= Long.SIZE) {
            throw new IllegalArgumentException("only the first " + Long.SIZE + " fields of a step can mark a row "
                    + "they cannot use");
        }
        return 1L << field;
    }

    // ---- keys ---------------------------------------------------------------------------------

    private static byte[] key(byte[] group, int kind) {
        byte[] key = Arrays.copyOf(group, group.length + 1);
        key[group.length] = (byte) kind;
        return key;
    }

    /**
     * Where a group's totals are kept: the kind first, then the group. Every other entry of a group
     * starts with the group, which keeps a group's entries together; the totals start with their kind,
     * which keeps the totals of all groups together, so the groups can be listed without reading their rows.
     * No group's bytes start with a kind: the first byte of a value is its type's tag, and no tag is a kind.
     */
    private static byte[] totalsKey(byte[] group) {
        return concat(new byte[] {(byte) TOTALS}, group);
    }

    private static byte[] distinctPrefix(byte[] group, int field) {
        byte[] key = Arrays.copyOf(group, group.length + 3);
        key[group.length] = (byte) DISTINCT;
        key[group.length + 1] = (byte) (field >>> 8);
        key[group.length + 2] = (byte) field;
        return key;
    }

    private static byte[] rowKey(byte[] group, byte[] inputKey) {
        return concat(key(group, ROW), inputKey);
    }

    private static byte[] orderedPrefix(byte[] group, int field) {
        byte[] key = Arrays.copyOf(group, group.length + 3);
        key[group.length] = (byte) ORDERED;
        key[group.length + 1] = (byte) (field >>> 8);
        key[group.length + 2] = (byte) field;
        return key;
    }

    private static byte[] orderedKey(byte[] group, int field, byte[] sort, byte[] inputKey) {
        return concat(concat(orderedPrefix(group, field), sort), inputKey);
    }

    private static byte[] distinctKey(byte[] group, int field, Object value) {
        byte[] prefix = Arrays.copyOf(group, group.length + 3);
        prefix[group.length] = (byte) DISTINCT;
        prefix[group.length + 1] = (byte) (field >>> 8);
        prefix[group.length + 2] = (byte) field;
        return concat(prefix, OrderedBytes.of(value));
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] joined = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, joined, a.length, b.length);
        return joined;
    }

    // ---- what is held while folding -----------------------------------------------------------

    private record Key(byte[] bytes) {
        @Override
        public boolean equals(Object other) {
            return other instanceof Key key && Arrays.equals(bytes, key.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static final class Count {
        private final long before;
        private long now;

        Count(long held) {
            this.before = held;
            this.now = held;
        }

        /** A count held as {@code held} and counted again from nothing. */
        static Count again(long held) {
            Count count = new Count(held);
            count.now = 0;
            return count;
        }
    }

    private static final class Budget {
        private int operations;
        private long bytes;

        void add(Fold fold) {
            operations += fold.operations();
            bytes += fold.bytes();
        }
    }

    private static final class Fold {
        private final byte[] group;
        private final TreeMap<byte[], byte[]> saves = new TreeMap<>(Arrays::compareUnsigned);
        private final TreeSet<byte[]> deletes = new TreeSet<>(Arrays::compareUnsigned);
        private TreeMap<byte[], Count> counts;
        private GroupOutcome outcome;

        Fold(byte[] group) {
            this.group = group;
        }

        int operations() {
            return saves.size() + deletes.size();
        }

        long bytes() {
            long bytes = 0;
            for (Map.Entry<byte[], byte[]> save : saves.entrySet()) {
                bytes += save.getKey().length + save.getValue().length;
            }
            for (byte[] delete : deletes) {
                bytes += delete.length;
            }
            return bytes;
        }

        boolean fits(Budget used, OrderedStateStore.Limit limit) {
            return used.operations + operations() <= limit.operations() && used.bytes + bytes() <= limit.bytes();
        }
    }

    /** One input row as stored in its group: the order that wrote it, and what it gives, or nothing once it left. */
    private record StoredRow(SourceOrder order, byte[] payload, long unusable) {
    }

    private final class Totals {
        private final List<Object> group;
        private long members;
        private SourceOrder emptiedAt;
        private Map<String, Object> lastGood;
        private long lastGoodAt;
        private long failedSince;
        private final FieldTotals[] fields;

        Totals(List<Object> group) {
            this.group = group;
            this.fields = new FieldTotals[GroupStage.this.fields.size()];
            for (int field = 0; field < fields.length; field++) {
                RollupSpec spec = GroupStage.this.fields.get(field);
                fields[field] = new FieldTotals();
                if (Aggregates.of(spec.function()).stateKind() == StateKind.VALUE) {
                    fields[field].running = RunningValue.of(spec.function(), spec.argumentType());
                }
            }
        }
    }

    private static final class FieldTotals {
        private RunningValue running;
        private long inputs;
        private long unusable;
        private long distinct;
        private byte[] bestKey;
        private Object bestValue;
        private boolean stale;
        /** The winner that left, when one did: nothing beyond it is left to find. */
        private byte[] lost;
    }

    // ---- stored forms -------------------------------------------------------------------------

    private byte[] writeTotals(Totals totals) {
        return written(out -> {
            out.writeByte(1);
            StoredValues.writeAll(out, totals.group);
            out.writeLong(totals.members);
            out.writeBoolean(totals.emptiedAt != null);
            if (totals.emptiedAt != null) {
                out.writeLong(totals.emptiedAt.epoch());
                out.writeLong(totals.emptiedAt.seq());
            }
            for (int field = 0; field < fields.size(); field++) {
                FieldTotals state = totals.fields[field];
                out.writeLong(state.unusable);
                if (state.running != null) {
                    StoredValues.bytes(out, state.running.state());
                    continue;
                }
                out.writeLong(state.inputs);
                out.writeLong(state.distinct);
                out.writeBoolean(state.bestKey != null);
                if (state.bestKey != null) {
                    StoredValues.bytes(out, state.bestKey);
                    StoredValues.write(out, state.bestValue);
                }
            }
            out.writeLong(totals.failedSince);
            out.writeBoolean(totals.lastGood != null);
            if (totals.lastGood != null) {
                out.writeLong(totals.lastGoodAt);
                for (RollupSpec field : fields) {
                    StoredValues.write(out, totals.lastGood.get(field.field()));
                }
            }
        });
    }

    private Totals readTotals(byte[] bytes) {
        return read(bytes, in -> {
            version(in);
            Totals totals = new Totals(StoredValues.readAll(in));
            totals.members = in.readLong();
            if (in.readBoolean()) {
                totals.emptiedAt = new SourceOrder(in.readLong(), in.readLong());
            }
            for (int field = 0; field < fields.size(); field++) {
                FieldTotals state = totals.fields[field];
                state.unusable = in.readLong();
                if (state.running != null) {
                    RollupSpec spec = fields.get(field);
                    state.running = RunningValue.restore(spec.function(), spec.argumentType(), StoredValues.bytes(in));
                    continue;
                }
                state.inputs = in.readLong();
                state.distinct = in.readLong();
                if (in.readBoolean()) {
                    state.bestKey = StoredValues.bytes(in);
                    state.bestValue = StoredValues.read(in);
                }
            }
            totals.failedSince = in.readLong();
            if (in.readBoolean()) {
                totals.lastGoodAt = in.readLong();
                Map<String, Object> held = new LinkedHashMap<>();
                for (RollupSpec field : fields) {
                    held.put(field.field(), StoredValues.read(in));
                }
                totals.lastGood = java.util.Collections.unmodifiableMap(held);
            }
            return totals;
        });
    }

    private static byte[] writeRow(StoredRow row) {
        return written(out -> {
            out.writeByte(1);
            out.writeLong(row.order.epoch());
            out.writeLong(row.order.seq());
            out.writeBoolean(row.payload != null);
            if (row.payload != null) {
                out.writeLong(row.unusable);
                StoredValues.bytes(out, row.payload);
            }
        });
    }

    private static StoredRow readRow(byte[] bytes) {
        return read(bytes, in -> {
            version(in);
            SourceOrder order = new SourceOrder(in.readLong(), in.readLong());
            if (!in.readBoolean()) {
                return new StoredRow(order, null, 0L);
            }
            long unusable = in.readLong();
            return new StoredRow(order, StoredValues.bytes(in), unusable);
        });
    }

    /** What a row gives the fields, as the bytes two deliveries of one event are compared by. */
    private static byte[] payload(List<FieldInput> inputs) {
        return written(out -> {
            out.writeInt(inputs.size());
            for (FieldInput input : inputs) {
                out.writeBoolean(input.in());
                if (input.in()) {
                    StoredValues.write(out, input.value());
                    StoredValues.writeAll(out, input.by());
                }
            }
        });
    }

    private static List<FieldInput> inputs(byte[] payload) {
        return read(payload, in -> {
            int size = in.readInt();
            List<FieldInput> inputs = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                inputs.add(in.readBoolean() ? new FieldInput(true, StoredValues.read(in), StoredValues.readAll(in))
                        : FieldInput.OUT);
            }
            return inputs;
        });
    }

    private static byte[] value(Object value) {
        return written(out -> StoredValues.write(out, value));
    }

    private static Object valueOf(byte[] bytes) {
        return read(bytes, StoredValues::read);
    }

    private static byte[] longBytes(long value) {
        return written(out -> out.writeLong(value));
    }

    private static long longOf(byte[] bytes) {
        return read(bytes, DataInputStream::readLong);
    }

    private static void version(DataInputStream in) throws IOException {
        int version = in.readUnsignedByte();
        if (version != 1) {
            throw new IOException("aggregate state of version " + version + " is not one this build reads");
        }
    }

    private interface Writing {
        void to(DataOutputStream out) throws IOException;
    }

    private interface Reading<T> {
        T from(DataInputStream in) throws IOException;
    }

    private static byte[] written(Writing writing) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writing.to(out);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static <T> T read(byte[] bytes, Reading<T> reading) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return reading.from(in);
        } catch (IOException unreadable) {
            throw new IllegalStateException("aggregate state cannot be read", unreadable);
        }
    }
}
