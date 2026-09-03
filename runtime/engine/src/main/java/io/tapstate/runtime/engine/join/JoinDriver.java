package io.tapstate.runtime.engine.join;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.Expressions;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The incremental half of a join: what one change to one source means for the flat rows already
 * published, including the changes that arrive on the side nothing is driven from.
 *
 * <p><b>Why this is written rather than taken off the shelf.</b> The substrate's own enrichment
 * operators cover the easy half - a fact row arrives, its dimension is looked up, a row goes out - and
 * do nothing at all for the other half: a dimension row that changes affects every fact row pointing
 * at it, and none of them is arriving. Measured on the substrate's two: a dimension row edited while a
 * job ran produced zero re-emissions, with the job running and nothing reported. Reverse propagation
 * needs an index from dimension key to fact rows and something that drives the re-emission itself,
 * which is what this is.
 *
 * <p><b>Two rules here are correctness and read like tuning.</b>
 *
 * <ul>
 *   <li><b>A recompute re-reads the fact row when it emits it, never the copy it had when the work was
 *       queued.</b> Queuing the row is the more natural implementation - it is in hand at that moment -
 *       and it is wrong in a way nothing reports: a fact row updated while its own recompute is part
 *       way through has the new value emitted first and the queued old one after it, so the target
 *       table settles on the older value and stays there. Re-reading makes both interleavings settle
 *       on the current one; the worst case is the same row sent twice with the same value.
 *   <li><b>Fact rows are read many pages at a time.</b> A recompute reads one fact row per row it
 *       re-emits, and asking for a million of them one at a time is a million round trips - three
 *       orders of magnitude, invisible except as slowness, with the store looking like the culprit.
 *       One page at a time is barely better, and that is the trap: a read is answered by every
 *       partition its keys fall across, each asking the layer beneath separately, so a read smaller
 *       than the partition count is a call per key wearing the batch's name. Measured: eight keys
 *       reached the layer as eight calls. So pages are gathered until a read is far larger than the
 *       partition count, and how large a stored page may be stays a separate question.
 * </ul>
 *
 * <p><b>What the index says is checked against what the fact row says.</b> The index is derived; the
 * truth about which dimension row a fact row points at is the foreign key on the fact row. A bucket
 * may name rows that have since been deleted or re-pointed, so each one is read and asked, and one
 * that no longer belongs is dropped from the bucket rather than emitted. Without that check a stale
 * entry re-publishes a row against a dimension row it has nothing to do with, and the row looks
 * entirely ordinary.
 *
 * <p><b>What this does not keep is the queue itself.</b> Work outstanding when a member dies is not
 * written down; what is written down is the reverse index, from which the work is derivable. Nothing
 * here rebuilds it on the way back up, so a member lost mid-recompute leaves the rows it had not
 * reached unsent until the next change to that dimension row.
 */
public final class JoinDriver {

    private final JoinPlan plan;
    private final JoinStores stores;
    private final String outputStream;
    private final String factSource;
    private final List<String> factKeyColumns;
    private final List<Dimension> dimensions;
    private final Deque<Work> pending = new ArrayDeque<>();
    private final int keysPerRead;

    /**
     * @param plan           what to match on and what to publish
     * @param factKeyColumns the fact row's own identity, which is what a mirror entry and an index
     *                       entry are filed under. It comes from whoever knows the source's key rather
     *                       than from the SQL: the SQL need not select it, and a join that mirrored
     *                       rows under something the SQL happened to name would file two different
     *                       rows under one entry
     * @param outputStream   the name the published changelog carries
     */
    public JoinDriver(JoinPlan plan, List<String> factKeyColumns, String outputStream,
            JoinStores stores) {
        this(plan, factKeyColumns, outputStream, stores, DEFAULT_KEYS_PER_READ);
    }

    /** As above, with the size of one read named - which is what a case needs to be small. */
    public JoinDriver(JoinPlan plan, List<String> factKeyColumns, String outputStream,
            JoinStores stores, int keysPerRead) {
        if (keysPerRead < 1) {
            throw new IllegalArgumentException("a read carries at least one key");
        }
        this.keysPerRead = keysPerRead;
        this.plan = Objects.requireNonNull(plan, "plan");
        this.stores = Objects.requireNonNull(stores, "stores");
        this.outputStream = Objects.requireNonNull(outputStream, "outputStream");
        this.factKeyColumns = List.copyOf(Objects.requireNonNull(factKeyColumns, "factKeyColumns"));
        if (this.factKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("a join needs the fact row's own key to file it under");
        }
        this.factSource = plan.factSource().name();
        this.dimensions = compile(plan.from());
    }

    /** Which source the rows are driven from: the one a change to any other source reaches through. */
    public String factSource() {
        return factSource;
    }

    /** The sources joined onto the fact source, in the order the from clause names them. */
    public List<String> dimensionSources() {
        return dimensions.stream().map(Dimension::source).toList();
    }

    /**
     * Absorbs {@code changes} into the state and pushes as much of the changelog as {@code sink} takes.
     *
     * <p>Absorbing and emitting are separate on purpose. A sink may refuse, and a change whose own row
     * was refused half way through would otherwise have to be either dropped or applied twice; queuing
     * the emission makes a refusal mean "later" for every kind of work in the same way.
     *
     * @return whether nothing is left to send. False means this must be called again - with no changes
     *         at all if none are arriving, which is the only way a large recompute ever finishes
     */
    public boolean apply(List<SourceChange> changes, JoinSink sink) {
        for (SourceChange change : changes) {
            absorb(change);
        }
        return drain(sink);
    }

    /** Whether anything is still waiting to be sent. */
    public boolean hasPending() {
        return !pending.isEmpty();
    }

    private void absorb(SourceChange change) {
        if (change.source().equals(factSource)) {
            absorbFact(change.event());
            return;
        }
        Dimension dimension = dimensionNamed(change.source());
        if (dimension == null) {
            throw new IllegalArgumentException("this join reads no source called " + change.source());
        }
        absorbDimension(dimension, change.event());
    }

    /**
     * A change to the driving source: one row in, one row out - plus, when the row's join key moved,
     * an explicit removal of what it used to be.
     *
     * <p>The removal is not tidiness. Where the published row's identity includes the dimension key -
     * which is what a fan-out target table has to key on - the row under the old key and the row under
     * the new one are two different rows, and writing the new one leaves the old one behind: one order
     * showing under two customers at once, with nothing reporting anything.
     */
    private void absorbFact(Envelope event) {
        Map<String, Object> after = event.after();
        Map<String, Object> before = event.before();
        if (after == null) {
            if (before == null) {
                // A change carrying neither image says nothing about which row it is about.
                return;
            }
            String key = factKeyOf(before);
            Map<String, Object> mirrored = stores.fact(key);
            // The mirror where there is one: a connector's before image may hold the key columns
            // alone, and the published row is built from the whole row.
            Map<String, Object> row = mirrored != null ? mirrored : before;
            queueRow(row, event.ts(), true);
            forget(key, row);
            return;
        }
        String key = factKeyOf(after);
        Map<String, Object> previous = before != null ? before : stores.fact(key);
        if (previous != null) {
            String previousKey = factKeyOf(previous);
            if (!previousKey.equals(key)) {
                // The row's own identity moved, so what was published under the old one is a different
                // row and nothing else will ever remove it.
                queueRow(previous, event.ts(), true);
                forget(previousKey, previous);
                previous = null;
            }
        }
        for (Dimension dimension : dimensions) {
            String was = previous == null ? null : dimensionKeyIn(previous, dimension);
            String now = dimensionKeyIn(after, dimension);
            if (Objects.equals(was, now)) {
                continue;
            }
            if (was != null) {
                stores.indexRemove(dimension.source(), was, key);
                // The old bucket's row is a different published row wherever the identity carries the
                // dimension key, so it is removed rather than overwritten.
                queueRow(previous, event.ts(), true);
            }
            if (now != null) {
                stores.indexAdd(dimension.source(), now, key);
            }
        }
        stores.putFact(key, after);
        queueRow(after, event.ts(), false);
    }

    /**
     * A change on a side nothing is driven from. Nothing about it can be published on its own: what it
     * means is that some set of already-published rows is now wrong, and that set is what the reverse
     * index names.
     *
     * <p>An insert queues work too, and that is the case a matching-only index cannot serve. A fact row
     * that found no dimension row was published with nulls; when the dimension row finally arrives
     * those rows are supposed to become matches, and they can only be found again because they were
     * written into the bucket when they missed.
     */
    private void absorbDimension(Dimension dimension, Envelope event) {
        Map<String, Object> after = event.after();
        Map<String, Object> before = event.before();
        if (before != null) {
            String was = keyOfDimensionRow(before, dimension);
            if (was != null && (after == null || !was.equals(keyOfDimensionRow(after, dimension)))) {
                stores.removeDimensionRow(dimension.source(), was);
                pending.add(new Recompute(dimension, was, 0, 0, event.ts()));
            }
        }
        if (after == null) {
            return;
        }
        String now = keyOfDimensionRow(after, dimension);
        if (now == null) {
            // A dimension row whose join key holds a null matches nothing, by the same rule that makes
            // a null key on the other side match nothing. Keeping it would give it a bucket no fact row
            // can ever name.
            return;
        }
        stores.putDimensionRow(dimension.source(), now, after);
        pending.add(new Recompute(dimension, now, 0, 0, event.ts()));
    }

    /** Pushes queued work until the sink refuses or there is none left. */
    private boolean drain(JoinSink sink) {
        while (!pending.isEmpty()) {
            Work work = pending.peek();
            if (work instanceof Row row) {
                if (!sink.offer(row.event())) {
                    return false;
                }
                pending.poll();
                continue;
            }
            Recompute recompute = (Recompute) work;
            if (!advance(recompute, sink)) {
                return false;
            }
            pending.poll();
        }
        return true;
    }

    /**
     * Walks what is left of one bucket, a page at a time, re-reading each fact row as it emits it.
     * Returns false when the sink refused, having written down where to carry on from - the page and
     * the position within it, so nothing is sent twice and nothing is skipped.
     */
    private boolean advance(Recompute recompute, JoinSink sink) {
        Dimension dimension = recompute.dimension();
        String source = dimension.source();
        String dimensionKey = recompute.dimensionKey();
        while (recompute.page() < stores.indexPageCount(source, dimensionKey)) {
            // Several pages at a time, not one. A page is sized by what one stored entry may hold; a
            // read is answered by every partition the keys fall across, each asking the layer beneath
            // for its own share. A read the size of a page is therefore a handful of keys per partition
            // - measured, one key per call on a small enough page - which is the key-at-a-time read
            // wearing the batch's name. Reading far more keys than there are partitions is what turns
            // it back into a batch.
            int through = recompute.page();
            List<String> gathered = new ArrayList<>();
            int pages = stores.indexPageCount(source, dimensionKey);
            while (through < pages && gathered.size() < keysPerRead) {
                gathered.addAll(stores.indexPage(source, dimensionKey, through));
                through++;
            }
            Map<String, Map<String, Object>> rows = stores.factsUnder(gathered);
            while (recompute.page() < through) {
                if (!emit(recompute, dimension, rows, sink)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Walks what is left of one page, emitting from {@code rows} - which was read for this page and
     * several after it. Returns false when the sink refused, leaving the bookmark where it stopped.
     */
    private boolean emit(Recompute recompute, Dimension dimension,
            Map<String, Map<String, Object>> rows, JoinSink sink) {
        List<String> factKeys =
                stores.indexPage(dimension.source(), recompute.dimensionKey(), recompute.page());
        List<String> stale = new ArrayList<>();
        while (recompute.at() < factKeys.size()) {
            String factKey = factKeys.get(recompute.at());
            Map<String, Object> factRow = rows.get(factKey);
            // The index is derived; the fact row's own foreign key is the truth. A bucket may name a
            // row that has gone or that now points elsewhere - emitting either would publish it
            // against a dimension row it has nothing to do with, and the row would look ordinary.
            if (factRow == null
                    || !recompute.dimensionKey().equals(dimensionKeyIn(factRow, dimension))) {
                stale.add(factKey);
                recompute.at(recompute.at() + 1);
                continue;
            }
            Envelope event = rowEvent(factRow, recompute.ts(), false);
            if (event != null && !sink.offer(event)) {
                // Nothing has been removed yet, so the entries found stale on this pass are simply
                // found again on the next one. Removing them mid-walk would move the positions the
                // bookmark below is written in.
                return false;
            }
            recompute.at(recompute.at() + 1);
        }
        // Dropped once the page is done rather than as they are found: a removal compacts the page,
        // and compacting the list being walked is how a walk skips entries. Dropping them at all is
        // what keeps a bucket from growing for ever under a key nobody ever changes.
        for (String gone : stale) {
            stores.indexRemove(dimension.source(), recompute.dimensionKey(), gone);
        }
        recompute.page(recompute.page() + 1);
        recompute.at(0);
        return true;
    }

    /**
     * How many fact keys one read gathers before it goes out. Far more than the substrate's partition
     * count, because a read is split across the partitions its keys fall in and each of those asks the
     * layer beneath separately: a read smaller than the partition count is a call per key by another
     * name. Pages are gathered until this is reached, so a page's size and a read's size stay two
     * numbers rather than one.
     */
    static final int DEFAULT_KEYS_PER_READ = 8_000;

    /** Queues the published row for {@code factRow}, or its removal. */
    private void queueRow(Map<String, Object> factRow, long ts, boolean removed) {
        if (factRow == null) {
            return;
        }
        Envelope event = rowEvent(factRow, ts, removed);
        if (event != null) {
            pending.add(new Row(event));
        }
    }

    /**
     * The published event for one fact row, or null where this join publishes no row for it - an inner
     * join whose dimension row is missing. A removal is published whatever the match, because a row
     * that never existed being removed is a no-op at an idempotent sink, while a row that did exist and
     * is not removed stays for ever.
     */
    private Envelope rowEvent(Map<String, Object> factRow, long ts, boolean removed) {
        Map<String, Map<String, Object>> sources = new HashMap<>();
        sources.put(factSource, factRow);
        for (Dimension dimension : dimensions) {
            String key = dimensionKeyIn(factRow, dimension);
            Map<String, Object> dimensionRow =
                    key == null ? null : stores.dimensionRow(dimension.source(), key);
            if (dimensionRow == null && dimension.kind() == JoinKind.INNER && !removed) {
                return null;
            }
            sources.put(dimension.source(), dimensionRow);
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (OutputField field : plan.outputFields()) {
            row.put(field.name(),
                    Expressions.coerce(Expressions.evaluate(field.from(), sources), field.type()));
        }
        return removed ? Envelope.delete(ts, outputStream, row, null)
                : Envelope.insert(ts, outputStream, row, null);
    }

    /** Drops a fact row from the mirror and from every bucket it was named in. */
    private void forget(String factKey, Map<String, Object> factRow) {
        for (Dimension dimension : dimensions) {
            String key = dimensionKeyIn(factRow, dimension);
            if (key != null) {
                stores.indexRemove(dimension.source(), key, factKey);
            }
        }
        stores.removeFact(factKey);
    }

    private Dimension dimensionNamed(String source) {
        for (Dimension dimension : dimensions) {
            if (dimension.source().equals(source)) {
                return dimension;
            }
        }
        return null;
    }

    /** Which dimension key this fact row points at, or null where its key holds a null. */
    private String dimensionKeyIn(Map<String, Object> factRow, Dimension dimension) {
        return keyOf(factRow, dimension.factColumns());
    }

    /** Which key this dimension row is filed under, or null where its key holds a null. */
    private String keyOfDimensionRow(Map<String, Object> row, Dimension dimension) {
        return keyOf(row, dimension.dimensionColumns());
    }

    private String factKeyOf(Map<String, Object> row) {
        String key = keyOf(row, factKeyColumns);
        if (key == null) {
            throw new IllegalArgumentException(
                    "a fact row arrived with a null in its own key, so it cannot be filed at all");
        }
        return key;
    }

    /**
     * The one name a set of column values is matched by, or null where any of them is null. Null being
     * a refusal rather than a value is SQL's rule and it is load-bearing here: a hash table treats null
     * as an ordinary key and cheerfully matches every such row with every other, which is a fan-out
     * made of rows that should not have matched at all.
     */
    private static String keyOf(Map<String, Object> row, List<String> columns) {
        List<Object> values = new ArrayList<>(columns.size());
        for (String column : columns) {
            values.add(row.get(column));
        }
        JoinKey key = JoinKey.of(values);
        return key.matchable() ? key.name() : null;
    }

    /**
     * The dimensions this plan joins on, outermost last.
     *
     * <p>Two shapes are refused rather than half-handled, and both would otherwise publish rows that
     * look ordinary: a join whose right side is itself a join (a chain of dimensions, where a change to
     * the far one has to travel through the near one to find its fact rows), and a join keyed on
     * anything but the driving source (the same problem written differently).
     */
    private List<Dimension> compile(JoinTree tree) {
        List<Dimension> collected = new ArrayList<>();
        collect(tree, collected);
        return List.copyOf(collected.reversed());
    }

    private void collect(JoinTree node, List<Dimension> into) {
        if (node instanceof JoinTree.Source) {
            return;
        }
        JoinTree.Join join = (JoinTree.Join) node;
        if (!(join.right() instanceof JoinTree.Source right)) {
            throw new IllegalArgumentException(
                    "this join carrier reads one dimension per join, and this one joins onto a join");
        }
        if (join.hasUncapturedCondition()) {
            throw new IllegalArgumentException("this join's condition holds more than the equalities "
                    + "it was compiled to, so matching on those alone would publish rows that should "
                    + "not exist");
        }
        List<String> factColumns = new ArrayList<>();
        List<String> dimensionColumns = new ArrayList<>();
        for (JoinTree.KeyPair pair : join.on()) {
            if (!pair.left().source().equals(factSource)) {
                throw new IllegalArgumentException("this join carrier matches every dimension against "
                        + "the driving source, and '" + right.name() + "' is matched against '"
                        + pair.left().source() + "'");
            }
            factColumns.add(pair.left().column());
            dimensionColumns.add(pair.right().column());
        }
        if (factColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "'" + right.name() + "' is joined on nothing, which is every row against every row");
        }
        into.add(new Dimension(right.name(), join.kind(), List.copyOf(factColumns),
                List.copyOf(dimensionColumns)));
        collect(join.left(), into);
    }

    /** One source joined onto the driving one, and the columns either side is matched by. */
    private record Dimension(String source, JoinKind kind, List<String> factColumns,
            List<String> dimensionColumns) {
    }

    private sealed interface Work permits Row, Recompute {
    }

    /** One published row waiting for the sink to take it. */
    private record Row(Envelope event) implements Work {
    }

    /**
     * A bucket that has to be walked, and how far the walk has got. Mutable because it is exactly the
     * bookmark a refusal leaves behind: a sink that stops taking rows part way through a million-row
     * fan-out must be able to be offered the rest, in order, without the ones already sent.
     */
    private static final class Recompute implements Work {

        private final Dimension dimension;
        private final String dimensionKey;
        private final long ts;
        private int page;
        private int at;

        private Recompute(Dimension dimension, String dimensionKey, int page, int at, long ts) {
            this.dimension = dimension;
            this.dimensionKey = dimensionKey;
            this.page = page;
            this.at = at;
            this.ts = ts;
        }

        private Dimension dimension() {
            return dimension;
        }

        private String dimensionKey() {
            return dimensionKey;
        }

        private long ts() {
            return ts;
        }

        private int page() {
            return page;
        }

        private void page(int page) {
            this.page = page;
        }

        private int at() {
            return at;
        }

        private void at(int at) {
            this.at = at;
        }
    }
}
