package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one change means for the rows already published, including changes to the side nothing is
 * driven from.
 *
 * <p>Almost every failure here publishes a row that looks entirely ordinary, so most of these cases
 * are about what did <em>not</em> happen: a dimension edit that produced nothing, a bucket that never
 * recorded a miss, an old row that was left behind, a recompute that stopped when the input went
 * quiet.
 */
class JoinDriverTest {

    private static final String STREAM = "order_state";

    @Test
    @DisplayName("a fact row arriving is published with its dimension row joined in")
    void aFactRowIsPublishedJoined() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));

        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));

        assertThat(fixture.published()).containsExactly(
                Map.entry(Op.INSERT, Map.of("order_id", 10L, "customer_name", "Ada")));
    }

    @Test
    @DisplayName("a left join with no dimension row publishes the row with nulls, an inner join publishes nothing")
    void anUnmatchedRowFollowsTheJoinKind() {
        Fixture left = new Fixture(JoinKind.LEFT);
        left.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));
        assertThat(left.published()).singleElement()
                .extracting(Map.Entry::getValue)
                .isEqualTo(rowOf("order_id", 10L, "customer_name", null));

        Fixture inner = new Fixture(JoinKind.INNER);
        inner.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));
        assertThat(inner.published()).isEmpty();
    }

    /**
     * The case a matching-only index cannot serve. The fact row missed, so an index built from matches
     * has no record that it exists; when the dimension row finally arrives, those rows stay null for
     * ever with nothing reporting it.
     */
    @Test
    @DisplayName("a dimension row arriving later turns rows that missed it into matches")
    void aLateDimensionRowTurnsMissesIntoMatches() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));
        fixture.clear();

        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));

        assertThat(fixture.published()).containsExactly(
                Map.entry(Op.INSERT, Map.of("order_id", 10L, "customer_name", "Ada")));
    }

    /**
     * The half the substrate's own enrichment operators do not do at all: measured on both of them, a
     * dimension row edited while a job ran produced zero re-emissions, with the job running and nothing
     * reported.
     */
    @Test
    @DisplayName("a dimension row changing re-emits every fact row pointing at it")
    void aDimensionChangeReEmitsItsFactRows() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));
        fixture.apply(fact(insert(Map.of("id", 11L, "cust_id", 1L))));
        fixture.apply(fact(insert(Map.of("id", 12L, "cust_id", 2L))));
        fixture.clear();

        fixture.apply(dimension("c", update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace"))));

        assertThat(fixture.published()).containsExactlyInAnyOrder(
                Map.entry(Op.INSERT, Map.of("order_id", 10L, "customer_name", "Grace")),
                Map.entry(Op.INSERT, Map.of("order_id", 11L, "customer_name", "Grace")));
    }

    /**
     * The contract that keeps a large recompute from stopping half way. A stream that has caught up
     * stops delivering, so if the only moment work were pushed were the arrival of the next change,
     * the rest of a fan-out would never be sent - with the job running, no errors, and the target table
     * half updated.
     */
    @Test
    @DisplayName("a recompute finishes while nothing at all is arriving")
    void aRecomputeIsPushedWithNoInput() {
        Fixture fixture = new Fixture(JoinKind.LEFT, 2);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        for (long id = 0; id < 5; id++) {
            fixture.apply(fact(insert(Map.of("id", id, "cust_id", 1L))));
        }
        fixture.clear();
        fixture.sink.takeAtMost(1);

        assertThat(fixture.driver.apply(List.of(
                dimension("c", update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace")))),
                fixture.sink)).as("refused after one row, so there is more to do").isFalse();

        fixture.sink.takeAtMost(Integer.MAX_VALUE);
        // Nothing arriving at all, which is the ordinary state of a stream that has caught up.
        assertThat(fixture.driver.apply(List.of(), fixture.sink)).isTrue();
        assertThat(fixture.published()).hasSize(5)
                .allSatisfy(entry -> assertThat(entry.getValue())
                        .containsEntry("customer_name", "Grace"));
    }

    /**
     * A sink refusing part way through must be able to be offered the rest, in order, without the ones
     * it already took. Both failures are silent at the sink: a duplicate is absorbed by an idempotent
     * write, and a gap is a row that simply never updates.
     */
    @Test
    @DisplayName("a refused fan-out resumes with no gaps and no repeats")
    void aRefusedFanOutResumesExactlyWhereItStopped() {
        Fixture fixture = new Fixture(JoinKind.LEFT, 2);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        for (long id = 0; id < 7; id++) {
            fixture.apply(fact(insert(Map.of("id", id, "cust_id", 1L))));
        }
        fixture.clear();

        fixture.sink.takeAtMost(3);
        fixture.driver.apply(List.of(dimension("c",
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace")))), fixture.sink);
        fixture.sink.takeAtMost(Integer.MAX_VALUE);
        fixture.drainFully();

        List<Object> ids = fixture.published().stream()
                .map(entry -> entry.getValue().get("order_id")).toList();
        assertThat(ids).containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L, 6L);
    }

    /**
     * The rule that reads like tuning and is correctness. Queuing the fact row when the work is queued
     * is the more natural implementation, and it loses a concurrent update: the new value is emitted
     * first and the queued old one lands after it, so the target table settles on the older value.
     */
    @Test
    @DisplayName("a recompute emits the fact row as it is when it is sent, not as it was when it was queued")
    void aRecomputeReReadsTheFactRow() {
        Fixture fixture = new Fixture(JoinKind.LEFT, 1);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L, "note", "first"))));
        fixture.apply(fact(insert(Map.of("id", 11L, "cust_id", 1L, "note", "first"))));
        fixture.clear();

        fixture.sink.takeAtMost(1);
        fixture.driver.apply(List.of(dimension("c",
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace")))), fixture.sink);
        // Both fact rows are edited while the recompute is part way through, and nothing is taken while
        // that happens - so everything published afterwards is published by a recompute that was
        // already under way when the rows changed.
        fixture.sink.takeAtMost(0);
        fixture.driver.apply(List.of(
                fact(update(Map.of("id", 10L, "cust_id", 1L, "note", "first"),
                        Map.of("id", 10L, "cust_id", 1L, "note", "second"))),
                fact(update(Map.of("id", 11L, "cust_id", 1L, "note", "first"),
                        Map.of("id", 11L, "cust_id", 1L, "note", "second")))), fixture.sink);
        fixture.clear();
        fixture.sink.takeAtMost(Integer.MAX_VALUE);
        fixture.drainFully();

        // Not "the last one is right": a recompute holding the row it was queued with would publish the
        // older note here, and the newer one would land afterwards anyway - so the last value is the
        // same under both implementations and says nothing. What differs is whether the older value is
        // published at all.
        assertThat(fixture.notesPublished())
                .as("nothing carries the value the row held when the work was queued")
                .doesNotContain("first");
        assertThat(fixture.notesPublished()).contains("second");
    }

    /**
     * Where the published row's identity carries the dimension key - which is what a fan-out target
     * table has to key on - the row under the old key and the row under the new one are two different
     * rows. Writing only the new one leaves the old one behind: one order under two customers at once.
     */
    @Test
    @DisplayName("a fact row re-pointed at another dimension key removes what it published under the old one")
    void aRePointedFactRowRemovesItsOldRow() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        fixture.apply(dimension("c", insert(Map.of("id", 2L, "name", "Grace"))));
        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));
        fixture.clear();

        fixture.apply(fact(update(Map.of("id", 10L, "cust_id", 1L), Map.of("id", 10L, "cust_id", 2L))));

        assertThat(fixture.published()).containsExactly(
                Map.entry(Op.DELETE, Map.of("order_id", 10L, "customer_name", "Ada")),
                Map.entry(Op.INSERT, Map.of("order_id", 10L, "customer_name", "Grace")));
    }

    @Test
    @DisplayName("a fact row deleted is removed and stops being reached by its dimension key")
    void aDeletedFactRowIsRemovedAndUnindexed() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 1L))));
        fixture.clear();

        fixture.apply(fact(delete(Map.of("id", 10L, "cust_id", 1L))));
        assertThat(fixture.published()).containsExactly(
                Map.entry(Op.DELETE, Map.of("order_id", 10L, "customer_name", "Ada")));

        fixture.clear();
        fixture.apply(dimension("c",
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace"))));
        assertThat(fixture.published()).as("nothing points at it any more").isEmpty();
    }

    /**
     * The index is derived and may name a row that has since gone or moved. Emitting from it without
     * asking the fact row would publish that row against a dimension row it has nothing to do with -
     * and the published row would look entirely ordinary.
     */
    @Test
    @DisplayName("an index entry the fact row no longer agrees with is dropped rather than emitted")
    void aStaleIndexEntryIsNeverEmitted() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        fixture.apply(fact(insert(Map.of("id", 10L, "cust_id", 2L))));
        fixture.clear();
        // A bucket naming a row that points somewhere else: what a rebuild or a missed removal leaves.
        String bucket = fixture.dimensionKeyOf(1L);
        fixture.stores.indexAdd("c", bucket, fixture.onlyFactKey());

        fixture.apply(dimension("c",
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace"))));

        assertThat(fixture.published()).isEmpty();
        assertThat(fixture.stores.indexPageCount("c", bucket))
                .as("and it is gone, so the bucket does not grow for ever").isZero();
    }

    /**
     * A million keys asked one at a time against a remote store is a million round trips. One page at a
     * time is barely better: a read is answered by every partition its keys fall across, so a read the
     * size of a small page is a call per key wearing the batch's name. Neither answers differently from
     * the other, so nothing but the clock says which is happening - which is why this counts the asking.
     */
    @Test
    @DisplayName("fact rows are read across pages in one go, not a page at a time")
    void factRowsAreReadAcrossPagesInOneBatch() {
        Fixture fixture = new Fixture(JoinKind.LEFT, 4);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));
        for (long id = 0; id < 8; id++) {
            fixture.apply(fact(insert(Map.of("id", id, "cust_id", 1L))));
        }
        assertThat(fixture.stores.indexPageCount("c", fixture.dimensionKeyOf(1L)))
                .as("two pages, so a page-at-a-time read is visible as two") .isEqualTo(2);
        fixture.stores.batchReads = 0;
        fixture.stores.keysRead = 0;

        fixture.apply(dimension("c",
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace"))));

        assertThat(fixture.stores.keysRead).isEqualTo(8);
        assertThat(fixture.stores.batchReads).as("both pages in one read, not one read each")
                .isEqualTo(1);
    }

    /**
     * The first load and the changes after it, through the one method that serves both. A source
     * delivers its snapshot as read events on the stream its later changes arrive on, so there is no
     * second phase to run - and the rows a snapshot arrives in no useful order still have to converge.
     *
     * <p>The order here is the hostile one on purpose: every fact row before any dimension row, which
     * is what a snapshot over two independent tables routinely produces. Each of those rows publishes
     * with nulls first and is corrected when its dimension row lands, which is the same mechanism a
     * late dimension row uses at any other time - not a separate one for loading.
     */
    @Test
    @DisplayName("the first load and the changes after it come out of the same call")
    void theSnapshotAndTheChangesAfterItShareOnePath() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        for (long id = 0; id < 3; id++) {
            fixture.apply(fact(read(Map.of("id", id, "cust_id", 1L))));
        }
        fixture.apply(dimension("c", read(Map.of("id", 1L, "name", "Ada"))));
        fixture.clear();

        // A change arriving after the load is applied by the same call, and lands on the rows the load
        // left behind rather than on a second copy of them.
        fixture.apply(dimension("c",
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace"))));

        assertThat(fixture.published()).containsExactlyInAnyOrder(
                Map.entry(Op.INSERT, Map.of("order_id", 0L, "customer_name", "Grace")),
                Map.entry(Op.INSERT, Map.of("order_id", 1L, "customer_name", "Grace")),
                Map.entry(Op.INSERT, Map.of("order_id", 2L, "customer_name", "Grace")));
    }

    /**
     * The load itself, in that hostile order: what it settles on. Not "no nulls were ever published" -
     * they are, and that is the mechanism working - but that the last thing said about each row is the
     * joined one.
     */
    @Test
    @DisplayName("a snapshot whose fact rows arrive before their dimension rows still converges")
    void aSnapshotConvergesWhateverOrderItArrivesIn() {
        Fixture fixture = new Fixture(JoinKind.LEFT);

        for (long id = 0; id < 3; id++) {
            fixture.apply(fact(read(Map.of("id", id, "cust_id", 1L))));
        }
        fixture.apply(dimension("c", read(Map.of("id", 1L, "name", "Ada"))));

        Map<Object, Object> settled = new LinkedHashMap<>();
        for (Map.Entry<Op, Map<String, Object>> row : fixture.published()) {
            settled.put(row.getValue().get("order_id"), row.getValue().get("customer_name"));
        }
        assertThat(settled).containsExactly(
                Map.entry(0L, "Ada"), Map.entry(1L, "Ada"), Map.entry(2L, "Ada"));
    }

    @Test
    @DisplayName("a null in a join key matches nothing, on either side")
    void aNullJoinKeyMatchesNothing() {
        Fixture fixture = new Fixture(JoinKind.LEFT);
        fixture.apply(dimension("c", insert(Map.of("id", 1L, "name", "Ada"))));

        Map<String, Object> withNullKey = new LinkedHashMap<>();
        withNullKey.put("id", 10L);
        withNullKey.put("cust_id", null);
        fixture.apply(fact(insert(withNullKey)));

        assertThat(fixture.published()).singleElement().extracting(Map.Entry::getValue)
                .isEqualTo(rowOf("order_id", 10L, "customer_name", null));
        assertThat(fixture.stores.indexPageCount("c", fixture.dimensionKeyOf(1L)))
                .as("and it is in no bucket, because it names none").isZero();
    }

    private static Map<String, Object> rowOf(String first, Object firstValue, String second,
            Object secondValue) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(first, firstValue);
        row.put(second, secondValue);
        return row;
    }

    private static SourceChange fact(Envelope event) {
        return new SourceChange("o", event);
    }

    private static SourceChange dimension(String source, Envelope event) {
        return new SourceChange(source, event);
    }

    private static Envelope read(Map<String, Object> after) {
        return Envelope.read(1L, "src", after, null);
    }

    private static Envelope insert(Map<String, Object> after) {
        return Envelope.insert(1L, "src", after, null);
    }

    private static Envelope update(Map<String, Object> before, Map<String, Object> after) {
        return Envelope.update(1L, "src", before, after, null);
    }

    private static Envelope delete(Map<String, Object> before) {
        return Envelope.delete(1L, "src", before, null);
    }

    /** One driver over plain maps, with a sink that can be told to stop taking rows. */
    private static final class Fixture {

        private final CountingStores stores;
        private final RecordingSink sink = new RecordingSink();
        private final JoinDriver driver;
        private final boolean withNote;

        Fixture(JoinKind kind) {
            this(kind, ReverseIndex.DEFAULT_PAGE_SIZE);
        }

        Fixture(JoinKind kind, int pageSize) {
            this.withNote = true;
            this.stores = new CountingStores(pageSize);
            this.driver = new JoinDriver(planOf(kind), List.of("id"), STREAM, stores);
        }

        void apply(SourceChange change) {
            if (!driver.apply(List.of(change), sink)) {
                drainFully();
            }
        }

        /**
         * Offers until there is nothing left, the way the idle hook does - but a bounded number of
         * times. An unbounded loop here does not fail when the driver stops making progress, it hangs,
         * and a hang in a suite reads as an infrastructure problem rather than as this case.
         */
        void drainFully() {
            for (int offer = 0; offer < 10_000; offer++) {
                if (driver.apply(List.of(), sink)) {
                    return;
                }
            }
            throw new AssertionError("the driver never finished with nothing arriving");
        }

        void clear() {
            sink.taken.clear();
        }

        List<Map.Entry<Op, Map<String, Object>>> published() {
            List<Map.Entry<Op, Map<String, Object>>> rows = new ArrayList<>();
            for (Envelope event : sink.taken) {
                Map<String, Object> row = event.after() != null ? event.after() : event.before();
                Map<String, Object> withoutNote = new LinkedHashMap<>(row);
                if (withNote) {
                    withoutNote.remove("note");
                }
                rows.add(Map.entry(event.op(), withoutNote));
            }
            return rows;
        }

        /** Every value the {@code note} column has been published with, in order. */
        List<Object> notesPublished() {
            List<Object> notes = new ArrayList<>();
            for (Envelope event : sink.taken) {
                Map<String, Object> row = event.after() != null ? event.after() : event.before();
                notes.add(row.get("note"));
            }
            return notes;
        }

        String dimensionKeyOf(long id) {
            return io.tapstate.core.sql.JoinKey.of(List.of(id)).name();
        }

        String onlyFactKey() {
            return factKeyOf(10L);
        }

        String factKeyOf(long id) {
            return io.tapstate.core.sql.JoinKey.of(List.of(id)).name();
        }
    }

    private static JoinPlan planOf(JoinKind kind) {
        JoinTree from = new JoinTree.Join(
                new JoinTree.Source("o", "orders"),
                new JoinTree.Source("c", "customers"),
                kind,
                List.of(new JoinTree.KeyPair(new JoinTree.ColumnRef("o", "cust_id"),
                        new JoinTree.ColumnRef("c", "id"))),
                false);
        return new JoinPlan(List.of(
                new OutputField("order_id", TapstateType.INT64, false,
                        new Expr.Column(new JoinTree.ColumnRef("o", "id"))),
                new OutputField("customer_name", TapstateType.STRING, true,
                        new Expr.Column(new JoinTree.ColumnRef("c", "name"))),
                new OutputField("note", TapstateType.STRING, true,
                        new Expr.Column(new JoinTree.ColumnRef("o", "note")))),
                from,
                Map.of("o", List.of("cust_id", "id", "note"), "c", List.of("id", "name")));
    }

    /** The plain-map state, plus a count of how the fact rows were asked for. */
    private static final class CountingStores implements JoinStores {

        private final MapJoinStores held;
        private int batchReads;
        private int keysRead;

        CountingStores(int pageSize) {
            this.held = new MapJoinStores(pageSize);
        }

        @Override
        public Map<String, Object> fact(String factKey) {
            return held.fact(factKey);
        }

        @Override
        public Map<String, Map<String, Object>> factsUnder(Collection<String> factKeys) {
            batchReads++;
            keysRead += factKeys.size();
            return held.factsUnder(factKeys);
        }

        @Override
        public void putFact(String factKey, Map<String, Object> row) {
            held.putFact(factKey, row);
        }

        @Override
        public void removeFact(String factKey) {
            held.removeFact(factKey);
        }

        @Override
        public Map<String, Object> dimensionRow(String source, String dimensionKey) {
            return held.dimensionRow(source, dimensionKey);
        }

        @Override
        public void putDimensionRow(String source, String dimensionKey, Map<String, Object> row) {
            held.putDimensionRow(source, dimensionKey, row);
        }

        @Override
        public void removeDimensionRow(String source, String dimensionKey) {
            held.removeDimensionRow(source, dimensionKey);
        }

        @Override
        public int indexPageCount(String source, String dimensionKey) {
            return held.indexPageCount(source, dimensionKey);
        }

        @Override
        public List<String> indexPage(String source, String dimensionKey, int page) {
            return held.indexPage(source, dimensionKey, page);
        }

        @Override
        public void indexAdd(String source, String dimensionKey, String factKey) {
            held.indexAdd(source, dimensionKey, factKey);
        }

        @Override
        public void indexRemove(String source, String dimensionKey, String factKey) {
            held.indexRemove(source, dimensionKey, factKey);
        }
    }

    /** A sink that takes what it is told to and refuses the rest. */
    private static final class RecordingSink implements JoinSink {

        private final List<Envelope> taken = new ArrayList<>();
        private int room = Integer.MAX_VALUE;

        void takeAtMost(int room) {
            this.room = room;
        }

        @Override
        public boolean offer(Envelope change) {
            if (room <= 0) {
                return false;
            }
            room--;
            taken.add(change);
            return true;
        }
    }
}
