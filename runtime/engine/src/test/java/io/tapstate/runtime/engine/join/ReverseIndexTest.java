package io.tapstate.runtime.engine.join;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the reverse index has to hold, and the three ways it can be wrong without saying so.
 *
 * <ul>
 *   <li><b>It can rewrite the whole bucket to add one key.</b> The answers stay right; an initial load
 *       decays smoothly and never finishes, which reads as a slow database. Witnessed by counting the
 *       entries each append writes, not by timing one.
 *   <li><b>It can leave empty pages behind.</b> Also right, also invisible: the memory budget over these
 *       maps counts entries rather than bytes, so a trail of empty ones spends the budget on nothing.
 *   <li><b>It can scatter one bucket's pages across the cluster.</b> Right again - and every read of a
 *       bucket becomes a read across members, on the one member that already holds the key it belongs to.
 * </ul>
 */
class ReverseIndexTest {

    @Test
    void aFactKeyIsFoundUnderTheDimensionKeyItReferences() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 2);

        index.add("C1", "O1");

        assertThat(index.pageCount("C1")).isEqualTo(1);
        assertThat(index.page("C1", 0)).containsExactly("O1");
    }

    @Test
    void aDimensionKeyWithNoFactsHasNoPagesAtAll() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 2);

        assertThat(index.pageCount("C1")).isZero();
        assertThat(index.page("C1", 0)).isEmpty();
        assertThat(index.page("C1", 7)).isEmpty();
    }

    @Test
    void oneBucketNeverAnswersWithAnothersFactKeys() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 2);

        index.add("C1", "O1");
        index.add("C2", "O2");

        assertThat(index.page("C1", 0)).containsExactly("O1");
        assertThat(index.page("C2", 0)).containsExactly("O2");
    }

    @Test
    void aPageIsClosedAtItsSizeAndTheNextOneOpens() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 2);

        index.add("C1", "O1");
        index.add("C1", "O2");
        assertThat(index.pageCount("C1")).isEqualTo(1);

        index.add("C1", "O3");

        assertThat(index.pageCount("C1")).isEqualTo(2);
        assertThat(index.page("C1", 0)).containsExactly("O1", "O2");
        assertThat(index.page("C1", 1)).containsExactly("O3");
    }

    /**
     * The one that catches a bucket rewritten whole. Three full pages already exist; adding one more key
     * must touch the page it lands on and nothing else, or - when it opens a page - that page and the
     * head that counts them.
     */
    @Test
    void anAppendWritesOnePageRatherThanTheWholeBucket() {
        CountingMap written = new CountingMap();
        ReverseIndex index = new ReverseIndex(written, 2);
        for (int i = 0; i < 5; i++) {
            index.add("C1", "O" + i);
        }
        assertThat(index.pageCount("C1")).as("three pages, so a whole-bucket write is visible")
                .isEqualTo(3);
        written.puts = 0;

        index.add("C1", "O5");

        assertThat(written.puts).as("landed inside the last page, so one entry moved").isEqualTo(1);

        written.puts = 0;
        index.add("C1", "O6");
        index.add("C1", "O7");

        assertThat(written.puts).as("one opened a page and bumped the head, the next landed in it")
                .isEqualTo(3);
        assertThat(index.pageCount("C1")).isEqualTo(4);
    }

    @Test
    void aRemovedFactKeyStopsBeingFoundWhereverItsPageWas() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 2);
        index.add("C1", "O1");
        index.add("C1", "O2");
        index.add("C1", "O3");

        assertThat(index.remove("C1", "O1")).isTrue();

        assertThat(index.page("C1", 0)).containsExactly("O2");
        assertThat(index.page("C1", 1)).containsExactly("O3");
    }

    @Test
    void removingWhatIsNotThereChangesNothingAndSaysSo() {
        CountingMap written = new CountingMap();
        ReverseIndex index = new ReverseIndex(written, 2);
        index.add("C1", "O1");
        written.puts = 0;
        written.removes = 0;

        assertThat(index.remove("C1", "O-never")).isFalse();
        assertThat(index.remove("C-never", "O1")).isFalse();

        assertThat(written.puts).isZero();
        assertThat(written.removes).isZero();
        assertThat(index.page("C1", 0)).containsExactly("O1");
    }

    /**
     * The one that catches empty pages left behind. Emptying the last page has to take its entry with
     * it, and the head has to stop claiming a page that is gone - otherwise every walk of the bucket
     * reads a page that holds nothing.
     */
    @Test
    void aPageThatEmptiesIsClosedAndTheHeadStopsCountingIt() {
        CountingMap written = new CountingMap();
        ReverseIndex index = new ReverseIndex(written, 1);
        index.add("C1", "O1");
        index.add("C1", "O2");
        index.add("C1", "O3");
        assertThat(index.pageCount("C1")).isEqualTo(3);

        index.remove("C1", "O2");
        index.remove("C1", "O3");

        assertThat(index.pageCount("C1")).as("both trailing pages gone, not just the last one")
                .isEqualTo(1);
        assertThat(index.page("C1", 0)).containsExactly("O1");
        assertThat(written.keySet()).hasSize(1);
    }

    @Test
    void aBucketEmptiedCompletelyLeavesNoEntryBehind() {
        CountingMap written = new CountingMap();
        ReverseIndex index = new ReverseIndex(written, 2);
        index.add("C1", "O1");
        index.add("C1", "O2");
        index.add("C1", "O3");

        index.remove("C1", "O1");
        index.remove("C1", "O2");
        index.remove("C1", "O3");

        assertThat(index.pageCount("C1")).isZero();
        assertThat(written).as("a dimension key nothing points at holds no entry at all").isEmpty();
    }

    /**
     * The head has to survive its own page emptying while pages follow it, because it is the only place
     * the number of them is written down. Losing it strands every following page: nothing knows they
     * are there, and the rows they name never rebuild.
     */
    @Test
    void theHeadOutlivesItsOwnKeysWhilePagesStillFollowIt() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 1);
        index.add("C1", "O1");
        index.add("C1", "O2");

        index.remove("C1", "O1");

        assertThat(index.pageCount("C1")).isEqualTo(2);
        assertThat(index.page("C1", 0)).isEmpty();
        assertThat(index.page("C1", 1)).containsExactly("O2");
    }

    /**
     * A redelivered insert adds the key again rather than being recognised, because recognising it means
     * reading every page of the bucket on every insert. One remove takes one of them, which is what a
     * redelivered delete of the same row does.
     */
    @Test
    void aRepeatedAddIsRecordedTwiceAndOneRemoveTakesOne() {
        ReverseIndex index = new ReverseIndex(new HashMap<>(), 4);
        index.add("C1", "O1");
        index.add("C1", "O1");

        index.remove("C1", "O1");

        assertThat(index.page("C1", 0)).containsExactly("O1");
    }

    /**
     * The one that catches pages scattered across the cluster. Every page of a bucket has to be placed
     * by the dimension key, because the member that reads a bucket is the one that key was routed to.
     */
    @Test
    void everyPageOfOneBucketIsPlacedByTheDimensionKey() {
        List<Object> placements = new ArrayList<>();
        for (int page = 0; page < 3; page++) {
            placements.add(new ReverseBucket.At("C1", page).getPartitionKey());
        }

        assertThat(placements).containsOnly("C1");
        assertThat(new ReverseBucket.At("C2", 0).getPartitionKey())
                .as("positive control: a different key does place differently")
                .isEqualTo("C2");
    }

    /** A map that says how many entries were written or removed through it. */
    private static final class CountingMap extends HashMap<ReverseBucket.At, ReverseBucket> {

        private static final long serialVersionUID = 1L;

        private int puts;
        private int removes;

        @Override
        public ReverseBucket put(ReverseBucket.At key, ReverseBucket value) {
            puts++;
            return super.put(key, value);
        }

        @Override
        public ReverseBucket remove(Object key) {
            removes++;
            return super.remove(key);
        }
    }
}
