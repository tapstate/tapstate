package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a join restarted over the state of a run that died part way through a load makes of the fact
 * row the dead run was in the middle of taking in.
 *
 * <p><b>A run that dies without being stopped keeps its join state, and its sources are read again
 * from the start on top of it.</b> Nothing records how far a load got, so the restart reads every table
 * the load still owed once more, and nothing clears the mirrors or the reverse index in between. The
 * driver is written for that: a row delivered a second time asks the mirror what it held, and where the
 * key it points at has not moved it leaves the index alone, on the grounds that the index already names
 * it.
 *
 * <p><b>Those grounds do not hold for the row a run was taking in when it died.</b> Taking a fact row
 * in is two writes to two maps - the mirror first, then the reverse index - and a member shut down
 * between them leaves a row the mirror holds and no bucket names. The replay finds that row mirrored
 * under the key it arrives with, adds nothing, and publishes it with nulls because its dimension row is
 * not there yet. When the dimension row arrives, its bucket has no record of the row, so nothing turns
 * it into a match: it stays null for good, beside rows that all matched, with the job running and the
 * source idle.
 *
 * <p>The two rows either side of it are asserted first - one the dead run took in whole, one it never
 * reached - so that a failure on the row between them is about that row, not about a join that matches
 * nothing.
 */
class AReplayOverADeadRunsStateStillMatchesEveryFactRowTest {

    private static final String STREAM = "cve_record";

    /** The fact table's own key, which a mirror entry and an index entry are filed under. */
    private static final List<String> FACT_KEY = List.of("row_id");

    /**
     * A fact table and the key-mapping dimension it is joined to, on plain text on both sides, under
     * the aliases the step declares - which is how the step's own compilation registers them.
     */
    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("c", List.of(
                    new SourceColumn("row_id", TapstateType.INT64, true),
                    new SourceColumn("cve_id", TapstateType.STRING, true),
                    new SourceColumn("published", TapstateType.DATE, true))),
            new SourceTable("x", List.of(
                    new SourceColumn("surface_key", TapstateType.STRING, true),
                    new SourceColumn("cve", TapstateType.STRING, true))));

    private static final JoinPlan PLAN = SqlFrontEnd.derive(
            "SELECT c.row_id AS row_id, x.cve AS cve, c.published AS published "
                    + "FROM c LEFT JOIN x ON c.cve_id = x.surface_key",
            TABLES);

    @Test
    @DisplayName("a fact row the dead run mirrored but never indexed is matched once the replay brings its dimension row")
    void aRowTheDeadRunMirroredButNeverIndexedIsMatchedByItsLateDimensionRow() {
        MapJoinStores kept = new MapJoinStores();
        DiesAtTheNextIndexWrite member = new DiesAtTheNextIndexWrite(kept);
        View view = new View();

        // The run that died. It took row 8389 in whole; then the member was shut down while it was
        // taking row 8390 in - after the fact mirror had the row, before the reverse index did. No
        // dimension row had arrived by then.
        JoinDriver dead = new JoinDriver(PLAN, FACT_KEY, STREAM, member);
        view.take(dead, List.of(cve(8389L, "cve-2020-23326")));
        member.dieAtTheNextIndexWrite();
        assertThatThrownBy(() -> dead.apply(List.of(cve(8390L, "cve-2020-23327")), view))
                .isInstanceOf(HazelcastInstanceNotActiveException.class);

        // The restart: a new run over what the dead one kept, both tables read again from the start -
        // the fact table first, so every fact row is published before its dimension row arrives.
        JoinDriver replay = new JoinDriver(PLAN, FACT_KEY, STREAM, kept);
        view.take(replay, List.of(
                cve(8389L, "cve-2020-23326"),
                cve(8390L, "cve-2020-23327"),
                cve(8391L, "cve-2020-23328")));
        view.take(replay, List.of(
                crosswalk("cve-2020-23326", "CVE-2020-23326"),
                crosswalk("cve-2020-23327", "CVE-2020-23327"),
                crosswalk("cve-2020-23328", "CVE-2020-23328")));

        assertThat(view.lastRowFor(8389L))
                .as("the row the dead run took in whole is matched once its dimension row arrives")
                .containsEntry("cve", "CVE-2020-23326");
        assertThat(view.lastRowFor(8391L))
                .as("and so is a row the dead run never reached")
                .containsEntry("cve", "CVE-2020-23328");
        assertThat(view.lastRowFor(8390L))
                .as("and so must be the row the dead run left in the mirror with no index entry: its "
                        + "dimension row has arrived, so a null here is a wrong answer nothing reports")
                .containsEntry("cve", "CVE-2020-23327");
    }

    /**
     * The same gap reached through an update rather than a load read again, which is how a pipeline
     * whose load has finished meets it: its restart resumes the change stream and reads nothing again,
     * so the only thing that comes back is the change the dead run was in the middle of.
     *
     * <p>The update moves the row's join key and its before image carries the row's own key alone, as
     * postgres publishes it under its default replica identity. The dead run mirrored the new key and
     * died before the index took it; delivered again, the update finds the mirror already holding the
     * key it moves to, so an index that is only ever touched where the key changes is not touched at
     * all - and the dimension row arriving after it has no record of the row to reach it through.
     */
    @Test
    @DisplayName("an update the dead run mirrored but never indexed is matched once it is delivered again")
    void anUpdateTheDeadRunMirroredButNeverIndexedIsMatchedWhenDeliveredAgain() {
        MapJoinStores kept = new MapJoinStores();
        DiesAtTheNextIndexWrite member = new DiesAtTheNextIndexWrite(kept);
        View view = new View();

        // The run that died. It loaded both rows and took row 8389's update in whole; then it was shut
        // down while taking row 8390's update in - after the mirror had the new key, before the index.
        JoinDriver dead = new JoinDriver(PLAN, FACT_KEY, STREAM, member);
        view.take(dead, List.of(cve(8389L, "cve-unmapped-1"), cve(8390L, "cve-unmapped-2")));
        view.take(dead, List.of(repointed(8389L, "cve-2020-23326")));
        member.dieAtTheNextIndexWrite();
        assertThatThrownBy(() -> dead.apply(List.of(repointed(8390L, "cve-2020-23327")), view))
                .isInstanceOf(HazelcastInstanceNotActiveException.class);

        // The restart resumes the change stream where it had last been confirmed, before both updates,
        // so both are delivered again; then the dimension rows they point at arrive.
        JoinDriver replay = new JoinDriver(PLAN, FACT_KEY, STREAM, kept);
        view.take(replay, List.of(
                repointed(8389L, "cve-2020-23326"),
                repointed(8390L, "cve-2020-23327")));
        view.take(replay, List.of(
                crosswalk("cve-2020-23326", "CVE-2020-23326"),
                crosswalk("cve-2020-23327", "CVE-2020-23327")));

        assertThat(view.lastRowFor(8389L))
                .as("the update the dead run took in whole is matched once its dimension row arrives")
                .containsEntry("cve", "CVE-2020-23326");
        assertThat(view.lastRowFor(8390L))
                .as("and so must be the update it left in the mirror with no index entry: its dimension "
                        + "row has arrived, so a null here is a wrong answer nothing reports")
                .containsEntry("cve", "CVE-2020-23327");
    }

    /** One row of the fact table as a snapshot reads it. */
    private static SourceChange cve(long rowId, String cveId) {
        return new SourceChange("c", Envelope.read(1L, "cves", cveRow(rowId, cveId), null));
    }

    /**
     * An update pointing one fact row at {@code cveId}, whose before image carries the row's own key
     * and nothing else - what postgres publishes under its default replica identity.
     */
    private static SourceChange repointed(long rowId, String cveId) {
        return new SourceChange("c",
                Envelope.update(1L, "cves", Map.of("row_id", rowId), cveRow(rowId, cveId), null));
    }

    private static Map<String, Object> cveRow(long rowId, String cveId) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("row_id", rowId);
        row.put("cve_id", cveId);
        row.put("published", LocalDate.of(2023, 3, 14));
        return row;
    }

    /** One row of the key-mapping dimension as a snapshot reads it. */
    private static SourceChange crosswalk(String surfaceKey, String cve) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("surface_key", surfaceKey);
        row.put("cve", cve);
        return new SourceChange("x", Envelope.read(1L, "xw_cves", row, null));
    }

    /**
     * What the view ends up holding: the last row published under each fact row, across both runs, the
     * way a target keyed on the fact row keeps only the latest write.
     */
    private static final class View implements JoinSink {

        private final List<Envelope> taken = new ArrayList<>();

        @Override
        public boolean offer(Envelope change) {
            taken.add(change);
            return true;
        }

        /**
         * Hands one delivery to {@code driver} and offers until nothing is left, the way the idle call
         * does - a bounded number of times, so a driver that stops making progress fails here rather
         * than hanging the suite.
         */
        void take(JoinDriver driver, List<SourceChange> delivery) {
            if (driver.apply(delivery, this)) {
                return;
            }
            for (int offer = 0; offer < 10_000; offer++) {
                if (driver.apply(List.of(), this)) {
                    return;
                }
            }
            throw new AssertionError("the driver never finished with nothing arriving");
        }

        /** The last row published for one fact row, or null where the last word was a removal. */
        Map<String, Object> lastRowFor(long rowId) {
            Envelope last = null;
            for (Envelope event : taken) {
                Map<String, Object> row = event.after() != null ? event.after() : event.before();
                if (row != null && Long.valueOf(rowId).equals(row.get("row_id"))) {
                    last = event;
                }
            }
            return last == null || last.op() == Op.DELETE ? null : last.after();
        }
    }

    /**
     * The state a member keeps, and the point at which the member stops: the next write to the reverse
     * index fails the way every call does once the member has been shut down, and never lands. What was
     * written before it stays written, which is what a restarted run finds.
     */
    private static final class DiesAtTheNextIndexWrite extends ForwardingJoinStores {

        private boolean dying;

        DiesAtTheNextIndexWrite(JoinStores held) {
            super(held);
        }

        void dieAtTheNextIndexWrite() {
            dying = true;
        }

        @Override
        public void indexAdd(String source, String dimensionKey, String factKey) {
            if (dying) {
                throw new HazelcastInstanceNotActiveException();
            }
            super.indexAdd(source, dimensionKey, factKey);
        }
    }
}
