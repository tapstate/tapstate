package io.tapstate.e2e;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.runtime.engine.join.JoinDriver;
import io.tapstate.runtime.engine.join.JoinExecutor;
import io.tapstate.runtime.engine.join.JoinProjection;
import io.tapstate.runtime.engine.join.JoinSink;
import io.tapstate.runtime.engine.join.JoinStores;
import io.tapstate.runtime.engine.join.MapJoinStores;
import io.tapstate.runtime.engine.join.ReverseBucket;
import io.tapstate.runtime.engine.join.SourceChange;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Holds a join restarted part way through its load against MySQL's answer to the same SQL.
 *
 * <p>A run that dies without being stopped keeps its join state, and the restart reads the load again
 * from the start on top of it. Taking a fact row in writes the mirror and then the reverse index, so a
 * member that stops between the two leaves a row the mirror holds and no bucket names; if that row's
 * dimension row arrives only in the load read again, nothing but the index can find the row for it.
 *
 * <p>The specification vocabulary cannot stop a member between two writes to its state, or start the
 * next run over the state the stopped one left. Both are explicit here; the SQL front end, the drivers,
 * the final projection and the database oracle are the real implementations.
 */
@RequiresDocker
class AJoinRestartedPartWayThroughItsLoadStillAgreesWithItsSourceIT {

    private static final String SQL = """
            SELECT c.row_id AS row_id, x.cve AS cve, c.published AS published
            FROM cves c LEFT JOIN xw_cves x ON c.cve_id = x.surface_key
            """;

    @Test
    void aRowTheStoppedRunMirroredButNeverIndexedIsMatchedWhenTheLoadIsReadAgain() throws Exception {
        try (Connection db = database("join_restart_mid_load")) {
            Member[] member = new Member[1];
            try (JoinConformance answer = JoinConformance.of(db, SQL, List.of("row_id"),
                    (keys, stream) -> member[0] = new Member(keys, stream))) {
                // The run that stops: one row taken in whole, then the member stops while taking the
                // next one in - after the mirror has it, before the reverse index does.
                answer.upsert("cves", cve(8389, "cve-2020-23326"));
                member[0].stopAtTheNextIndexWrite();
                assertThatThrownBy(() -> answer.upsert("cves", cve(8390, "cve-2020-23327")))
                        .isInstanceOf(MemberStopped.class);
                // What the load had not reached yet: one more fact row, and the whole dimension.
                try (Statement sql = db.createStatement()) {
                    sql.execute("INSERT INTO cves VALUES (8391, 'cve-2020-23328', '2023-03-14')");
                    sql.execute("INSERT INTO xw_cves VALUES ('cve-2020-23326', 'CVE-2020-23326'), "
                            + "('cve-2020-23327', 'CVE-2020-23327'), ('cve-2020-23328', 'CVE-2020-23328')");
                }

                // The restart: a new run over what the stopped one kept, reading the load again from the
                // start - the fact table first, so every fact row is out before its dimension row.
                member[0].restart();
                answer.reread("cves");
                answer.reread("xw_cves");

                assertThat(answer.differences())
                        .as("every fact row matched, the one the stopped run left unindexed included")
                        .isEmpty();
            }
        }
    }

    /**
     * The same gap reached through an update, which is how a pipeline whose load has finished meets
     * it: its restart resumes the change stream and reads nothing again, so what comes back is the
     * change the stopped run was in the middle of. The update moves the row's join key and carries a
     * before image holding the row's key alone, as postgres publishes it under its default replica
     * identity - so the mirror already holds the key it moves to when it is delivered again.
     */
    @Test
    void anUpdateTheStoppedRunMirroredButNeverIndexedIsMatchedWhenDeliveredAgain() throws Exception {
        try (Connection db = database("join_restart_mid_update")) {
            Member[] member = new Member[1];
            try (JoinConformance answer = JoinConformance.of(db, SQL, List.of("row_id"),
                    (keys, stream) -> member[0] = new Member(keys, stream))) {
                // The run that stops: both rows loaded and row 8389's update taken in whole, then the
                // member stops while taking row 8390's update in - after the mirror has the key it
                // moves to, before the reverse index does.
                answer.upsert("cves", cve(8389, "cve-unmapped-1"));
                answer.upsert("cves", cve(8390, "cve-unmapped-2"));
                answer.updateWithKeyOnlyBefore("cves", cve(8389, "cve-2020-23326"));
                member[0].stopAtTheNextIndexWrite();
                assertThatThrownBy(() -> answer.updateWithKeyOnlyBefore("cves", cve(8390, "cve-2020-23327")))
                        .isInstanceOf(MemberStopped.class);

                // The restart resumes the change stream from before both updates, so both are delivered
                // again; then the dimension rows they point at arrive.
                member[0].restart();
                answer.updateWithKeyOnlyBefore("cves", cve(8389, "cve-2020-23326"));
                answer.updateWithKeyOnlyBefore("cves", cve(8390, "cve-2020-23327"));
                answer.upsert("xw_cves", Map.of("surface_key", "cve-2020-23326", "cve", "CVE-2020-23326"));
                answer.upsert("xw_cves", Map.of("surface_key", "cve-2020-23327", "cve", "CVE-2020-23327"));

                assertThat(answer.differences())
                        .as("both updated rows matched, the one the stopped run left unindexed included")
                        .isEmpty();
            }
        }
    }

    private static Map<String, Object> cve(int rowId, String cveId) {
        return Map.of("row_id", rowId, "cve_id", cveId, "published", Date.valueOf("2023-03-14"));
    }

    private static Connection database(String name) throws SQLException {
        Connection db = SharedMySql.connect(SharedMySql.settings(name));
        try (Statement sql = db.createStatement()) {
            sql.execute("CREATE TABLE cves (row_id INT PRIMARY KEY, cve_id VARCHAR(32), published DATE)");
            sql.execute("CREATE TABLE xw_cves (surface_key VARCHAR(32) PRIMARY KEY, cve VARCHAR(32))");
        }
        return db;
    }

    /** What every call to a member's state answers once the member has stopped. */
    private static final class MemberStopped extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private MemberStopped() {
            super("the member stopped");
        }
    }

    /**
     * One member's join: a fact driver and a dimension driver over one state, each publishing through
     * the final projection. It can be stopped at the next write to the reverse index, and started again
     * over the state it kept.
     */
    private static final class Member implements JoinExecutor {

        private final List<String> keys;
        private final String stream;
        private final MapJoinStores kept = new MapJoinStores();
        private final StopsAtTheNextIndexWrite stopping = new StopsAtTheNextIndexWrite(kept);
        private JoinPlan plan;
        private JoinDriver facts;
        private JoinDriver dimensions;
        private JoinProjection projection;

        private Member(List<String> keys, String stream) {
            this.keys = keys;
            this.stream = stream;
        }

        @Override
        public void open(JoinPlan plan) {
            this.plan = plan;
            run(stopping);
        }

        void stopAtTheNextIndexWrite() {
            stopping.stopping = true;
        }

        /** The next run, reading and writing what the stopped one kept. */
        void restart() {
            run(kept);
        }

        private void run(JoinStores stores) {
            facts = new JoinDriver(plan, keys, stream, stores);
            dimensions = new JoinDriver(plan, keys, stream, stores);
            projection = new JoinProjection(plan, keys, stream, stores);
        }

        @Override
        public boolean apply(List<SourceChange> changes, JoinSink sink) {
            for (SourceChange change : changes) {
                JoinDriver driver = change.source().equals(plan.factSource().name()) ? facts : dimensions;
                driver.absorb(List.of(change));
            }
            boolean factsDone = drain(facts, sink);
            return drain(dimensions, sink) && factsDone;
        }

        private boolean drain(JoinDriver driver, JoinSink sink) {
            return driver.drainUpdates(update -> {
                for (Envelope event : projection.refresh(List.of(update))) {
                    assertThat(sink.offer(event)).as("the oracle sink accepts every event").isTrue();
                }
                return true;
            });
        }

        @Override
        public void close() {
        }
    }

    /** The state, and the point at which the member stops: the next reverse-index write never lands. */
    private static final class StopsAtTheNextIndexWrite implements JoinStores {
        private final JoinStores held;
        private boolean stopping;

        private StopsAtTheNextIndexWrite(JoinStores held) {
            this.held = held;
        }

        @Override public Map<String, Object> fact(String key) { return held.fact(key); }
        @Override public Map<String, Map<String, Object>> factsUnder(Collection<String> keys) {
            return held.factsUnder(keys);
        }
        @Override public void putFact(String key, Map<String, Object> row) { held.putFact(key, row); }
        @Override public void removeFact(String key) { held.removeFact(key); }
        @Override public Map<String, Object> dimensionRow(String source, String key) {
            return held.dimensionRow(source, key);
        }
        @Override public Map<String, Object> putDimensionRow(String source, String key,
                Map<String, Object> row) {
            return held.putDimensionRow(source, key, row);
        }
        @Override public void removeDimensionRow(String source, String key) { held.removeDimensionRow(source, key); }
        @Override public int indexPageCount(String source, String key) { return held.indexPageCount(source, key); }
        @Override public List<String> indexPage(String source, String key, int page) {
            return held.indexPage(source, key, page);
        }
        @Override public Map<ReverseBucket.At, Set<String>> indexNames(String source,
                Map<ReverseBucket.At, Set<String>> asked) {
            return held.indexNames(source, asked);
        }
        @Override public void indexRemove(String source, String key, String fact) { held.indexRemove(source, key, fact); }
        @Override public long batchesTakenIn(String writer) { return held.batchesTakenIn(writer); }
        @Override public void putBatchesTakenIn(String writer, long batch) {
            held.putBatchesTakenIn(writer, batch);
        }
        @Override public void indexAdd(String source, String key, String fact) {
            if (stopping) {
                throw new MemberStopped();
            }
            held.indexAdd(source, key, fact);
        }
    }
}
