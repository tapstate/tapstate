package io.tapstate.e2e;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.runtime.engine.join.JoinDriver;
import io.tapstate.runtime.engine.join.JoinExecutor;
import io.tapstate.runtime.engine.join.JoinProjection;
import io.tapstate.runtime.engine.join.JoinSink;
import io.tapstate.runtime.engine.join.JoinStores;
import io.tapstate.runtime.engine.join.JoinUpdate;
import io.tapstate.runtime.engine.join.MapJoinStores;
import io.tapstate.runtime.engine.join.SourceChange;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds controlled cross-partition deliveries against MySQL's answer to the same three-source SQL.
 * The specification vocabulary cannot pause a processor between publishing an index entry and its
 * fact row, or hold one processor's outbox while another drains. Those two schedules are explicit
 * here; the SQL front end, drivers, final projection and database oracle are the real implementations.
 * The published three-source example separately exercises the complete server on both fidelity tiers.
 */
@RequiresDocker
class AConcurrentJoinStillAgreesWithItsSourceIT {
    private static final String SQL = """
            SELECT o.id AS order_id, c.name AS customer_name, p.channel AS payment_method
            FROM orders o JOIN customers c ON o.customer_id = c.id
            LEFT JOIN payments p ON o.payment_id = p.id
            """;

    @Test
    void aCustomerChangeDuringFactAdmissionDoesNotRemoveTheOrderFromLaterUpdates() throws Exception {
        try (Connection db = database("join_admission_race")) {
            SplitCarrier[] carrier = new SplitCarrier[1];
            try (JoinConformance answer = JoinConformance.of(db, SQL, List.of("order_id"),
                    (keys, stream) -> carrier[0] = new SplitCarrier(keys, stream))) {
                answer.upsert("customers", Map.of("id", 1, "name", "ada"));
                answer.upsert("payments", Map.of("id", 100, "channel", "card"));
                carrier[0].stores.afterCustomerIndex = () -> {
                    try {
                        answer.upsert("customers", Map.of("id", 1, "name", "adelaide"));
                    } catch (SQLException failure) {
                        throw new AssertionError(failure);
                    }
                };
                answer.upsert("orders", Map.of("id", 10, "customer_id", 1, "payment_id", 100));
                answer.upsert("orders", Map.of("id", 12, "customer_id", 1, "payment_id", 100));
                assertThat(answer.differences()).as("the initial materialization").isEmpty();

                answer.upsert("customers", Map.of("id", 1, "name", "augusta"));
                assertThat(answer.differences())
                        .as("both orders must still belong to the customer's reverse index").isEmpty();
            }
        }
    }

    @Test
    void anOldOutboxCannotUndoThePaymentOrCustomerThatAlreadyArrived() throws Exception {
        try (Connection db = database("join_output_race")) {
            SplitCarrier[] carrier = new SplitCarrier[1];
            try (JoinConformance answer = JoinConformance.of(db, SQL, List.of("order_id"),
                    (keys, stream) -> carrier[0] = new SplitCarrier(keys, stream))) {
                answer.upsert("customers", Map.of("id", 1, "name", "ada"));
                carrier[0].holdFacts = true;
                answer.upsert("orders", Map.of("id", 10, "customer_id", 1, "payment_id", 100));
                assertThat(carrier[0].delayed).singleElement()
                        .satisfies(event -> assertThat(event.event().after()).containsEntry("payment_method", null));

                answer.upsert("payments", Map.of("id", 100, "channel", "card"));
                answer.upsert("customers", Map.of("id", 1, "name", "adelaide"));
                assertThat(answer.differences()).as("the newer results arrived first").isEmpty();

                carrier[0].release();
                assertThat(answer.differences()).as("the delayed older result cannot overwrite either column")
                        .isEmpty();
            }
        }
    }

    private static Connection database(String name) throws SQLException {
        Connection db = SharedMySql.connect(SharedMySql.settings(name));
        try (Statement sql = db.createStatement()) {
            sql.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(32))");
            sql.execute("CREATE TABLE payments (id INT PRIMARY KEY, channel VARCHAR(32))");
            sql.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, payment_id INT)");
        }
        return db;
    }

    /** Two source owners and independently drainable outboxes, followed by the ordered projection. */
    private static final class SplitCarrier implements JoinExecutor {
        private final List<String> keys;
        private final String stream;
        private final InterleavedStores stores = new InterleavedStores();
        private final List<JoinUpdate> delayed = new ArrayList<>();
        private JoinDriver facts;
        private JoinDriver dimensions;
        private JoinProjection projection;
        private JoinSink sink;
        private String factSource;
        private boolean holdFacts;

        private SplitCarrier(List<String> keys, String stream) {
            this.keys = keys;
            this.stream = stream;
        }

        @Override
        public void open(JoinPlan plan) {
            facts = new JoinDriver(plan, keys, stream, stores);
            dimensions = new JoinDriver(plan, keys, stream, stores);
            projection = new JoinProjection(plan, keys, stream, stores);
            factSource = plan.factSource().name();
        }

        @Override
        public boolean apply(List<SourceChange> changes, JoinSink sink) {
            this.sink = sink;
            for (SourceChange change : changes) {
                boolean fact = change.source().equals(factSource);
                JoinDriver driver = fact ? facts : dimensions;
                driver.absorb(List.of(change));
                boolean drained = driver.drainUpdates(event -> {
                    if (fact && holdFacts) {
                        delayed.add(event);
                    } else {
                        publish(List.of(event));
                    }
                    return true;
                });
                assertThat(drained).as("the oracle sink accepts every event").isTrue();
            }
            return true;
        }

        private void publish(List<JoinUpdate> events) {
            for (Envelope event : projection.refresh(events)) {
                assertThat(sink.offer(event)).isTrue();
            }
        }

        private void release() {
            publish(delayed);
            delayed.clear();
        }

        @Override
        public void close() {
        }
    }

    /** Runs the other source owner at the exact boundary a concurrent index reader can observe. */
    private static final class InterleavedStores implements JoinStores {
        private final JoinStores held = new MapJoinStores();
        private Runnable afterCustomerIndex;

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
        @Override public void indexRemove(String source, String key, String fact) { held.indexRemove(source, key, fact); }
        @Override public void indexAdd(String source, String key, String fact) {
            held.indexAdd(source, key, fact);
            if (source.equals("c") && afterCustomerIndex != null) {
                Runnable interleaved = afterCustomerIndex;
                afterCustomerIndex = null;
                interleaved.run();
            }
        }
    }
}
