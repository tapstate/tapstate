package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * What a change to the driving source means when its before image does not carry the whole row.
 *
 * <p><b>This is the ordinary configuration, not an edge one.</b> Postgres under its default
 * {@code REPLICA IDENTITY} publishes the key columns alone, and a change stream with no pre-image
 * configured publishes an empty map. Every other case here hands the driver a complete before image,
 * which is the one shape that makes the three failures below impossible - so they were all invisible
 * with the whole suite green.
 *
 * <p>What goes wrong when the rest of the row is read out of a partial image is that every column it
 * omits reads as null. The dimension key the row used to point at is one of those columns, so it
 * reads as "this row pointed at nothing": no old index entry is removed and a new one is appended,
 * on every edit, for ever. Nothing reports any of it - the published rows stay correct throughout,
 * and the only thing that moves is the size of a structure whose per-key size has no bound.
 */
class APartialBeforeImageNamesTheRowItLeftTest {

    private static final String STREAM = "order_state";

    /** The key columns alone, which is what REPLICA IDENTITY DEFAULT publishes. */
    private static Map<String, Object> keyOnly(long id) {
        Map<String, Object> before = new LinkedHashMap<>();
        before.put("id", id);
        return before;
    }

    @Test
    @DisplayName("editing a row under a key-only before image leaves it in its bucket once, not once per edit")
    void repeatedEditsDoNotGrowTheBucket() {
        Fixture fixture = new Fixture();
        fixture.apply(dimension(Envelope.insert(1L, "src", Map.of("id", 1L, "name", "Ada"), null)));
        fixture.apply(fact(Envelope.insert(1L, "src", Map.of("id", 10L, "cust_id", 1L, "note", "a"), null)));

        for (int edit = 0; edit < 3; edit++) {
            fixture.apply(fact(Envelope.update(1L, "src", keyOnly(10L),
                    Map.of("id", 10L, "cust_id", 1L, "note", "edit" + edit), null)));
        }

        assertThat(fixture.bucket(1L))
                .as("one entry however many times the row was edited - the bucket is what a dimension "
                        + "change walks, and a duplicate is a row recomputed and republished twice")
                .containsExactly(factKey(10L));
    }

    /**
     * The published rows are right either way, which is why this needs the index asserted rather than
     * the output: the stale entry costs a recompute of a row that no longer points there, and the
     * bucket it is stranded in is never emptied, so the head that outlived its rows is never dropped.
     */
    @Test
    @DisplayName("a join key that moves under a key-only before image is removed from the bucket it left")
    void aMovedKeyClearsTheBucketItLeft() {
        Fixture fixture = new Fixture();
        fixture.apply(dimension(Envelope.insert(1L, "src", Map.of("id", 1L, "name", "Ada"), null)));
        fixture.apply(dimension(Envelope.insert(1L, "src", Map.of("id", 2L, "name", "Grace"), null)));
        fixture.apply(fact(Envelope.insert(1L, "src", Map.of("id", 10L, "cust_id", 1L, "note", "a"), null)));

        fixture.apply(fact(Envelope.update(1L, "src", keyOnly(10L),
                Map.of("id", 10L, "cust_id", 2L, "note", "a"), null)));

        assertThat(fixture.bucket(1L)).as("the row no longer points at customer 1").isEmpty();
        assertThat(fixture.bucket(2L)).as("and it points at customer 2").containsExactly(factKey(10L));
    }

    /**
     * A before image holding nothing at all names no row, and asking it which row this is answers
     * null - which the fact key is entitled to refuse. It is the after image that names the row here,
     * so the refusal has to not be reached rather than be caught.
     */
    @Test
    @DisplayName("an empty before image is taken as naming no row, rather than failing the change")
    void anEmptyBeforeImageNamesNoRow() {
        Fixture fixture = new Fixture();
        fixture.apply(dimension(Envelope.insert(1L, "src", Map.of("id", 1L, "name", "Ada"), null)));
        fixture.apply(fact(Envelope.insert(1L, "src", Map.of("id", 10L, "cust_id", 1L, "note", "a"), null)));

        assertThatCode(() -> fixture.apply(fact(Envelope.update(1L, "src", Map.of(),
                Map.of("id", 10L, "cust_id", 1L, "note", "b"), null))))
                .doesNotThrowAnyException();

        assertThat(fixture.bucket(1L)).as("and it is still in its bucket once")
                .containsExactly(factKey(10L));
    }

    /**
     * The batch path and the single path prime differently, and only the batch path reads ahead - so a
     * case that only ever hands changes over singly cannot see what the read ahead does with them.
     */
    @Test
    @DisplayName("the same holds when the edits arrive as one batch, which is the path that reads ahead")
    void aBatchOfEditsBehavesTheSame() {
        Fixture fixture = new Fixture();
        fixture.apply(dimension(Envelope.insert(1L, "src", Map.of("id", 1L, "name", "Ada"), null)));
        fixture.apply(dimension(Envelope.insert(1L, "src", Map.of("id", 2L, "name", "Grace"), null)));
        fixture.apply(fact(Envelope.insert(1L, "src", Map.of("id", 10L, "cust_id", 1L, "note", "a"), null)));

        fixture.applyBatch(List.of(
                fact(Envelope.update(1L, "src", keyOnly(10L),
                        Map.of("id", 10L, "cust_id", 1L, "note", "b"), null)),
                fact(Envelope.update(1L, "src", keyOnly(10L),
                        Map.of("id", 10L, "cust_id", 2L, "note", "c"), null))));

        assertThat(fixture.bucket(1L)).as("the row left customer 1").isEmpty();
        assertThat(fixture.bucket(2L)).as("and is under customer 2 once")
                .containsExactly(factKey(10L));
    }

    private static String factKey(long id) {
        return JoinKey.of(List.of(id)).name();
    }

    private static SourceChange fact(Envelope event) {
        return new SourceChange("o", event);
    }

    private static SourceChange dimension(Envelope event) {
        return new SourceChange("c", event);
    }

    /** One driver over plain maps, with a sink that takes everything. */
    private static final class Fixture {

        private final CountingJoinStores stores = new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE);
        private final JoinDriver driver = new JoinDriver(plan(), List.of("id"), STREAM, stores);
        private final List<Envelope> taken = new ArrayList<>();
        private final JoinSink sink = taken::add;

        void apply(SourceChange change) {
            applyBatch(List.of(change));
        }

        void applyBatch(List<SourceChange> changes) {
            if (driver.apply(changes, sink)) {
                return;
            }
            for (int offer = 0; offer < 10_000; offer++) {
                if (driver.apply(List.of(), sink)) {
                    return;
                }
            }
            throw new AssertionError("the driver never finished with nothing arriving");
        }

        /** Every fact key filed under one dimension key, across all of that bucket's pages. */
        List<String> bucket(long dimensionId) {
            String key = JoinKey.of(List.of(dimensionId)).name();
            List<String> keys = new ArrayList<>();
            for (int page = 0; page < stores.indexPageCount("c", key); page++) {
                keys.addAll(stores.indexPage("c", key, page));
            }
            return keys;
        }
    }

    private static JoinPlan plan() {
        JoinTree from = new JoinTree.Join(
                new JoinTree.Source("o", "orders"),
                new JoinTree.Source("c", "customers"),
                JoinKind.LEFT,
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
}
