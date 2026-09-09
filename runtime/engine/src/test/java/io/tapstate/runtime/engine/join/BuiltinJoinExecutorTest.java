package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The carrier seam's first implementation.
 *
 * <p>Every case here is about the plan the driver was built from, because that is the one thing this
 * class owns: the driver is built around a plan, the seam hands a plan to a carrier chosen before the
 * plan was read, and each way of getting that wrong publishes rows of the right shape carrying another
 * plan's projection.
 */
class BuiltinJoinExecutorTest {

    @Test
    @DisplayName("the plan handed to open is the one apply publishes rows from")
    void openDecidesWhatIsPublished() {
        Collecting sink = new Collecting();
        BuiltinJoinExecutor executor =
                new BuiltinJoinExecutor(List.of("id"), "order_state", new MapJoinStores());

        executor.open(planPublishing("customer_name"));
        executor.apply(List.of(dimension(1L, "Ada")), sink);
        executor.apply(List.of(fact(10L, 1L)), sink);

        assertThat(sink.rows).containsExactly(Map.of("order_id", 10L, "customer_name", "Ada"));
    }

    /**
     * Without this the first call is a {@link NullPointerException} out of a field nobody named, which
     * is the same report a carrier gets for any other missing collaborator.
     */
    @Test
    @DisplayName("applying before a plan has been opened says that rather than failing on a null driver")
    void applyingWithoutAPlanIsRefusedByName() {
        BuiltinJoinExecutor executor =
                new BuiltinJoinExecutor(List.of("id"), "order_state", new MapJoinStores());

        assertThatThrownBy(() -> executor.apply(List.of(), new Collecting()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("open");
    }

    /**
     * A close that only stopped taking changes would leave the driver in place, and the next apply
     * would run on a plan its caller believes was let go - publishing the closed join's rows onto the
     * new one's stream.
     */
    @Test
    @DisplayName("closing lets the plan go, so a later apply is refused rather than run on it")
    void closingLetsThePlanGo() {
        BuiltinJoinExecutor executor =
                new BuiltinJoinExecutor(List.of("id"), "order_state", new MapJoinStores());
        executor.open(planPublishing("customer_name"));

        executor.close();

        assertThatThrownBy(() -> executor.apply(List.of(), new Collecting()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("open");
    }

    /**
     * Opening a second time has to replace the driver, not be ignored. An implementation that kept the
     * first one publishes the first plan's columns for ever, and the rows are the right shape.
     */
    @Test
    @DisplayName("opening a second plan replaces the first rather than being ignored")
    void reopeningReplacesThePlan() {
        Collecting sink = new Collecting();
        BuiltinJoinExecutor executor =
                new BuiltinJoinExecutor(List.of("id"), "order_state", new MapJoinStores());

        executor.open(planPublishing("customer_name"));
        executor.open(planPublishing("who"));
        executor.apply(List.of(dimension(1L, "Ada")), sink);
        executor.apply(List.of(fact(10L, 1L)), sink);

        assertThat(sink.rows).containsExactly(Map.of("order_id", 10L, "who", "Ada"));
    }

    private static SourceChange fact(long id, long customerId) {
        return new SourceChange("o",
                Envelope.insert(1L, "orders", Map.of("id", id, "cust_id", customerId), null));
    }

    private static SourceChange dimension(long id, String name) {
        return new SourceChange("c",
                Envelope.insert(1L, "customers", Map.of("id", id, "name", name), null));
    }

    /** The same two-source join throughout, publishing the dimension's name under {@code as}. */
    private static JoinPlan planPublishing(String as) {
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
                new OutputField(as, TapstateType.STRING, true,
                        new Expr.Column(new JoinTree.ColumnRef("c", "name")))),
                from,
                Map.of("o", List.of("cust_id", "id"), "c", List.of("id", "name")));
    }

    private static final class Collecting implements JoinSink {

        private final List<Map<String, Object>> rows = new ArrayList<>();

        @Override
        public boolean offer(Envelope change) {
            rows.add(change.after() != null ? change.after() : change.before());
            return true;
        }
    }
}
