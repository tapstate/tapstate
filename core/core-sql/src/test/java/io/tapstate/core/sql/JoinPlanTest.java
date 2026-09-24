package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which columns of a source the published row's values actually come from.
 *
 * <p>This is what lets a carrier throw away a dimension change that cannot alter a single byte of the
 * output - the common case in a real workload, where most edits to a customer touch an address or a
 * timestamp the join never reads. Getting it wrong is silent in both directions: too wide and every
 * edit triggers a full fan-out recompute, too narrow and an edit that does change the output is
 * dropped and the target keeps the old value with nothing reported.
 */
class JoinPlanTest {

    @Test
    @DisplayName("a column the output is computed from is one of that source's output columns")
    void aProjectedColumnIsCollected() {
        assertThat(planOf(new Expr.Column(ref("c", "name"))).outputColumns("c"))
                .containsExactly("name");
    }

    /**
     * The one that decides whether this is safe to filter on. A column can sit anywhere inside an
     * expression, and an implementation that looks only at the top level sees {@code UPPER(c.name)} as
     * referencing nothing - so an edit to the name is discarded and the target keeps the old one.
     */
    @Test
    @DisplayName("a column nested inside an expression is collected too")
    void aColumnInsideAnExpressionIsCollected() {
        Expr nested = new Expr.Call("UPPER", List.of(
                new Expr.Call("||", List.of(
                        new Expr.Column(ref("c", "first")),
                        new Expr.Column(ref("c", "last"))))));

        assertThat(planOf(nested).outputColumns("c")).containsExactlyInAnyOrder("first", "last");
    }

    @Test
    @DisplayName("a column of another source is not one of this source's output columns")
    void anotherSourcesColumnIsNotCollected() {
        assertThat(planOf(new Expr.Column(ref("o", "total"))).outputColumns("c")).isEmpty();
    }

    /**
     * What separates this from the columns a source is <em>read</em> for. A join key is read and
     * routinely not projected; if the two were the same set, every edit would look like it changed the
     * output and the filter this exists for would never discard anything.
     */
    @Test
    @DisplayName("a column read but never projected is not an output column")
    void aReadButUnprojectedColumnIsNotCollected() {
        JoinPlan plan = planOf(new Expr.Column(ref("c", "name")));

        assertThat(plan.readColumns().get("c")).contains("id");
        assertThat(plan.outputColumns("c")).doesNotContain("id");
    }

    @Test
    @DisplayName("a source the plan does not read has no output columns rather than failing")
    void anUnknownSourceIsEmpty() {
        assertThat(planOf(new Expr.Column(ref("c", "name"))).outputColumns("nobody")).isEmpty();
    }

    private static JoinTree.ColumnRef ref(String source, String column) {
        return new JoinTree.ColumnRef(source, column);
    }

    private static JoinPlan planOf(Expr second) {
        JoinTree from = new JoinTree.Join(
                new JoinTree.Source("o", "orders"),
                new JoinTree.Source("c", "customers"),
                JoinKind.LEFT,
                List.of(new JoinTree.KeyPair(ref("o", "cust_id"), ref("c", "id"))),
                false);
        return new JoinPlan(List.of(
                new OutputField("order_id", TapstateType.INT64, false,
                        new Expr.Column(ref("o", "id"))),
                new OutputField("second", TapstateType.STRING, true, second)),
                from,
                Map.of("o", List.of("cust_id", "id"), "c", List.of("first", "id", "last", "name")));
    }
}
