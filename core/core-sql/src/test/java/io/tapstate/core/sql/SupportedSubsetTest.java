package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The grammar boundary: which SQL a join declaration may be written in.
 *
 * <p>Every case here is a shape that parses and validates perfectly well, so nothing downstream
 * would refuse it. What the plan cannot state, a carrier cannot execute -- and a carrier handed a
 * plan that is missing half of what the SQL said emits rows that read as ordinary output.
 */
class SupportedSubsetTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true),
                    new SourceColumn("o_amount", TapstateType.DECIMAL, false))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    private static String shapeOf(String sql) {
        Optional<Unsupported> found = SqlFrontEnd.unsupported(sql);
        assertThat(found).as("expected this SQL to be outside the subset: %s", sql).isPresent();
        return found.get().shape();
    }

    private static void inside(String sql) {
        assertThat(SqlFrontEnd.unsupported(sql))
                .as("expected this SQL to be inside the subset: %s", sql).isEmpty();
    }

    @Test
    @DisplayName("control: one SELECT over tables combined by equality is inside the subset")
    void theSupportedShapesAreNotRefused() {
        // Without this, every assertion below is satisfied by a checker that refuses everything --
        // and a checker that refuses everything looks exactly like a correct one from the outside.
        inside("SELECT * FROM a");
        inside("SELECT o.o_id, c.c_name FROM orders o JOIN customers c ON o.o_cust_id = c.c_id");
        inside("SELECT o.o_id FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id");
        inside("SELECT o.o_id FROM orders o RIGHT JOIN customers c ON o.o_cust_id = c.c_id");
        inside("SELECT o.o_amount * 2 AS doubled FROM orders o");
        inside("SELECT COALESCE(c.c_name, 'x') AS nm FROM customers c");
        inside("SELECT o.o_id FROM orders o JOIN customers c "
                + "ON o.o_cust_id = c.c_id AND o.o_id = c.c_id");
    }

    @Test
    @DisplayName("FULL OUTER JOIN is refused by name")
    void fullOuterJoinIsOutsideTheSubset() {
        // The plan can state it (JoinKind.FULL exists precisely so this refusal can name it), but
        // no carrier here executes it -- so the refusal has to happen before anything runs.
        assertThat(shapeOf("SELECT o.o_id FROM orders o "
                + "FULL OUTER JOIN customers c ON o.o_cust_id = c.c_id"))
                .isEqualTo("FULL OUTER JOIN");
    }

    @Test
    @DisplayName("a join condition that is not an equality between two columns is refused")
    void nonEquiJoinConditionsAreOutsideTheSubset() {
        // The discriminating detail: both of these parse, validate and produce a plan whose key
        // list is EMPTY or SHORT. Executing that plan joins on nothing, or on half the condition,
        // and emits rows nobody asked for -- with no error anywhere.
        String comparison = shapeOf("SELECT o.o_id FROM orders o "
                + "JOIN customers c ON o.o_amount > c.c_id");
        String expression = shapeOf("SELECT o.o_id FROM orders o "
                + "JOIN customers c ON o.o_cust_id = c.c_id + 1");
        String disjunction = shapeOf("SELECT o.o_id FROM orders o "
                + "JOIN customers c ON o.o_cust_id = c.c_id OR o.o_id = c.c_id");

        assertThat(List.of(comparison, expression, disjunction))
                .allMatch(shape -> shape.contains("equality"));
    }

    @Test
    @DisplayName("joins that name no explicit condition are refused, including the comma form")
    void joinsWithoutAnOnConditionAreOutsideTheSubset() {
        assertThat(shapeOf("SELECT o.o_id FROM orders o NATURAL JOIN customers c"))
                .isEqualTo("NATURAL JOIN");
        assertThat(shapeOf("SELECT o.o_id FROM orders o JOIN customers c USING (c_id)"))
                .isEqualTo("USING");
        assertThat(shapeOf("SELECT o.o_id FROM orders o CROSS JOIN customers c"))
                .isEqualTo("CROSS JOIN");
        // the comma form is a cross join wearing different syntax; missing it would let the one
        // shape that multiplies every row by every other row through
        assertThat(shapeOf("SELECT o.o_id FROM orders o, customers c"))
                .isEqualTo("CROSS JOIN");
    }

    @Test
    @DisplayName("aggregation is refused, including an aggregate with no GROUP BY to give it away")
    void aggregationIsOutsideTheSubset() {
        assertThat(shapeOf("SELECT c.c_id, COUNT(*) FROM customers c GROUP BY c.c_id"))
                .isEqualTo("GROUP BY");
        assertThat(shapeOf("SELECT c.c_id FROM customers c GROUP BY c.c_id HAVING COUNT(*) > 1"))
                .isEqualTo("GROUP BY");
        // The one that a structural scan misses: no GROUP BY, no HAVING, nothing in the shape of
        // the statement says "aggregate". The parser reports COUNT and SUM as unresolved functions,
        // indistinguishable from SUBSTRING -- so telling them apart needs the operator table, and
        // an implementation that skips that step passes every other case in this class.
        assertThat(shapeOf("SELECT COUNT(*) AS n FROM orders o")).isEqualTo("COUNT");
        assertThat(shapeOf("SELECT SUM(o.o_amount) AS t FROM orders o")).isEqualTo("SUM");
    }

    @Test
    @DisplayName("a window function is refused")
    void windowFunctionsAreOutsideTheSubset() {
        assertThat(shapeOf("SELECT SUM(o.o_amount) OVER (PARTITION BY o.o_cust_id) AS w "
                + "FROM orders o")).isEqualTo("OVER");
    }

    @Test
    @DisplayName("clauses that add, remove or reorder rows are refused")
    void clausesThatChangeTheRowSetAreOutsideTheSubset() {
        // A maintained join publishes one row per matched combination and nothing else touches the
        // row set. JoinPlan carries no place to put any of these, so a carrier would simply not
        // apply them -- WHERE in particular means every row the filter should have removed is
        // published instead, which is the silent-extra-rows failure this whole layer exists to stop.
        assertThat(shapeOf("SELECT o.o_id FROM orders o WHERE o.o_id > 3")).isEqualTo("WHERE");
        assertThat(shapeOf("SELECT DISTINCT o.o_id FROM orders o")).isEqualTo("DISTINCT");
        assertThat(shapeOf("SELECT o.o_id FROM orders o ORDER BY o.o_id")).isEqualTo("ORDER BY");
        assertThat(shapeOf("SELECT o.o_id FROM orders o LIMIT 10")).isEqualTo("LIMIT");
        assertThat(shapeOf("SELECT o.o_id FROM orders o OFFSET 2 ROWS")).isEqualTo("OFFSET");
    }

    @Test
    @DisplayName("a statement that is not a single SELECT over a FROM is refused")
    void statementsThatAreNotOneSelectAreOutsideTheSubset() {
        assertThat(shapeOf("SELECT o.o_id FROM orders o UNION SELECT c.c_id FROM customers c"))
                .isEqualTo("UNION");
        assertThat(shapeOf("INSERT INTO orders VALUES (1)")).isEqualTo("INSERT");
        assertThat(shapeOf("WITH x AS (SELECT c.c_id FROM customers c) SELECT x.c_id FROM x"))
                .isEqualTo("WITH");
    }

    @Test
    @DisplayName("a subquery anywhere is refused, in the FROM clause and in a predicate alike")
    void subqueriesAreOutsideTheSubset() {
        assertThat(shapeOf("SELECT o.o_id FROM (SELECT * FROM orders) o"))
                .isEqualTo("a subquery");
        assertThat(shapeOf("SELECT o.o_id FROM orders o "
                + "JOIN customers c ON o.o_cust_id = c.c_id "
                + "AND c.c_id IN (SELECT c2.c_id FROM customers c2)"))
                .isEqualTo("a subquery");
    }

    @Test
    @DisplayName("text that is not SQL at all is reported apart from a shape this release refuses")
    void unparsableSqlIsNotReportedAsAnUnsupportedShape() {
        // Two different things for the person reading the message: "you mistyped" and "we do not
        // do that yet". Folding them together sends everyone who typoed off to read a support
        // matrix, and everyone who hit the boundary off to hunt a syntax error.
        assertThatThrownBy(() -> SqlFrontEnd.unsupported("SELECT FROM WHERE"))
                .isInstanceOf(SqlFrontEndException.class);
    }

    @Test
    @DisplayName("the finding points at the line and column the shape sits on")
    void theFindingCarriesItsPosition() {
        Unsupported found = SqlFrontEnd.unsupported(
                "SELECT o.o_id\n"
                        + "FROM orders o\n"
                        + "FULL OUTER JOIN customers c ON o.o_cust_id = c.c_id").orElseThrow();

        assertThat(found.shape()).isEqualTo("FULL OUTER JOIN");
        assertThat(found.line()).as("the join sits on the third line").isEqualTo(3);
        assertThat(found.column()).isPositive();
    }

    @Test
    @DisplayName("whatever the boundary admits, the derived plan states in full")
    void whatTheBoundaryAdmitsThePlanStatesInFull() {
        // The two halves of one contract, and the reason deriving does not run this check itself:
        // a FULL kind and an uncaptured condition are things the plan exists to REPORT, so a
        // derive that threw on them would destroy the only signal the layer above has. What must
        // hold instead is this -- nothing the checker lets through arrives as a plan with
        // something left behind. Were the non-equality case ever dropped from the checker, this
        // fails here rather than as extra rows in somebody's target table a month later.
        List<String> admitted = List.of(
                "SELECT o.o_id, c.c_name FROM orders o JOIN customers c ON o.o_cust_id = c.c_id",
                "SELECT o.o_id FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id",
                "SELECT o.o_id FROM orders o RIGHT JOIN customers c ON o.o_cust_id = c.c_id",
                "SELECT o.o_id FROM orders o JOIN customers c "
                        + "ON o.o_cust_id = c.c_id AND o.o_id = c.c_id");

        for (String sql : admitted) {
            assertThat(SqlFrontEnd.unsupported(sql)).as("admitted: %s", sql).isEmpty();
            JoinTree.Join join = (JoinTree.Join) SqlFrontEnd.derive(sql, TABLES).from();
            assertThat(join.hasUncapturedCondition()).as("fully stated: %s", sql).isFalse();
            assertThat(join.kind()).as("executable kind: %s", sql).isNotEqualTo(JoinKind.FULL);
        }
    }
}
