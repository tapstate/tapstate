package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where each column of the output row comes from, and what it works out to.
 *
 * <p>The plan used to say what the row was called and what each column's type was, and nothing about
 * where the values came from. A carrier could publish a row of exactly the right shape holding
 * whatever it guessed - and the shape being right is what makes the guess invisible, because every
 * check downstream is on the shape.
 */
class JoinProjectionTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true),
                    new SourceColumn("o_qty", TapstateType.INT64, false),
                    new SourceColumn("o_price", TapstateType.DECIMAL, false))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    private static final String FROM =
            " FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

    @Test
    @DisplayName("a projected column says which source and which column it reads")
    void aColumnNamesWhereItComesFrom() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id, c.c_name AS customer_name" + FROM, TABLES);

        assertThat(plan.outputFields()).extracting(OutputField::from).containsExactly(
                new Expr.Column(new JoinTree.ColumnRef("o", "o_id")),
                new Expr.Column(new JoinTree.ColumnRef("c", "c_name")));
    }

    @Test
    @DisplayName("an unqualified column is resolved to the source that holds it")
    void anUnqualifiedColumnIsResolved() {
        JoinPlan plan = SqlFrontEnd.derive("SELECT c_name" + FROM, TABLES);

        assertThat(plan.outputFields()).singleElement().extracting(OutputField::from)
                .isEqualTo(new Expr.Column(new JoinTree.ColumnRef("c", "c_name")));
    }

    @Test
    @DisplayName("a star stands for every column of every source, in the order the from clause names them")
    void aStarExpandsToEveryColumn() {
        JoinPlan plan = SqlFrontEnd.derive("SELECT *" + FROM, TABLES);

        assertThat(plan.outputFields()).extracting(OutputField::from).containsExactly(
                new Expr.Column(new JoinTree.ColumnRef("o", "o_id")),
                new Expr.Column(new JoinTree.ColumnRef("o", "o_cust_id")),
                new Expr.Column(new JoinTree.ColumnRef("o", "o_qty")),
                new Expr.Column(new JoinTree.ColumnRef("o", "o_price")),
                new Expr.Column(new JoinTree.ColumnRef("c", "c_id")),
                new Expr.Column(new JoinTree.ColumnRef("c", "c_name")));
    }

    @Test
    @DisplayName("a qualified star stands for that source's columns alone")
    void aQualifiedStarExpandsToOneSource() {
        JoinPlan plan = SqlFrontEnd.derive("SELECT c.*" + FROM, TABLES);

        assertThat(plan.outputFields()).extracting(OutputField::from).containsExactly(
                new Expr.Column(new JoinTree.ColumnRef("c", "c_id")),
                new Expr.Column(new JoinTree.ColumnRef("c", "c_name")));
    }

    @Test
    @DisplayName("a row-level expression is carried as an operator over its arguments")
    void anExpressionIsCarriedRatherThanLost() {
        JoinPlan plan = SqlFrontEnd.derive("SELECT o.o_qty * o.o_price AS amt" + FROM, TABLES);

        assertThat(plan.outputFields()).singleElement().extracting(OutputField::from)
                .isEqualTo(new Expr.Call("*", List.of(
                        new Expr.Column(new JoinTree.ColumnRef("o", "o_qty")),
                        new Expr.Column(new JoinTree.ColumnRef("o", "o_price")))));
    }

    @Test
    @DisplayName("the projected values are what the SQL says they are")
    void theValuesAreWorkedOut() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id AS order_id, c.c_name AS customer_name, "
                        + "o.o_qty * o.o_price AS amt" + FROM, TABLES);

        Map<String, Object> row = project(plan, Map.of(
                "o", Map.of("o_id", 7L, "o_qty", 3L, "o_price", new BigDecimal("2.50")),
                "c", Map.of("c_name", "Ada")));

        assertThat(row).containsExactly(
                Map.entry("order_id", 7L),
                Map.entry("customer_name", "Ada"),
                Map.entry("amt", new BigDecimal("7.50")));
    }

    /**
     * The case a left outer join makes routine rather than exotic. Every unmatched fact row arrives
     * with the whole dimension side missing, so an evaluation that treated an absent row as anything
     * but nulls would be wrong on exactly the rows this join kind exists to keep - and it would be
     * wrong with a plausible value rather than with an error.
     */
    @Test
    @DisplayName("an unmatched side makes its columns null, and arithmetic over a null is null")
    void anUnmatchedSideIsNullRatherThanMissing() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT c.c_name AS customer_name, c.c_id + 1 AS bumped" + FROM, TABLES);

        Map<String, Object> row = project(plan, Map.of("o", Map.of("o_id", 7L)));

        assertThat(row).containsEntry("customer_name", null).containsEntry("bumped", null);
    }

    @Test
    @DisplayName("a value is published as the type the plan declared, not as whatever arithmetic left")
    void aValueIsCoercedToItsDeclaredType() {
        JoinPlan plan = SqlFrontEnd.derive("SELECT o.o_id + 1 AS next_id" + FROM, TABLES);

        assertThat(plan.outputFields()).singleElement()
                .extracting(OutputField::type).isEqualTo(TapstateType.INT64);
        assertThat(project(plan, Map.of("o", Map.of("o_id", 7L))))
                .containsEntry("next_id", 8L);
    }

    /**
     * The gate that keeps the admitted set and the evaluated set from drifting. Both failures are
     * silent in opposite directions: an operator admitted but not evaluated dies on whichever row
     * first reaches that column, and one evaluated but not admitted is a capability nobody can use.
     */
    @Test
    @DisplayName("every operator the front end admits is one a plan can be evaluated with")
    void everyAdmittedOperatorIsEvaluated() {
        Map<String, String> samples = new LinkedHashMap<>();
        samples.put("+", "o.o_qty + 1");
        samples.put("-", "o.o_qty - 1");
        samples.put("*", "o.o_qty * 2");
        samples.put("/", "o.o_price / 2");
        samples.put("=", "o.o_qty = 3");
        samples.put("<>", "o.o_qty <> 3");
        samples.put("<", "o.o_qty < 3");
        samples.put("<=", "o.o_qty <= 3");
        samples.put(">", "o.o_qty > 3");
        samples.put(">=", "o.o_qty >= 3");
        samples.put("AND", "o.o_qty > 1 AND o.o_qty < 9");
        samples.put("OR", "o.o_qty > 1 OR o.o_qty < 9");
        samples.put("NOT", "NOT (o.o_qty > 1)");
        samples.put("IS NULL", "c.c_name IS NULL");
        samples.put("IS NOT NULL", "c.c_name IS NOT NULL");
        samples.put("CASE", "CASE WHEN o.o_qty > 1 THEN 'many' ELSE 'one' END");
        samples.put("COALESCE", "COALESCE(c.c_name, 'nobody')");
        samples.put("||", "c.c_name || '!'");
        samples.put("UPPER", "UPPER(c.c_name)");
        samples.put("LOWER", "LOWER(c.c_name)");
        samples.put("CHAR_LENGTH", "CHAR_LENGTH(c.c_name)");
        samples.put("SUBSTRING", "SUBSTRING(c.c_name FROM 1 FOR 2)");

        assertThat(new TreeSet<>(samples.keySet()))
                .as("a sample per operator, so the walk below actually reaches all of them")
                .isEqualTo(new TreeSet<>(Expressions.SUPPORTED));

        Set<String> reached = new TreeSet<>();
        Map<String, Map<String, Object>> sources = Map.of(
                "o", Map.of("o_id", 7L, "o_qty", 3L, "o_price", new BigDecimal("2.50")),
                "c", Map.of("c_id", 1L, "c_name", "Ada"));
        samples.forEach((operator, sql) -> {
            assertThat(SqlFrontEnd.unsupported("SELECT " + sql + FROM))
                    .as("%s is admitted", operator).isEqualTo(Optional.empty());
            JoinPlan plan = SqlFrontEnd.derive("SELECT " + sql + " AS v" + FROM, TABLES);
            Expr from = plan.outputFields().get(0).from();
            operatorsIn(from, reached);
            // The assertion is that this returns at all: an operator nothing evaluates throws here.
            Expressions.evaluate(from, sources);
        });

        Set<String> admittedAndReached = new TreeSet<>(Expressions.SUPPORTED);
        admittedAndReached.removeAll(REWRITTEN_AWAY);
        assertThat(reached).containsAll(admittedAndReached);
    }

    /**
     * Operators the validator turns into something else before a plan is built, so no plan ever
     * carries one. They stay admitted - the SQL is legal and a person may write it - and they stay
     * evaluated, because what is rewritten is a property of this version of the SQL library rather
     * than of the operator. Named here rather than dropped from the walk above: a rewrite that stops
     * happening on an upgrade then reddens this case instead of a running job.
     */
    private static final Set<String> REWRITTEN_AWAY = Set.of("COALESCE");

    @Test
    @DisplayName("the operators named as rewritten really are rewritten, so the walk skips nothing else")
    void theRewrittenOperatorsAreActuallyRewritten() {
        Set<String> reached = new TreeSet<>();
        operatorsIn(SqlFrontEnd.derive("SELECT COALESCE(c.c_name, 'nobody') AS v" + FROM, TABLES)
                .outputFields().get(0).from(), reached);

        assertThat(reached).doesNotContain("COALESCE").contains("CASE");
    }

    @Test
    @DisplayName("an operator no carrier can work out is refused by name while the SQL is still text")
    void anUnevaluableOperatorIsRefusedByName() {
        assertThat(SqlFrontEnd.unsupported("SELECT CAST(o.o_id AS VARCHAR)" + FROM))
                .get().extracting(Unsupported::shape).isEqualTo("CAST");
    }

    @Test
    @DisplayName("an aggregate is still reported as itself rather than as an unknown operator")
    void anAggregateKeepsItsOwnRefusal() {
        assertThat(SqlFrontEnd.unsupported("SELECT SUM(o.o_qty)" + FROM))
                .get().extracting(Unsupported::shape).isEqualTo("SUM");
    }

    /** The flat row a plan publishes for one match, in the order it publishes its columns. */
    private static Map<String, Object> project(JoinPlan plan,
            Map<String, Map<String, Object>> sources) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (OutputField field : plan.outputFields()) {
            row.put(field.name(),
                    Expressions.coerce(Expressions.evaluate(field.from(), sources), field.type()));
        }
        return row;
    }

    private static void operatorsIn(Expr expression, Set<String> into) {
        if (expression instanceof Expr.Call call) {
            into.add(call.operator());
            call.arguments().forEach(argument -> operatorsIn(argument, into));
        }
    }
}
