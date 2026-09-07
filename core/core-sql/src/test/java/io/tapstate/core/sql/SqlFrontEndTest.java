package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlFrontEndTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true),
                    new SourceColumn("o_region", TapstateType.STRING, true),
                    new SourceColumn("o_amount", TapstateType.DECIMAL, false))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_region", TapstateType.STRING, true),
                    new SourceColumn("c_name", TapstateType.STRING, false))),
            new SourceTable("payments", List.of(
                    new SourceColumn("p_order_id", TapstateType.INT64, false),
                    new SourceColumn("p_method", TapstateType.STRING, false))));

    @Test
    @DisplayName("the output row's names and types come from the SQL, including a computed column")
    void derivesNamesAndTypes() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id, c.c_name AS customer_name, o.o_amount * 2 AS doubled "
                        + "FROM orders o JOIN customers c ON o.o_cust_id = c.c_id", TABLES);

        assertThat(plan.outputFields()).extracting(OutputField::name)
                .containsExactly("o_id", "customer_name", "doubled");
        assertThat(plan.outputFields()).extracting(OutputField::type)
                .containsExactly(TapstateType.INT64, TapstateType.STRING, TapstateType.DECIMAL);
    }

    @Test
    @DisplayName("a LEFT JOIN makes the dimension side nullable even though its source column is not")
    void leftJoinWidensNullability() {
        // The discriminating case: c_name is declared NOT NULL, so an implementation that copies
        // the source column's nullability gets the inner-join answer right and this one wrong --
        // and the difference is invisible until a fact row has no matching dimension row.
        JoinPlan inner = SqlFrontEnd.derive(
                "SELECT c.c_name FROM orders o JOIN customers c ON o.o_cust_id = c.c_id", TABLES);
        JoinPlan left = SqlFrontEnd.derive(
                "SELECT c.c_name FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id", TABLES);

        assertThat(inner.outputFields()).singleElement()
                .extracting(OutputField::nullable).isEqualTo(false);
        assertThat(left.outputFields()).singleElement()
                .extracting(OutputField::nullable).isEqualTo(true);
    }

    @Test
    @DisplayName("a RIGHT JOIN is rewritten into a LEFT JOIN over swapped sides")
    void rightJoinIsRewrittenIntoASwappedLeftJoin() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT c.c_name, o.o_id FROM customers c RIGHT JOIN orders o "
                        + "ON o.o_cust_id = c.c_id", TABLES);

        JoinTree.Join join = (JoinTree.Join) plan.from();
        assertThat(join.kind()).isEqualTo(JoinKind.LEFT);
        assertThat(join.left()).isEqualTo(new JoinTree.Source("o", "orders"));
        assertThat(join.right()).isEqualTo(new JoinTree.Source("c", "customers"));
        assertThat(plan.factSource().table())
                .as("the preserved side is what rows are driven from, and the rewrite must move it")
                .isEqualTo("orders");
        assertThat(join.on()).singleElement().satisfies(pair -> {
            assertThat(pair.left().column())
                    .as("the key pair swaps with the sides, or a carrier probes with the wrong column")
                    .isEqualTo("o_cust_id");
            assertThat(pair.right().column()).isEqualTo("c_id");
        });

        assertThat(plan.outputFields()).extracting(OutputField::name)
                .as("the rewrite is invisible in the output: the row a person asked for does not move")
                .containsExactly("c_name", "o_id");
        assertThat(plan.outputFields()).extracting(OutputField::nullable)
                .as("and the side that may be missing is still the one the original SQL said")
                .containsExactly(true, false);
    }

    @Test
    @DisplayName("a LEFT JOIN keeps the sides it was written with")
    void leftJoinKeepsItsSides() {
        // Control for the rewrite above: an implementation that always swaps, or never swaps,
        // passes one of these two and fails the other.
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT c.c_name, o.o_id FROM customers c LEFT JOIN orders o "
                        + "ON o.o_cust_id = c.c_id", TABLES);

        JoinTree.Join join = (JoinTree.Join) plan.from();
        assertThat(join.kind()).isEqualTo(JoinKind.LEFT);
        assertThat(join.left()).isEqualTo(new JoinTree.Source("c", "customers"));
        assertThat(plan.factSource().table()).isEqualTo("customers");
        assertThat(join.on()).singleElement().satisfies(pair -> {
            assertThat(pair.left().column()).isEqualTo("c_id");
            assertThat(pair.right().column()).isEqualTo("o_cust_id");
        });
    }

    @Test
    @DisplayName("every equality of a composite ON becomes a key pair")
    void compositeOnBecomesEveryKeyPair() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id FROM orders o JOIN customers c "
                        + "ON o.o_cust_id = c.c_id AND o.o_region = c.c_region", TABLES);

        JoinTree.Join join = (JoinTree.Join) plan.from();
        assertThat(join.on())
                .as("an implementation reading only the condition's top operator finds none: it is AND")
                .hasSize(2);
        assertThat(join.on()).extracting(pair -> pair.left().column())
                .containsExactly("o_cust_id", "o_region");
        assertThat(join.hasUncapturedCondition()).isFalse();
    }

    @Test
    @DisplayName("a condition that is not an equality between the two sides is reported, not dropped")
    void nonEquiConditionIsReported() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id FROM orders o JOIN customers c "
                        + "ON o.o_cust_id = c.c_id AND o.o_amount > 100", TABLES);

        JoinTree.Join join = (JoinTree.Join) plan.from();
        assertThat(join.on()).hasSize(1);
        assertThat(join.hasUncapturedCondition())
                .as("dropping the range predicate emits rows that should not exist, and they look real")
                .isTrue();
    }

    @Test
    @DisplayName("the join kinds are reported as written, and a right outer is never one of them")
    void joinKindsAreReportedAsWritten() {
        assertThat(kindOf("JOIN")).isEqualTo(JoinKind.INNER);
        assertThat(kindOf("LEFT JOIN")).isEqualTo(JoinKind.LEFT);
        assertThat(kindOf("FULL JOIN")).isEqualTo(JoinKind.FULL);
        assertThat(kindOf("RIGHT JOIN"))
                .as("there is no right-outer kind to report, so it must have become a left one")
                .isEqualTo(JoinKind.LEFT);
    }

    @Test
    @DisplayName("a chain of joins keeps one fact source at the head and one key pair per join")
    void chainOfJoins() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id, c.c_name, p.p_method FROM orders o "
                        + "JOIN customers c ON o.o_cust_id = c.c_id "
                        + "JOIN payments p ON p.p_order_id = o.o_id", TABLES);

        assertThat(plan.factSource()).isEqualTo(new JoinTree.Source("o", "orders"));
        assertThat(plan.from().sources()).extracting(JoinTree.Source::table)
                .containsExactly("orders", "customers", "payments");

        JoinTree.Join outer = (JoinTree.Join) plan.from();
        assertThat(outer.on()).singleElement().satisfies(pair ->
                assertThat(pair.right().column()).isEqualTo("p_order_id"));
        assertThat(((JoinTree.Join) outer.left()).on()).singleElement().satisfies(pair ->
                assertThat(pair.right().column()).isEqualTo("c_id"));
    }

    @Test
    @DisplayName("a source is read for every column the SQL names, not only the projected ones")
    void readColumnsCoverEveryPlaceAColumnIsNamed() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT o.o_id, c.c_name FROM orders o JOIN customers c ON o.o_cust_id = c.c_id "
                        + "WHERE o.o_region = 'north'", TABLES);

        assertThat(plan.readColumns().get("o"))
                .as("the join key and the filtered column are never projected, and a carrier that "
                        + "mirrored the output row alone could not match a single row")
                .containsExactly("o_cust_id", "o_id", "o_region");
        assertThat(plan.readColumns().get("c")).containsExactly("c_id", "c_name");
        assertThat(plan.readColumns())
                .as("every source gets an entry, so an empty one reads as 'read for nothing' "
                        + "rather than as 'never looked at'")
                .containsOnlyKeys("o", "c");
    }

    private static JoinKind kindOf(String joinWords) {
        JoinPlan plan = SqlFrontEnd.derive("SELECT o.o_id FROM orders o " + joinWords
                + " customers c ON o.o_cust_id = c.c_id", TABLES);
        return ((JoinTree.Join) plan.from()).kind();
    }
}
