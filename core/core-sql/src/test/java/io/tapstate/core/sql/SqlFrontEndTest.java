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
                    new SourceColumn("o_amount", TapstateType.DECIMAL, false))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

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
}
