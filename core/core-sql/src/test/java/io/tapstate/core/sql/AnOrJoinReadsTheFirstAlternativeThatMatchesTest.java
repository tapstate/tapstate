package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A left outer join on {@code a OR b}: a wallet transaction names its location either by location
 * code or by hub location id, and takes the location the first of those finds.
 */
class AnOrJoinReadsTheFirstAlternativeThatMatchesTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("wallet_tran", List.of(
                    new SourceColumn("tran_id", TapstateType.INT64, false),
                    new SourceColumn("locn_code", TapstateType.STRING, true),
                    new SourceColumn("hub_locn_id", TapstateType.STRING, true),
                    new SourceColumn("amount", TapstateType.DECIMAL, false))),
            new SourceTable("location", List.of(
                    new SourceColumn("locn_id", TapstateType.INT64, false),
                    new SourceColumn("locn_code", TapstateType.STRING, true),
                    new SourceColumn("hub_locn_id", TapstateType.STRING, true),
                    new SourceColumn("name", TapstateType.STRING, false))),
            new SourceTable("region", List.of(
                    new SourceColumn("region_id", TapstateType.INT64, false),
                    new SourceColumn("label", TapstateType.STRING, false))));

    private static final String BY_CODE_OR_HUB = "SELECT t.tran_id, l.name, l.locn_id AS location_id "
            + "FROM wallet_tran t LEFT JOIN location l "
            + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id";

    @Test
    @DisplayName("a LEFT JOIN on an OR of equalities is inside the subset")
    void aLeftJoinOnAlternativesIsAdmitted() {
        assertThat(SqlFrontEnd.unsupported(BY_CODE_OR_HUB)).isEmpty();
        assertThat(SqlFrontEnd.unsupported("SELECT t.tran_id, l.name FROM wallet_tran t LEFT JOIN location l "
                + "ON (t.locn_code = l.locn_code AND t.amount = l.locn_id) OR t.hub_locn_id = l.hub_locn_id "
                + "OR t.tran_id = l.locn_id")).isEmpty();
    }

    @Test
    @DisplayName("an OR is refused where first-match-wins is not what the statement says, or cannot be stated")
    void theShapesAnOrCannotTakeAreRefused() {
        assertThat(shapeOf("SELECT t.tran_id FROM wallet_tran t JOIN location l "
                + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id"))
                .as("an inner join on OR may publish a row per alternative; nothing here says which")
                .isEqualTo("OR in a join condition other than LEFT JOIN");
        assertThat(shapeOf("SELECT t.tran_id FROM wallet_tran t LEFT JOIN location l "
                + "ON t.locn_code = l.locn_code OR t.amount > 0"))
                .contains("equality");
        assertThat(shapeOf("SELECT t.tran_id FROM wallet_tran t LEFT JOIN region r ON t.tran_id = r.region_id "
                + "LEFT JOIN location l ON t.locn_code = l.locn_code OR t.tran_id = r.region_id"))
                .isEqualTo("an OR alternative that does not match on the joined table");
        assertThat(shapeOf("SELECT * FROM wallet_tran t LEFT JOIN location l "
                + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id"))
                .isEqualTo("a star over a table joined on OR");
        assertThat(shapeOf("SELECT l.* FROM wallet_tran t LEFT JOIN location l "
                + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id"))
                .isEqualTo("a star over a table joined on OR");
        assertThat(shapeOf("SELECT t.tran_id FROM wallet_tran t LEFT JOIN location l "
                + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id "
                + "LEFT JOIN region r ON r.region_id = l.locn_id"))
                .as("a later join would have to say which alternative's row it matches on")
                .isEqualTo("a join condition on a table joined on OR");
    }

    @Test
    @DisplayName("each alternative is a left join of its own, onto the same table, under a name of its own")
    void eachAlternativeBecomesAJoinOfItsOwn() {
        JoinPlan plan = SqlFrontEnd.derive(BY_CODE_OR_HUB, TABLES);

        assertThat(plan.from().sources()).containsExactly(
                new JoinTree.Source("t", "wallet_tran"),
                new JoinTree.Source("l", "location"),
                new JoinTree.Source("l$or2", "location", "l"));
        assertThat(plan.from().sources()).extracting(JoinTree.Source::declared)
                .as("the added source is fed by the rows the author declared, not by a name nobody declared")
                .containsExactly("t", "l", "l");

        JoinTree.Join outer = (JoinTree.Join) plan.from();
        JoinTree.Join inner = (JoinTree.Join) outer.left();
        assertThat(inner.kind()).isEqualTo(JoinKind.LEFT);
        assertThat(inner.on()).containsExactly(new JoinTree.KeyPair(
                new JoinTree.ColumnRef("t", "locn_code"), new JoinTree.ColumnRef("l", "locn_code")));
        assertThat(outer.kind()).isEqualTo(JoinKind.LEFT);
        assertThat(outer.on()).containsExactly(new JoinTree.KeyPair(
                new JoinTree.ColumnRef("t", "hub_locn_id"), new JoinTree.ColumnRef("l$or2", "hub_locn_id")));
        assertThat(List.of(inner, outer)).noneMatch(JoinTree.Join::hasUncapturedCondition);
    }

    @Test
    @DisplayName("the output row is the one the statement as written describes")
    void theOutputRowKeepsItsNamesAndTypes() {
        JoinPlan plan = SqlFrontEnd.derive(BY_CODE_OR_HUB, TABLES);

        assertThat(plan.outputFields()).extracting(OutputField::name)
                .containsExactly("tran_id", "name", "location_id");
        assertThat(plan.outputFields()).extracting(OutputField::type)
                .containsExactly(TapstateType.INT64, TapstateType.STRING, TapstateType.INT64);
        assertThat(plan.outputFields()).extracting(OutputField::nullable)
                .containsExactly(false, true, true);
    }

    @Test
    @DisplayName("a row takes the first alternative that matched, falls back to the next, and is null when none did")
    void theFirstAlternativeThatMatchedWins() {
        Expr name = SqlFrontEnd.derive(BY_CODE_OR_HUB, TABLES).outputFields().get(1).from();
        Map<String, Object> atrium = location(1, "A", null, "Atrium");
        Map<String, Object> harbor = location(2, null, "H2", "Harbor");

        assertThat(Expressions.evaluate(name, matches(atrium, harbor)))
                .as("both alternatives matched: the one written first wins").isEqualTo("Atrium");
        assertThat(Expressions.evaluate(name, matches(null, harbor)))
                .as("no location has the code: the hub id is the fallback").isEqualTo("Harbor");
        assertThat(Expressions.evaluate(name, matches(atrium, null))).isEqualTo("Atrium");
        assertThat(Expressions.evaluate(name, matches(null, null))).isNull();
    }

    @Test
    @DisplayName("a column of the OR-joined table read without its table name is refused, not guessed")
    void anUnqualifiedColumnOfTheOrJoinedTableIsRefused() {
        assertThatThrownBy(() -> SqlFrontEnd.derive("SELECT t.tran_id, name FROM wallet_tran t LEFT JOIN location l "
                + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id", TABLES))
                .isInstanceOf(SqlFrontEndException.class)
                .hasMessageContaining("ambiguous");
    }

    @Test
    @DisplayName("a name the from clause already uses is never taken for an alternative")
    void anAlternativesNameNeverShadowsADeclaredSource() {
        assertThatThrownBy(() -> SqlFrontEnd.derive("SELECT t.tran_id FROM wallet_tran t "
                + "LEFT JOIN region l$or2 ON t.tran_id = l$or2.region_id "
                + "LEFT JOIN location l ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id", TABLES))
                .isInstanceOf(SqlFrontEndException.class)
                .hasMessageContaining("l$or2");
    }

    private static Map<String, Map<String, Object>> matches(Map<String, Object> byCode, Map<String, Object> byHub) {
        Map<String, Map<String, Object>> sources = new HashMap<>();
        sources.put("t", Map.of("tran_id", 7L));
        sources.put("l", byCode);
        sources.put("l$or2", byHub);
        return sources;
    }

    private static Map<String, Object> location(long id, String code, String hub, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("locn_id", id);
        row.put("locn_code", code);
        row.put("hub_locn_id", hub);
        row.put("name", name);
        return row;
    }

    private static String shapeOf(String sql) {
        Optional<Unsupported> found = SqlFrontEnd.unsupported(sql);
        assertThat(found).as("expected this SQL to be outside the subset: %s", sql).isPresent();
        return found.get().shape();
    }
}
