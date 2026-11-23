package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Evaluating the read vocabulary against a row in this process, which the followed stream needs
 * because its filter cannot travel with it: a change capture is asked for a table, not for a query,
 * so what a follower narrows is what it is shown, never what is captured.
 *
 * <p>That makes this the vocabulary's second implementation — the first translates it into a store's
 * own dialect — and the two agreeing is not checked anywhere a unit test can reach. The cases below
 * therefore pin the places the two could most easily part ways: what a dotted path means when it
 * crosses a list, and which comparisons a mixed pair is allowed to answer at all.
 */
class DataBrowserCriteriaMatchTest {

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            row.put((String) pairs[i], pairs[i + 1]);
        }
        return row;
    }

    private static DataBrowserCriteria match(String field, DataBrowserCriteria.Operator op, Object value) {
        return new DataBrowserCriteria.Match(field, op, value);
    }

    @Test
    @DisplayName("a negated term over a list asks whether the list holds the value, not whether some entry differs")
    void negatesOverTheWholeListRatherThanPerEntry() {
        Map<String, Object> holdsPaid = row("tags", List.of("paid", "priority"));
        Map<String, Object> withoutPaid = row("tags", List.of("shipped", "priority"));

        // The bounded read of the same filter reaches the store as {tags: {$ne: "paid"}}, which does not
        // return a row whose list holds "paid". Any-entry-differs would return it here and nowhere else.
        assertThat(match("tags", DataBrowserCriteria.Operator.NE, "paid").matches(holdsPaid)).isFalse();
        assertThat(match("tags", DataBrowserCriteria.Operator.NE, "paid").matches(withoutPaid)).isTrue();
    }

    @Test
    @DisplayName("a malformed escape in a filter field is a coded refusal, not a bare crash")
    void refusesAFieldSpellingWithACode() {
        assertThatThrownBy(() -> match("price\\usd", DataBrowserCriteria.Operator.EQ, 1))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("TS0210001");
    }

    @Test
    @DisplayName("equality holds on the value itself")
    void matchesAScalarField() {
        Map<String, Object> paid = row("status", "Paid", "total", 12);

        assertThat(match("status", DataBrowserCriteria.Operator.EQ, "Paid").matches(paid)).isTrue();
        assertThat(match("status", DataBrowserCriteria.Operator.EQ, "Shipped").matches(paid)).isFalse();
        assertThat(match("status", DataBrowserCriteria.Operator.NE, "Shipped").matches(paid)).isTrue();
    }

    @Test
    @DisplayName("a field that is not there fails every test but the one asking whether it is there")
    void anAbsentFieldMatchesNothingButAPresenceTest() {
        Map<String, Object> row = row("status", "Paid");

        assertThat(match("tracking", DataBrowserCriteria.Operator.EQ, "1Z999").matches(row)).isFalse();
        assertThat(match("tracking", DataBrowserCriteria.Operator.NE, "1Z999").matches(row))
                .as("an absent field is not 'some other value' — answering true here would show the "
                        + "reader rows that have nothing to do with the field they asked about")
                .isFalse();
        assertThat(match("tracking", DataBrowserCriteria.Operator.EXISTS, false).matches(row)).isTrue();
        assertThat(match("tracking", DataBrowserCriteria.Operator.EXISTS, true).matches(row)).isFalse();
    }

    @Test
    @DisplayName("equality on a list field holds when any entry equals it, as it does in the store")
    void matchesAnyEntryOfAListField() {
        Map<String, Object> row = row("tags", List.of("paid", "priority"));

        assertThat(match("tags", DataBrowserCriteria.Operator.EQ, "priority").matches(row))
                .as("this is the store's own rule for equality against an array, and the two "
                        + "implementations of this vocabulary have to agree on it or the same filter "
                        + "answers differently depending on which view asked")
                .isTrue();
        assertThat(match("tags", DataBrowserCriteria.Operator.EQ, "late").matches(row)).isFalse();
    }

    @Test
    @DisplayName("a dotted path walks into nested objects, and across a list of them")
    void walksADottedPath() {
        Map<String, Object> row = row("shipments",
                List.of(row("carrier", "UPS"), row("carrier", "FedEx")));

        assertThat(match("shipments.carrier", DataBrowserCriteria.Operator.EQ, "FedEx").matches(row)).isTrue();
        assertThat(match("shipments.carrier", DataBrowserCriteria.Operator.EQ, "DHL").matches(row)).isFalse();
        assertThat(match("order.total", DataBrowserCriteria.Operator.EQ, 1).matches(row("order", row("total", 1))))
                .isTrue();
    }

    @Test
    @DisplayName("an escaped dot names a column, a bare dot steps into one")
    void tellsAColumnNamedWithADotFromAPathThroughOne() {
        // Both readings of one spelling, in one row, so neither assertion can pass by the row only having
        // the shape it happens to look for. A source may name a column `price.usd`; that name reaches the
        // store as a top-level key and sits beside a genuinely nested `price`.
        Map<String, Object> row = row("price.usd", 99, "price", row("usd", 1));

        assertThat(match("price\\.usd", DataBrowserCriteria.Operator.EQ, 99).matches(row))
                .as("the escaped spelling names the column, so it finds the column's value")
                .isTrue();
        assertThat(match("price\\.usd", DataBrowserCriteria.Operator.EQ, 1).matches(row))
                .as("and not the nested one, which is the reading it was written to exclude")
                .isFalse();
        assertThat(match("price.usd", DataBrowserCriteria.Operator.EQ, 1).matches(row))
                .as("the bare spelling still steps, which is what every path already written means")
                .isTrue();
        assertThat(match("price.usd", DataBrowserCriteria.Operator.EQ, 99).matches(row))
                .as("a step never reaches the column that merely spells like one")
                .isFalse();
    }

    @Test
    @DisplayName("a step that reaches nothing stays at nothing, even where the name would have matched")
    void doesNotFallBackToTheLiteralNameWhenTheStepReachesNothing() {
        // The row above holds both readings, which is what lets it tell them apart -- and is also why it
        // cannot see this: a step that resolves nowhere never happens in it. Here only the column exists,
        // so the tempting repair is available -- when the path reaches nothing, try the spelling as a name.
        // Taking it would make one filter mean two things depending on what the document it meets happens
        // to hold, and nothing would report the difference. The escape is what asks for the column.
        Map<String, Object> onlyTheColumn = row("price.usd", 99);

        assertThat(match("price.usd", DataBrowserCriteria.Operator.EQ, 99).matches(onlyTheColumn))
                .as("the bare spelling steps, finds no `price` to step into, and stays there")
                .isFalse();
        assertThat(match("price\\.usd", DataBrowserCriteria.Operator.EQ, 99).matches(onlyTheColumn))
                .as("the value is right there, so the refusal above is a reading and not an empty row")
                .isTrue();
    }

    @Test
    @DisplayName("comparisons order numbers by value, not by how they were spelt")
    void comparesNumbersByValue() {
        Map<String, Object> row = row("total", 12);

        assertThat(match("total", DataBrowserCriteria.Operator.GT, 5).matches(row)).isTrue();
        assertThat(match("total", DataBrowserCriteria.Operator.GT, 12).matches(row)).isFalse();
        assertThat(match("total", DataBrowserCriteria.Operator.GTE, 12).matches(row)).isTrue();
        assertThat(match("total", DataBrowserCriteria.Operator.LT, 12.5).matches(row))
                .as("a whole number and a decimal are one number line; comparing them as their Java "
                        + "types would answer by which class the reader's JSON happened to parse into")
                .isTrue();
    }

    @Test
    @DisplayName("equality holds on the number, not on the type the number arrived as")
    void matchesNumbersByValueRatherThanByTheTypeTheyArrivedAs() {
        // A follower's filter came from JSON text, so every whole number in it is a Long; the row came
        // from the connector, so an int32 column arrives as an Integer. Comparing those by type is a
        // filter that matches nothing and reports nothing -- the reader watches a table that never
        // seems to change. The ordered operators already read one number line; these three did not.
        Map<String, Object> row = row("total", 100);

        assertThat(match("total", DataBrowserCriteria.Operator.EQ, 100L).matches(row)).isTrue();
        assertThat(match("total", DataBrowserCriteria.Operator.NE, 100L).matches(row)).isFalse();
        assertThat(match("total", DataBrowserCriteria.Operator.IN, List.of(100L, 200L)).matches(row))
                .isTrue();
        assertThat(match("total", DataBrowserCriteria.Operator.EQ, 100.0d).matches(row))
                .as("a whole number and a decimal are the same point on the one number line")
                .isTrue();

        assertThat(match("total", DataBrowserCriteria.Operator.EQ, 101L).matches(row)).isFalse();
        assertThat(match("total", DataBrowserCriteria.Operator.NE, 101L).matches(row)).isTrue();
        assertThat(match("total", DataBrowserCriteria.Operator.IN, List.of(200L, 300L)).matches(row))
                .isFalse();
    }

    @Test
    @DisplayName("equality between whole numbers is exact, however far apart a double would put them")
    void doesNotCallTwoDistantWholeNumbersEqualBecauseTheyRoundAlike() {
        // Past a double's 53 bits of mantissa, distinct integers round to the same double. Ordering
        // two of those slightly wrong is invisible; calling them equal shows the reader another row.
        Map<String, Object> row = row("id", 9_007_199_254_740_993L);

        assertThat(match("id", DataBrowserCriteria.Operator.EQ, 9_007_199_254_740_992L).matches(row))
                .isFalse();
        assertThat(match("id", DataBrowserCriteria.Operator.EQ, 9_007_199_254_740_993L).matches(row))
                .isTrue();
    }

    @Test
    @DisplayName("a number and something that is not one are still not equal")
    void doesNotReadTextOrBooleansOntoTheNumberLine() {
        assertThat(match("total", DataBrowserCriteria.Operator.EQ, 100L).matches(row("total", "100")))
                .as("the store does not match a number against its text either, and a reader who "
                        + "wrote one meaning the other is better told nothing matched")
                .isFalse();
        assertThat(match("flag", DataBrowserCriteria.Operator.EQ, 1L).matches(row("flag", true)))
                .isFalse();
    }

    @Test
    @DisplayName("a comparison between kinds that have no order answers false rather than guessing")
    void refusesToOrderValuesThatHaveNoOrder() {
        assertThat(match("total", DataBrowserCriteria.Operator.GT, 5).matches(row("total", "twelve")))
                .as("ordering text against a number by any rule at all would silently show or hide "
                        + "rows on a comparison nobody asked for")
                .isFalse();
    }

    @Test
    @DisplayName("membership and substring hold to what their operators promise")
    void matchesMembershipAndSubstring() {
        assertThat(match("status", DataBrowserCriteria.Operator.IN, List.of("Paid", "Shipped"))
                .matches(row("status", "Shipped"))).isTrue();
        assertThat(match("status", DataBrowserCriteria.Operator.IN, List.of("Paid"))
                .matches(row("status", "Shipped"))).isFalse();
        assertThat(match("note", DataBrowserCriteria.Operator.CONTAINS, "urgent")
                .matches(row("note", "this is urgent, please"))).isTrue();
        assertThat(match("note", DataBrowserCriteria.Operator.CONTAINS, ".*")
                .matches(row("note", "this is urgent")))
                .as("the one operator that takes free text takes it literally — the store side escapes "
                        + "it whole, and an evaluator that treated it as a pattern would make the same "
                        + "filter mean two different things")
                .isFalse();
    }

    @Test
    @DisplayName("a combination holds when its members do")
    void matchesCombinations() {
        Map<String, Object> row = row("status", "Paid", "total", 12);
        DataBrowserCriteria.Match paid = new DataBrowserCriteria.Match(
                "status", DataBrowserCriteria.Operator.EQ, "Paid");
        DataBrowserCriteria.Match shipped = new DataBrowserCriteria.Match(
                "status", DataBrowserCriteria.Operator.EQ, "Shipped");
        DataBrowserCriteria.Match dear = new DataBrowserCriteria.Match(
                "total", DataBrowserCriteria.Operator.GT, 10);

        assertThat(new DataBrowserCriteria.All(List.of(paid, dear)).matches(row)).isTrue();
        assertThat(new DataBrowserCriteria.All(List.of(shipped, dear)).matches(row)).isFalse();
        assertThat(new DataBrowserCriteria.Any(List.of(paid, shipped)).matches(row)).isTrue();
        assertThat(new DataBrowserCriteria.Any(List.of(shipped)).matches(row)).isFalse();
        assertThat(new DataBrowserCriteria.All(List.of(dear, new DataBrowserCriteria.Any(List.of(paid, shipped))))
                .matches(row))
                .as("the one nested shape the vocabulary can spell")
                .isTrue();
    }
}
