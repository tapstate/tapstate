package io.tapstate.control.restapi;

import io.tapstate.control.core.DataBrowserCriteria;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The filter that arrives as text. A follow opens over a websocket and a handshake has no body to bind,
 * so the filter rides in the handshake's query as the same JSON a read would have posted. The claim that
 * makes is that the two roads end in the same place: the same criteria for a filter that is well formed,
 * and the same refusal, word for word, for one that is not.
 *
 * <p>So every vocabulary case below is written twice -- once as text, once as the body it spells --
 * and both are compared against one expected outcome. Asserting the written road alone against a
 * literal would pass just as well against a second grammar grown here, and a second grammar answering
 * the same filter differently is the one drift this file exists to catch.
 *
 * <p>Where the comparison stops, said plainly rather than left to be assumed from the cases that are
 * here: it starts at the vocabulary, with a filter already read off the wire. What refuses a key that
 * is not one of the five is the strict mapper the body road binds through, and this road binds through
 * nothing -- so a written filter carrying an unknown key alongside a good term is read rather than
 * refused, where the same body is a 400. That difference is real and is not asserted either way below,
 * because a test written around it would pin it in place.
 */
class WrittenFilterTest {

    private static final String MALFORMED = "TS0210001";

    @ParameterizedTest(name = "{0}")
    @MethodSource("theVocabularyDownBothRoads")
    void aWrittenFilterIsReadAsTheBodyItSpells(
            String name, String written, DataBrowserFindRequest.Filter posted, Outcome expected) {
        assertAll(
                () -> assertThat(reading(() -> WrittenFilter.of(written)))
                        .as("written as text").isEqualTo(expected),
                () -> assertThat(reading(() -> DataBrowserController.criteria(posted)))
                        .as("bound from a body").isEqualTo(expected));
    }

    static Stream<Arguments> theVocabularyDownBothRoads() {
        return Stream.of(
                // Every shape the vocabulary has, so a road that grew its own grammar could not agree
                // with this table by accident.
                road("a term",
                        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}",
                        term("status", "eq", "paid"),
                        read(match("status", DataBrowserCriteria.Operator.EQ, "paid"))),
                road("a term on a dot path, since documents nest",
                        "{\"field\":\"customer.city\",\"op\":\"eq\",\"value\":\"Berlin\"}",
                        term("customer.city", "eq", "Berlin"),
                        read(match("customer.city", DataBrowserCriteria.Operator.EQ, "Berlin"))),
                road("an operator spelt in capitals",
                        "{\"field\":\"status\",\"op\":\"EQ\",\"value\":\"paid\"}",
                        term("status", "EQ", "paid"),
                        read(match("status", DataBrowserCriteria.Operator.EQ, "paid"))),
                road("a presence test, whose value is a boolean rather than text",
                        "{\"field\":\"note\",\"op\":\"exists\",\"value\":true}",
                        term("note", "exists", true),
                        read(match("note", DataBrowserCriteria.Operator.EXISTS, true))),
                road("a membership test, whose value is a list rather than one value",
                        "{\"field\":\"status\",\"op\":\"in\",\"value\":[\"new\",\"paid\"]}",
                        term("status", "in", List.of("new", "paid")),
                        read(match("status", DataBrowserCriteria.Operator.IN, List.of("new", "paid")))),
                road("a conjunction",
                        "{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                                + "{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"gold\"}]}",
                        all(term("status", "eq", "paid"), term("tier", "eq", "gold")),
                        read(new DataBrowserCriteria.All(List.of(
                                match("status", DataBrowserCriteria.Operator.EQ, "paid"),
                                match("tier", DataBrowserCriteria.Operator.EQ, "gold"))))),
                road("an alternative",
                        "{\"any\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"new\"},"
                                + "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}",
                        any(term("status", "eq", "new"), term("status", "eq", "paid")),
                        read(new DataBrowserCriteria.Any(List.of(
                                match("status", DataBrowserCriteria.Operator.EQ, "new"),
                                match("status", DataBrowserCriteria.Operator.EQ, "paid"))))),
                road("an alternative inside a conjunction, the one nesting there is",
                        "{\"all\":[{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"gold\"},"
                                + "{\"any\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"new\"},"
                                + "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}]}",
                        all(term("tier", "eq", "gold"),
                                any(term("status", "eq", "new"), term("status", "eq", "paid"))),
                        read(new DataBrowserCriteria.All(List.of(
                                match("tier", DataBrowserCriteria.Operator.EQ, "gold"),
                                new DataBrowserCriteria.Any(List.of(
                                        match("status", DataBrowserCriteria.Operator.EQ, "new"),
                                        match("status", DataBrowserCriteria.Operator.EQ, "paid"))))))),

                // Every refusal the vocabulary raises, reached from text. These are the words a follower
                // is answered with, and they have to be the read face's words rather than this road's.
                road("neither a term nor a combination",
                        "{}",
                        term(null, null, null),
                        refused("a `filter` is either one term (`field`, `op`, `value`) "
                                + "or one combination (`all` or `any`) of them")),
                road("a term that is also a combination",
                        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\","
                                + "\"all\":[{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"gold\"}]}",
                        new DataBrowserFindRequest.Filter("status", "eq", "paid",
                                List.of(term("tier", "eq", "gold")), null),
                        refused("a `filter` is either one term (`field`, `op`, `value`) "
                                + "or one combination (`all` or `any`) of them")),
                road("a combination that is both `all` and `any`",
                        "{\"all\":[{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"gold\"}],"
                                + "\"any\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"new\"}]}",
                        new DataBrowserFindRequest.Filter(null, null, null,
                                List.of(term("tier", "eq", "gold")), List.of(term("status", "eq", "new"))),
                        refused("a `filter` combines with `all` or with `any`, not both")),
                road("an operator outside the vocabulary, including the backend's own spellings",
                        "{\"field\":\"status\",\"op\":\"$where\",\"value\":\"1\"}",
                        term("status", "$where", "1"),
                        refused("`$where` is not a filter operator; the ones there are: "
                                + "eq, ne, gt, gte, lt, lte, in, exists, contains")),
                road("a term with no field",
                        "{\"op\":\"eq\",\"value\":\"paid\"}",
                        term(null, "eq", "paid"),
                        refused("a filter term needs the `field` to test")),
                road("a term with no op",
                        "{\"field\":\"status\",\"value\":\"paid\"}",
                        term("status", null, "paid"),
                        refused("a filter term needs an `op`")),
                road("a term with no value",
                        "{\"field\":\"status\",\"op\":\"eq\"}",
                        term("status", "eq", null),
                        refused("a filter term needs a `value` to test against")),
                road("a combination inside an alternative, which is the nesting bound",
                        "{\"any\":[{\"all\":[{\"field\":\"tier\",\"op\":\"eq\",\"value\":\"gold\"}]}]}",
                        any(all(term("tier", "eq", "gold"))),
                        refused("a `filter` alternative holds terms, not further combinations")),
                road("an empty combination",
                        "{\"all\":[]}",
                        all(),
                        refused("a filter combination needs at least one term")),
                road("a membership test whose value is not a set",
                        "{\"field\":\"status\",\"op\":\"in\",\"value\":\"paid\"}",
                        term("status", "in", "paid"),
                        refused("`in` on `status` takes a non-empty list of values")),
                road("a presence test given the word true rather than true",
                        "{\"field\":\"note\",\"op\":\"exists\",\"value\":\"true\"}",
                        term("note", "exists", "true"),
                        refused("`exists` on `note` takes true or false")));
    }

    @Test
    void nothingWrittenIsNoFilterAtAll() {
        // Absent is a request rather than a gap on both roads: it reads every row. A follower that sent
        // no filter at all and one that sent an empty query string are the same follower.
        assertAll(
                () -> assertThat(WrittenFilter.of(null)).isNull(),
                () -> assertThat(WrittenFilter.of("")).isNull(),
                () -> assertThat(WrittenFilter.of("   ")).isNull(),
                () -> assertThat(DataBrowserController.criteria(null)).isNull());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "status: paid", "{\"field\":}", "", "  "})
    void theRoadsPartOnlyWhereTheOtherOneCannotReachAtAll(String written) {
        // Text that is not JSON has no counterpart on the body road -- there a malformed body never
        // reaches the vocabulary at all. Whatever the answer here, it stays a coded refusal or a clean
        // absence, never the decode failure that would surface as a 500.
        Outcome outcome = reading(() -> WrittenFilter.of(written));
        assertThat(outcome.code()).isIn(null, MALFORMED);
    }

    @Test
    void textThatIsNotJsonIsRefusedWithTheReasonAndTheCauseItFailedOn() {
        TapstateException refused = (TapstateException) org.assertj.core.api.Assertions.catchThrowable(
                () -> WrittenFilter.of("status: paid"));

        assertAll(
                () -> assertThat(refused.code().code()).isEqualTo(MALFORMED),
                () -> assertThat(refused.args()).containsEntry("reason", "the `filter` is not readable as JSON"),
                // The parse failure is carried rather than dropped: a reader debugging their own query
                // string is the only one who can use it, and the rendered reason says nothing about where.
                () -> assertThat(refused.getCause()).isNotNull());
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "\"paid\"", "3", "true", "null",
            "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"})
    void aFilterThatIsReadableJsonButNotAnObjectIsRefused(String written) {
        // A list of terms is the shape most likely to be tried, since that is what a combination holds --
        // and the answer has to name what a filter is rather than fail on a cast deeper in.
        assertThat(reading(() -> WrittenFilter.of(written)))
                .isEqualTo(refused("a `filter` is an object"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"all\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}}",
            "{\"any\":\"status\"}",
            "{\"all\":3}"})
    void aCombinationThatIsNotAListOfFiltersIsRefused(String written) {
        // One term written where a list of them belongs. On the body road the bind refuses this; here
        // nothing binds, so the check has to exist rather than be inherited.
        assertThat(reading(() -> WrittenFilter.of(written)))
                .isEqualTo(refused("a `filter` combination holds a list of filters"));
    }

    @Test
    void aFieldOrOperatorWrittenAsSomethingOtherThanTextIsReadAsItsText() {
        // The two keys the vocabulary reads as words are taken as the text of whatever was written, so a
        // number where a field belongs is answered by the vocabulary rather than by a cast failure. The
        // spelling then reaches the same refusal any other unknown word would.
        assertAll(
                () -> assertThat(WrittenFilter.of("{\"field\":7,\"op\":\"eq\",\"value\":\"paid\"}"))
                        .isEqualTo(match("7", DataBrowserCriteria.Operator.EQ, "paid")),
                () -> assertThat(reading(() -> WrittenFilter.of("{\"field\":\"status\",\"op\":7,\"value\":\"paid\"}")))
                        .isEqualTo(refused("`7` is not a filter operator; the ones there are: "
                                + "eq, ne, gt, gte, lt, lte, in, exists, contains")));
    }

    @Test
    void aValueIsCarriedAsItWasWrittenRatherThanAsItsText() {
        // The one key that must not be flattened to text. Three of the nine operators refuse a value of
        // the wrong kind, and every one of those checks would start failing on filters that are correct
        // -- a membership test would arrive as one string, a presence test as the word "true".
        assertAll(
                () -> assertThat(WrittenFilter.of("{\"field\":\"total\",\"op\":\"in\",\"value\":[1,2]}"))
                        .isEqualTo(match("total", DataBrowserCriteria.Operator.IN, List.of(1L, 2L))),
                () -> assertThat(WrittenFilter.of("{\"field\":\"note\",\"op\":\"exists\",\"value\":false}"))
                        .isEqualTo(match("note", DataBrowserCriteria.Operator.EXISTS, false)),
                () -> assertThat(WrittenFilter.of("{\"field\":\"total\",\"op\":\"gt\",\"value\":100}"))
                        .isEqualTo(match("total", DataBrowserCriteria.Operator.GT, 100L)));
    }

    /** What one road ended in: the criteria it read, or the code and reason it refused with. */
    private record Outcome(DataBrowserCriteria criteria, String code, Object reason) {
    }

    private static Outcome reading(Supplier<DataBrowserCriteria> road) {
        try {
            return new Outcome(road.get(), null, null);
        } catch (TapstateException refused) {
            return new Outcome(null, refused.code().code(), refused.args().get("reason"));
        }
    }

    private static Outcome read(DataBrowserCriteria criteria) {
        return new Outcome(criteria, null, null);
    }

    private static Outcome refused(String reason) {
        return new Outcome(null, MALFORMED, reason);
    }

    private static Arguments road(
            String name, String written, DataBrowserFindRequest.Filter posted, Outcome expected) {
        return Arguments.of(name, written, posted, expected);
    }

    private static DataBrowserCriteria.Match match(
            String field, DataBrowserCriteria.Operator operator, Object value) {
        return new DataBrowserCriteria.Match(field, operator, value);
    }

    private static DataBrowserFindRequest.Filter term(String field, String op, Object value) {
        return new DataBrowserFindRequest.Filter(field, op, value, null, null);
    }

    private static DataBrowserFindRequest.Filter all(DataBrowserFindRequest.Filter... members) {
        return new DataBrowserFindRequest.Filter(null, null, null, List.of(members), null);
    }

    private static DataBrowserFindRequest.Filter any(DataBrowserFindRequest.Filter... members) {
        return new DataBrowserFindRequest.Filter(null, null, null, null, List.of(members));
    }
}
