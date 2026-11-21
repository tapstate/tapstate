package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.control.core.DataBrowserCriteria.All;
import io.tapstate.control.core.DataBrowserCriteria.Any;
import io.tapstate.control.core.DataBrowserCriteria.Match;
import io.tapstate.control.core.DataBrowserCriteria.Operator;
import io.tapstate.core.common.TapstateException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The read face's filter vocabulary: which requests it accepts, and how it refuses the ones whose value
 * does not fit the operator that was asked for. Those are user input rather than caller mistakes — they
 * arrive over a wire format that cannot type them — so each is a coded refusal, not a crash.
 */
class DataBrowserCriteriaTest {

    @Test
    void refusesAMembershipTermWhoseValueIsNotASet() {
        // Left through, this reaches the store as a set-membership test against a single string, which
        // matches nothing - an empty read that looks exactly like a collection holding no such rows.
        assertThatThrownBy(() -> new Match("status", Operator.IN, "Paid"))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("TS0210001");
    }

    @Test
    void refusesAMembershipTermWithAnEmptySet() {
        // A membership test against nothing matches nothing, whatever the collection holds. It is never
        // what a caller meant, and its answer is indistinguishable from a correct read that found none.
        assertThatThrownBy(() -> new Match("status", Operator.IN, List.of()))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("TS0210001");
    }

    @Test
    void refusesAPresenceTermWhoseValueIsNotAYesOrNo() {
        assertThatThrownBy(() -> new Match("note", Operator.EXISTS, "true"))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("TS0210001");
    }

    @Test
    void refusesASubstringTermWhoseValueIsNotText() {
        assertThatThrownBy(() -> new Match("carrier", Operator.CONTAINS, 42))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("TS0210001");
    }

    @Test
    void namesTheOperatorItRefusedSoTheCallerKnowsWhichTermToFix() {
        // A refusal that says only "malformed" leaves a caller with several terms to guess between.
        assertThatThrownBy(() -> new Match("status", Operator.IN, "Paid"))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).args().get("reason"))
                .asString()
                .contains("in")
                .contains("status");
    }

    @Test
    void acceptsEveryOperatorGivenAValueThatFitsIt() {
        assertThatCode(() -> {
            new Match("f", Operator.EQ, "a");
            new Match("f", Operator.NE, 1);
            new Match("f", Operator.GT, 1);
            new Match("f", Operator.GTE, 1);
            new Match("f", Operator.LT, 1);
            new Match("f", Operator.LTE, 1);
            new Match("f", Operator.IN, List.of("a", "b"));
            new Match("f", Operator.EXISTS, false);
            new Match("f", Operator.CONTAINS, "a");
        }).doesNotThrowAnyException();
    }

    @Test
    void refusesACombinationWithNoTermsInIt() {
        // An empty combination reads as every row, which absent criteria already say. Two spellings of
        // one request is how a face comes to have two answers for it.
        assertThatThrownBy(() -> new All(List.of()))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("TS0210001");
    }

    @Test
    void boundsHowDeepAnExpressionCanNestByShape() {
        // The bound is held by the types rather than by a check: a conjunction takes conjuncts (a term or
        // one alternative), and an alternative takes terms only — so there is no fourth level to write.
        // Asserted because it is the property the vocabulary was narrowed for, and a later widening of
        // either signature would silently undo it.
        assertThat(All.class.getRecordComponents()[0].getGenericType().getTypeName())
                .isEqualTo("java.util.List<io.tapstate.control.core.DataBrowserCriteria$Conjunct>");
        assertThat(Any.class.getRecordComponents()[0].getGenericType().getTypeName())
                .isEqualTo("java.util.List<io.tapstate.control.core.DataBrowserCriteria$Match>");
    }
}
