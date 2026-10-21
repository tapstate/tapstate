package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationValues;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationArrayPredicateCompilerTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);
    private static final ValidationPredicate.Limits LIMITS = new ValidationPredicate.Limits(ROW, 10000, false);

    @Test void exactExistsExpansionAndGlobalArraySizeAreAdmitted() {
        var input = row(Map.of("items", List.of(Map.of("amount", 9L), Map.of("amount", 11L))));
        assertThat(compile("size(after.items) >= 2 && after.items.exists(item, item.amount > 10)").evaluate(input).matches()).isTrue();
        assertThat(compile("after.items.exists(item, item.amount > 12)").evaluate(input).matches()).isFalse();
        assertThat(compile("size(after.items) == 0").evaluate(row(Map.of("items", List.of()))).matches()).isTrue();
    }

    @Test void nestedReusedAliasesShadowOnlyTheirOwnBodies() {
        var input = row(Map.of("groups", List.of(Map.of("enabled", true, "members", List.of(1L, 2L)))));
        assertThat(compile("after.groups.exists(x, x.members.exists(x, x == 2) && x.enabled)").evaluate(input).matches()).isTrue();
        assertThat(compile("after.groups.exists(x, x.members.exists(x, x == 3) || !x.enabled)").evaluate(input).matches()).isFalse();
    }

    @Test void explicitMemberAliasesCannotEscapeBackToHistoricalEnvelopeData() {
        var input = row(Map.of("items", List.of(Map.of("keep", true)), "keep", false));
        assertThat(compile("after.items.exists(before, before.keep)").evaluate(input).matches()).isTrue();
        assertThat(compile("after.items.exists(after, after.keep)").evaluate(input).matches()).isTrue();
        assertThatThrownBy(() -> compile("after.items.exists(x, before.keep)"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        assertThatThrownBy(() -> compile("after.items.exists(x, x.keep) && x.keep"))
                .isInstanceOf(TapstateException.class);
    }

    @Test void problemPathsRetainActualNestedOrdinalsInsteadOfMacroVariableNames() {
        var predicate = compile("after.groups.exists(group, group.members.exists(member, member.keep))");
        var result = predicate.evaluate(row(Map.of("groups", List.of(Map.of("members", List.of(Map.of()))))));
        assertThat(result.matches()).isNull();
        assertThat(result.problem().path()).containsExactly("groups", 0, "members", 0, "keep");
    }

    @Test void otherMacrosAndReceiverFunctionsDoNotPassAsExists() {
        for (String source : List.of("after.items.all(x, x > 1)", "after.items.exists_one(x, x > 1)",
                "size(after.items.map(x, x)) > 0", "size(after.items.filter(x, x > 1)) > 0",
                "after.items.size() > 0", "after.items.exists(x, x.startsWith('a'))")) {
            assertThatThrownBy(() -> compile(source)).as(source).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        }
    }

    private static ValidationPredicate compile(String source) { return ValidationPredicateCompiler.compile("filter", source, LIMITS); }
    private static ValidationValues.Row row(Map<String, ?> input) { return ValidationValues.ownRow(input, ROW); }
}
