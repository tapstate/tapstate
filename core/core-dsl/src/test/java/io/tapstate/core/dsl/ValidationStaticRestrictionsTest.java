package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValidationStaticRestrictionsTest {
    private static final ValidationStaticRestrictions.Limits LIMITS = ValidationStaticRestrictions.Limits.defaults();

    @Test
    void normalizedMapRulesReportEveryComputedFieldIncludingConstantsWithoutExecutingThem() {
        var report = inspect("""
                transforms:
                  - id: mapped
                    from: src.orders
                    type: map
                    fields: { renamed: $original, dropped: false, literal: 2, constant: '=1', 'literal.dot': '=after.amount + 1' }
                serve: { from: mapped, sync: [{ id: output, source: target }] }
                """);
        assertThat(report.examinedFields()).isEqualTo(5);
        assertThat(report.reasons()).hasSize(2).allSatisfy(reason -> {
            assertThat(reason.kind()).isEqualTo(ValidationStaticRestrictions.Kind.UNSUPPORTED);
            assertThat(reason.rule()).isEqualTo(ValidationStaticRestrictions.Rule.COMPUTED_MAP);
            assertThat(reason.node()).isEqualTo("mapped");
        });
        assertThat(report.reasons()).extracting(ValidationStaticRestrictions.Reason::fieldPath)
                .containsExactly(List.of("fields", "constant"), List.of("fields", "literal.dot"));
    }

    @Test
    void reusedJavaScriptIsLocatedAtItsSelectedCallSiteAndUnrelatedBodyIsNotRequired() {
        var declarations = declarations("""
                transforms:
                  - { id: selected, use: reusable_js, from: src.orders }
                  - { id: unrelated, use: absent, from: src.archive }
                serve: { from: selected, sync: [{ id: output, source: target }] }
                """);
        var report = ValidationStaticRestrictions.inspect(declarations, LIMITS);
        assertThat(report.examinedNodes()).isEqualTo(1);
        assertThat(report.reasons()).singleElement().satisfies(reason -> {
            assertThat(reason.node()).isEqualTo("selected");
            assertThat(reason.rule()).isEqualTo(ValidationStaticRestrictions.Rule.JAVASCRIPT);
        });
    }

    @Test
    void aJoinIsUnsupportedWithoutParsingOrRunningItsSql() {
        var parser = new DslParser();
        var base = (PipelineResource) parser.parse("version: tapstate/v1\nkind: pipeline\nid: pipeline\nsource: src\n"
                + "serve: { from: combined, sync: [{ id: output, source: target }] }\n");
        var aliases = new io.tapstate.core.model.FromClause.Aliases(Map.of(
                "o", io.tapstate.core.model.FromRef.literal("src.orders"),
                "c", io.tapstate.core.model.FromRef.literal("src.archive")));
        var join = io.tapstate.core.model.Step.inline("combined", aliases,
                new io.tapstate.core.model.TransformBody.Join(io.tapstate.core.model.JoinEngine.BUILTIN, "deliberately not SQL"), null);
        var pipeline = new PipelineResource(base.id(), null, base.sources(), List.of(join), null, base.serve(), null, null);
        var report = ValidationStaticRestrictions.inspect(declarations(pipeline), LIMITS);
        assertThat(report.examinedNodes()).isEqualTo(1);
        assertThat(report.reasons()).singleElement().satisfies(reason ->
                assertThat(reason.rule()).isEqualTo(ValidationStaticRestrictions.Rule.JOIN));
    }

    @Test
    void unresolvedDynamicDependenciesRemainVisibleAlongsideKnownRestrictions() {
        var report = inspect("""
                transforms:
                  - { id: selected, use: reusable_js, from: /upstream.*/ }
                serve: { from: selected, sync: [{ id: output, source: target }] }
                """);
        assertThat(report.reasons()).extracting(ValidationStaticRestrictions.Reason::kind)
                .containsExactly(ValidationStaticRestrictions.Kind.UNSUPPORTED, ValidationStaticRestrictions.Kind.RESOLUTION_REQUIRED);
        assertThat(report.reasons().getLast().node()).isEqualTo("selected");
        assertThat(report.reasons().getLast().reference()).contains(io.tapstate.core.model.FromRef.regex("upstream.*"));
    }

    @Test
    void sharedStepsAreReportedOnceAndEmptyRestrictionsDoNotClaimOtherAdmission() {
        var shared = inspect("""
                transforms:
                  - { id: script, use: reusable_js, from: src.orders }
                  - { id: left_branch, from: script, type: filter, expr: 'true' }
                  - { id: right_branch, from: script, type: filter, expr: 'false' }
                serve: { from: [left_branch, right_branch], sync: [{ id: output, source: target }] }
                """);
        assertThat(shared.reasons()).hasSize(1);
        var otherStage = inspect("""
                transforms:
                  - { id: finite_check_pending, from: src.orders, type: filter, expr: 'after.amount + 1 > 0' }
                serve: { from: finite_check_pending, sync: [{ id: output, source: target }] }
                """);
        // The finite-predicate compiler is a required later stage; this report intentionally has no availability flag.
        assertThat(otherStage.reasons()).isEmpty();
        assertThat(otherStage.examinedNodes()).isEqualTo(1);
    }

    @Test
    void reportAndTraversalBudgetsNeverSilentlyTruncateReasons() {
        var declarations = declarations("""
                transforms:
                  - { id: m, from: src.orders, type: map, fields: { first: '=1', second: '=2' } }
                serve: { from: m, sync: [{ id: output, source: target }] }
                """);
        for (var limits : List.of(new ValidationStaticRestrictions.Limits(10, 1, 10, 65536),
                new ValidationStaticRestrictions.Limits(10, 10, 1, 65536),
                new ValidationStaticRestrictions.Limits(10, 10, 10, 300))) {
            assertThatThrownBy(() -> ValidationStaticRestrictions.inspect(declarations, limits))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("validation.value-limit-exceeded");
        }
        var multiple = declarations("""
                transforms:
                  - { id: first_map, from: src.orders, type: map, fields: { a: 1, b: 2 } }
                  - { id: second_map, from: first_map, type: map, fields: { c: 3, d: 4 } }
                serve: { from: second_map, sync: [{ id: output, source: target }] }
                """);
        assertThatThrownBy(() -> ValidationStaticRestrictions.inspect(multiple,
                new ValidationStaticRestrictions.Limits(10, 3, 10, 65536)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("staticRestrictions.fields");
        var report = ValidationStaticRestrictions.inspect(declarations, LIMITS);
        assertThat(report.accountedBytes()).isBetween(256L, LIMITS.bytes());
        assertThatThrownBy(() -> report.reasons().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    private static ValidationStaticRestrictions.Report inspect(String body) {
        return ValidationStaticRestrictions.inspect(declarations(body), LIMITS);
    }
    private static ValidationDeclarations.Inspection declarations(String body) {
        return declarations((PipelineResource) new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: pipeline\nsource: src\n" + body));
    }
    private static ValidationDeclarations.Inspection declarations(PipelineResource pipeline) {
        var parser = new DslParser();
        Resource source = parser.parse("version: tapstate/v1\nkind: source\nid: src\nconnector: mysql\nconfig: {}\nmode: cdc\ntables: [orders, archive]\n");
        Resource target = parser.parse("version: tapstate/v1\nkind: source\nid: target\nconnector: mongodb\nconfig: {}\n");
        Resource script = parser.parse("version: tapstate/v1\nkind: transform\nid: reusable_js\ntype: js\nscript: invalid JavaScript\n");
        return ValidationDeclarations.inspect(pipeline, Map.of("src", source, "target", target, "reusable_js", script),
                new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.SYNC, "output"), ValidationDeclarations.Limits.defaults());
    }
}
