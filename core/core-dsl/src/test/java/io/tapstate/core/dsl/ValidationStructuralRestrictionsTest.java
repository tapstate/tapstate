package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.validation.ValidationMethod;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValidationStructuralRestrictionsTest {
    private static final ValidationStructuralRestrictions.Limits LIMITS = ValidationStructuralRestrictions.Limits.defaults();

    @Test
    void oneOrdinalExpansionHasNoStructuralRefusalAndADotDoesNotInventAnotherLayer() {
        var report = inspect("""
                transforms:
                  - { id: expanded, from: src.orders, type: unwind, path: 'literal.items', include_array_index: ordinal }
                serve: { from: expanded, sync: [{ id: output, source: target }] }
                """, ValidationMethod.FULL_COLUMN);
        assertThat(report.reasons()).isEmpty();
    }

    @Test
    void unsupportedOptionsAndAColumnSelfCollisionAreLocatedSeparately() {
        var options = inspect("""
                transforms:
                  - { id: expanded, from: src.orders, type: unwind, path: items, element_key: id, preserve_null_and_empty_arrays: true }
                serve: { from: expanded, sync: [{ id: output, source: target }] }
                """, ValidationMethod.UNIQUE_KEY);
        assertThat(options.reasons()).extracting(ValidationStructuralRestrictions.Reason::rule)
                .containsExactly(ValidationStructuralRestrictions.Rule.ELEMENT_KEY,
                        ValidationStructuralRestrictions.Rule.PRESERVE_EMPTY, ValidationStructuralRestrictions.Rule.ORDINAL_REQUIRED);
        var conflict = inspect("""
                transforms:
                  - { id: expanded, from: src.orders, type: unwind, path: items, include_array_index: items }
                serve: { from: expanded, sync: [{ id: output, source: target }] }
                """, ValidationMethod.FULL_COLUMN);
        assertThat(conflict.reasons()).singleElement().satisfies(reason ->
                assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.ORDINAL_COLUMN_CONFLICT));
    }

    @Test
    void multipleUnwindsAreDetectedOnOnePathWithBothNodeIdentities() {
        var report = inspect("""
                transforms:
                  - { id: first, from: src.orders, type: unwind, path: items, include_array_index: first_ordinal }
                  - { id: second, from: first, type: unwind, path: children, include_array_index: second_ordinal }
                serve: { from: second, sync: [{ id: output, source: target }] }
                """, ValidationMethod.FULL_COLUMN);
        assertThat(report.reasons()).singleElement().satisfies(reason -> {
            assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.MULTIPLE_UNWINDS);
            assertThat(reason.node()).isEqualTo("second");
            assertThat(reason.relatedNode()).contains("first");
        });
    }

    @Test
    void independentUnwindsBeforeANestAreNotMistakenForASerialExpansion() {
        var report = inspect("""
                transforms:
                  - { id: left_expand, from: src.orders, type: unwind, path: items, include_array_index: ordinal }
                  - { id: right_expand, from: src.lines, type: unwind, path: parts, include_array_index: ordinal }
                  - id: assembled
                    from: { o: left_expand, c: right_expand }
                    type: nest
                    root:
                      from: o
                      key: [id]
                      embed:
                        - { from: c, on: { order_id: id }, as: array, path: children, arrayKey: [id] }
                serve: { from: assembled, sync: [{ id: output, source: target }] }
                """, ValidationMethod.FULL_COLUMN);
        assertThat(report.reasons()).isEmpty();
        // Logical keys, complete Nest rules and model/native admission are separate obligations.
        assertThat(report.visitedSteps()).isPositive();
    }

    @Test
    void anExpansionAfterNestIsRejectedEvenWhenItIsTheOnlyUnwind() {
        var report = inspect("""
                transforms:
                  - { id: assembled, from: { o: src.orders }, type: nest, root: { from: o, key: [id] } }
                  - { id: expanded, from: assembled, type: unwind, path: children, include_array_index: ordinal }
                serve: { from: expanded, sync: [{ id: output, source: target }] }
                """, ValidationMethod.UNIQUE_KEY);
        assertThat(report.reasons()).singleElement().satisfies(reason -> {
            assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.UNWIND_AFTER_NEST);
            assertThat(reason.node()).isEqualTo("expanded");
            assertThat(reason.relatedNode()).contains("assembled");
        });
    }

    @Test
    void unionIsRefusedForKeyAndColumnChecksButCountNeedsItsOwnRootRule() {
        String body = """
                transforms:
                  - { id: combined, from: [src.orders, src.lines], type: union }
                serve: { from: combined, sync: [{ id: output, source: target }] }
                """;
        for (var method : List.of(ValidationMethod.UNIQUE_KEY, ValidationMethod.FULL_COLUMN)) {
            assertThat(inspect(body, method).reasons()).singleElement().satisfies(reason -> {
                assertThat(reason.kind()).isEqualTo(ValidationStructuralRestrictions.Kind.UNSUPPORTED);
                assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.UNION);
            });
        }
        assertThat(inspect(body, ValidationMethod.COUNT).reasons()).singleElement().satisfies(reason -> {
            assertThat(reason.kind()).isEqualTo(ValidationStructuralRestrictions.Kind.ROOT_COUNT_RULE_REQUIRED);
            assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.COUNT_ROOT_RULE);
        });
    }

    @Test
    void countDoesNotRejectChildOnlyExpansionByTheKeyCheckMatrixOrAssumeItPreservesRoots() {
        var report = inspect("""
                transforms:
                  - { id: child_expand, from: src.lines, type: unwind, path: parts, element_key: id, preserve_null_and_empty_arrays: true }
                  - id: assembled
                    from: { o: src.orders, c: child_expand }
                    type: nest
                    root:
                      from: o
                      key: [id]
                      embed:
                        - { from: c, on: { order_id: id }, as: array, path: children, arrayKey: [id] }
                serve: { from: assembled, sync: [{ id: output, source: target }] }
                """, ValidationMethod.COUNT);
        assertThat(report.reasons()).singleElement().satisfies(reason -> {
            assertThat(reason.kind()).isEqualTo(ValidationStructuralRestrictions.Kind.ROOT_COUNT_RULE_REQUIRED);
            assertThat(reason.node()).isEqualTo("child_expand");
        });
    }

    @Test
    void knownDownstreamCombinationsAreStillCheckedWhenTheUpstreamReferenceIsPending() {
        var declarations = declarations("""
                transforms:
                  - { id: first, from: /src.*/, type: unwind, path: items, include_array_index: x }
                  - { id: second, from: first, type: unwind, path: parts, include_array_index: y }
                serve: { from: second, sync: [{ id: output, source: target }] }
                """);
        assertThat(declarations.pending()).hasSize(1);
        assertThat(ValidationStructuralRestrictions.inspect(declarations, ValidationMethod.FULL_COLUMN, LIMITS).reasons())
                .singleElement().satisfies(reason -> assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.MULTIPLE_UNWINDS));
    }

    @Test
    void aSharedInvalidCombinationConsumesOneReasonSlotAcrossSeveralRoutes() {
        var declarations = declarations("""
                transforms:
                  - { id: first, from: src.orders, type: unwind, path: items, include_array_index: x }
                  - { id: second, from: first, type: unwind, path: parts, include_array_index: y }
                  - { id: left_branch, from: second, type: filter, expr: 'true' }
                  - { id: right_branch, from: second, type: filter, expr: 'false' }
                serve: { from: [left_branch, right_branch], sync: [{ id: output, source: target }] }
                """);
        var report = ValidationStructuralRestrictions.inspect(declarations, ValidationMethod.FULL_COLUMN,
                new ValidationStructuralRestrictions.Limits(10, 100, 1, 65536));
        assertThat(report.reasons()).singleElement().satisfies(reason -> {
            assertThat(reason.node()).isEqualTo("second");
            assertThat(reason.relatedNode()).contains("first");
            assertThat(reason.rule()).isEqualTo(ValidationStructuralRestrictions.Rule.MULTIPLE_UNWINDS);
        });
    }

    @Test
    void selectedScopeAndBoundedVisitsPreventUnrelatedRefusalsAndUnlimitedWork() {
        var declarations = declarations("""
                transforms:
                  - { id: unrelated, from: src.orders, type: union }
                  - { id: selected, from: src.orders, type: unwind, path: items, include_array_index: ordinal }
                serve: { from: selected, sync: [{ id: output, source: target }] }
                """);
        assertThat(ValidationStructuralRestrictions.inspect(declarations, ValidationMethod.FULL_COLUMN, LIMITS).reasons()).isEmpty();
        assertThatThrownBy(() -> ValidationStructuralRestrictions.inspect(declarations, ValidationMethod.FULL_COLUMN,
                new ValidationStructuralRestrictions.Limits(10, 1, 10, 65536)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("structuralRestrictions.visits");
        assertThatThrownBy(() -> ValidationStructuralRestrictions.inspect(declarations, ValidationMethod.FULL_COLUMN,
                new ValidationStructuralRestrictions.Limits(10, 100, 10, 1)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("structuralRestrictions.bytes");
    }

    private static ValidationStructuralRestrictions.Report inspect(String body, ValidationMethod method) {
        return ValidationStructuralRestrictions.inspect(declarations(body), method, LIMITS);
    }
    private static ValidationDeclarations.Inspection declarations(String body) {
        var parser = new DslParser();
        var pipeline = (PipelineResource) parser.parse("version: tapstate/v1\nkind: pipeline\nid: pipeline\nsource: src\n" + body);
        Resource source = parser.parse("version: tapstate/v1\nkind: source\nid: src\nconnector: mysql\nconfig: {}\nmode: cdc\ntables: [orders, lines]\n");
        Resource target = parser.parse("version: tapstate/v1\nkind: source\nid: target\nconnector: mongodb\nconfig: {}\n");
        return ValidationDeclarations.inspect(pipeline, Map.of("src", source, "target", target),
                new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.SYNC, "output"), ValidationDeclarations.Limits.defaults());
    }
}
