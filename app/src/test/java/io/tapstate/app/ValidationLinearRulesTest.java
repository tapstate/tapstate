package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.ValidationDeclarations;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationRootReader;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ValidationLinearRulesTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 256, 20);
    private static final ValidationLinearRules.Limits LIMITS = new ValidationLinearRules.Limits(
            new ValidationPredicate.Limits(ROW, 2000, false), 16, 65536);
    private static final String CHAIN = """
            transforms:
              - { id: renamed, use: rename_price, from: src.orders }
              - { id: eligible, from: renamed, type: filter, expr: "after.price >= 10 && after.tag == 'checked'" }
              - { id: projected, from: eligible, type: map, fields: { total: $price, obsolete: false } }
            serve: { from: projected, sync: [{ id: output, source: target }] }
            """;

    @Test
    void declaredReusableMapFilterMapUsesTheSourceInOrderAndPreservesOriginalFacts() {
        var rules = compile(CHAIN);
        var source = row(Map.of("id", 7, "amount", 9007199254740993L, "obsolete", "old"));
        var result = rules.evaluate(source, () -> {});
        assertThat(result.disposition()).isEqualTo(ValidationRelationRows.Disposition.INCLUDED);
        assertThat(result.working().fields()).containsExactlyInAnyOrderEntriesOf(
                Map.of("id", 7, "total", 9007199254740993L, "tag", "checked"));
        assertThat(source.fields()).containsOnlyKeys("id", "amount", "obsolete");
        assertThat(result.problem()).isNull();
        assertThat(rules.retainedBytes()).isPositive().isLessThan(LIMITS.programBytes());
        assertThat(rules.executionReservationBytes(source)).isGreaterThan(source.accountedBytes());
    }

    @Test
    void aMissingFieldIsIndeterminateAndLocatedAtTheFilterRatherThanExcluded() {
        var result = compile(CHAIN).evaluate(row(Map.of("id", 7)), () -> {});
        assertThat(result.disposition()).isEqualTo(ValidationRelationRows.Disposition.INDETERMINATE);
        assertThat(result.problem().node()).isEqualTo("eligible");
        assertThat(result.problem().predicate().kind()).isEqualTo(ValidationPredicate.ProblemKind.MISSING_FIELD);
        assertThat(result.problem().predicate().path()).containsExactly("price");
        assertThat(result.working().fields()).containsEntry("tag", "checked").doesNotContainKey("total");
    }

    @Test
    void configuredComputedRulesAreRejectedEvenAfterAnAlwaysFalseFilter() {
        assertThatThrownBy(() -> compile("""
                transforms:
                  - { id: excluded, from: src.orders, type: filter, expr: "false" }
                  - { id: computed, from: excluded, type: map, fields: { total: '=1' } }
                serve: { from: computed, sync: [{ id: output, source: target }] }
                """))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.rule-unsupported")
                .hasMessageContaining("computed");
    }

    @Test
    void unresolvedReferencesAndMultipleRoutesCannotEnterTheSingleRouteInterpreter() {
        for (String from : List.of("/.*/", "[src.orders, src.archive]")) {
            assertThatThrownBy(() -> compile("serve: { from: " + from + ", sync: [{ id: output, source: target }] }\n"))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("linear.single-resolved-route-required");
        }
        assertThatThrownBy(() -> compile("""
                transforms:
                  - { id: script, from: src.orders, type: js, script: "return event;" }
                serve: { from: script, sync: [{ id: output, source: target }] }
                """))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("rule", "js"));
    }

    @Test
    void lossOfAuthorityBetweenOperationsPreventsAResultFromEscaping() {
        var calls = new AtomicInteger();
        var loss = new IllegalStateException("lost test authority");
        assertThatThrownBy(() -> compile(CHAIN).evaluate(row(Map.of("id", 7, "amount", 20)), () -> {
            if (calls.incrementAndGet() == 3) throw loss;
        })).isSameAs(loss);
        assertThat(calls).hasValue(3);
    }

    @Test
    void stepProgramAndPassthroughInputBudgetsAreEnforced() {
        var declarations = declarations(CHAIN);
        assertThatThrownBy(() -> ValidationLinearRules.compile(declarations,
                new ValidationLinearRules.Limits(LIMITS.predicate(), 2, 65536)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.linear.steps");
        assertThatThrownBy(() -> ValidationLinearRules.compile(declarations,
                new ValidationLinearRules.Limits(LIMITS.predicate(), 16, 300)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.linear.programBytes");
        var pass = compile("serve: { from: src.orders, sync: [{ id: output, source: target }] }\n");
        var oversized = ValidationValues.ownRow(Map.of("payload", "x".repeat(8192)), new ValidationValues.Limits(65536, 256, 20));
        assertThatThrownBy(() -> pass.evaluate(oversized, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.linear.input");
    }

    @Test
    void completeRootReadsUseTheCompiledDeclarationAndUnfilteredTargetCardinality() {
        var rules = compile(CHAIN);
        var source = new Cursor(List.of(row(Map.of("id", 7, "amount", 2)), row(Map.of("id", 7, "amount", 20))));
        var target = new Cursor(List.of(row(Map.of("id", 7, "total", 20, "tag", "checked"))));
        var result = read(rules, source, target);
        assertThat(result.observedAgreement()).isTrue();
        assertThat(source.closedRows).isEqualTo(2);
        assertThat(target.closedRows).isEqualTo(1);
        assertThat(source.closed && target.closed).isTrue();

        var duplicate = read(rules, new Cursor(List.of(row(Map.of("id", 7, "amount", 20)))),
                new Cursor(List.of(row(Map.of("id", 7, "total", 20, "tag", "checked")), row(Map.of("id", 7, "total", -1)))));
        assertThat(duplicate.counts()).containsEntry(ValidationRootCheck.Kind.TARGET_DUPLICATE, 1L);
        assertThat(duplicate.observedAgreement()).isFalse();
    }

    @Test
    void sourceExclusionReportsTargetExtraButUnknownParticipationDoesNotReadTheTarget() {
        var rules = compile(CHAIN);
        var extra = read(rules, new Cursor(List.of(row(Map.of("id", 7, "amount", 2)))),
                new Cursor(List.of(row(Map.of("id", 7, "total", 999, "tag", "checked")))));
        assertThat(extra.counts()).containsExactly(Map.entry(ValidationRootCheck.Kind.TARGET_EXTRA, 1L));
        var target = new Cursor(List.of(row(Map.of("id", 7, "total", 999))));
        var unknown = read(rules, new Cursor(List.of(row(Map.of("id", 7)))), target);
        assertThat(unknown.counts()).containsExactly(Map.entry(ValidationRootCheck.Kind.SOURCE_FIELD_MISSING, 1L));
        assertThat(unknown.observedAgreement()).isFalse();
        assertThat(target.opened).isZero();
        var invalidTarget = new Cursor(List.of(row(Map.of("id", 7, "total", 999))));
        var invalid = read(rules, new Cursor(List.of(row(Map.of("id", 7, "amount", "not-a-number")))), invalidTarget);
        assertThat(invalid.counts()).containsExactly(Map.entry(ValidationRootCheck.Kind.SOURCE_FIELD_INVALID, 1L));
        assertThat(invalidTarget.opened).isZero();
    }

    @Test
    void explicitEndpointBindingsCheckActualRenamedValuesWithoutInferringAMapping() {
        var rules = compile("""
                transforms:
                  - { id: first, from: src.orders, type: map, fields: { temporary: $id } }
                  - { id: final_map, from: first, type: map, fields: { final_key: $temporary } }
                serve: { from: final_map, sync: [{ id: output, source: target }] }
                """);
        var observation = observation();
        var identity = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(), binding(List.of("id")), binding(List.of("final_key")));
        var original = row(Map.of("id", 7, "value", "kept"));
        var result = rules.classify(original, identity("src"), observation, identity, () -> {});
        assertThat(result.row().disposition()).isEqualTo(ValidationRelationRows.Disposition.INCLUDED);
        assertThat(result.row().working().fields()).containsEntry("final_key", 7).doesNotContainKey("id");
        assertThat(original.fields()).containsEntry("id", 7).doesNotContainKey("final_key");
        assertThat(rules.executionReservationBytes(original, identity))
                .isGreaterThanOrEqualTo(rules.executionReservationBytes(original) + 2L * ValidationKey.MAX_KEY_BYTES);
    }

    @Test
    void changedOrMissingProjectedKeysFailBeforeTheyCanBeMispaired() {
        for (String assignment : List.of("9", "false")) {
            var rules = compile("transforms: [{ id: key_change, from: src.orders, type: map, fields: { id: " + assignment
                    + " } }]\nserve: { from: key_change, sync: [{ id: output, source: target }] }\n");
            var source = new Cursor(List.of(row(Map.of("id", 7))));
            var target = new Cursor(List.of(row(Map.of("id", 7))));
            assertThatThrownBy(() -> read(rules, source, target)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);
                assertThat(error.args()).containsEntry("node", "output").containsEntry("field", List.of("id"));
                assertThat(error.args().get("rule")).isEqualTo(assignment.equals("9")
                        ? "identity.projected-key-changed" : "identity.projected-key-unavailable");
            });
            assertThat(source.closed).isTrue(); assertThat(target.opened).isZero();
        }
    }

    @Test
    void aSourceKeyOutsideTheOriginalScopeAndAnIdentityFromAnotherRunAreRefused() {
        var rules = compile("serve: { from: src.orders, sync: [{ id: output, source: target }] }\n");
        var observation = observation();
        var identity = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(), binding(List.of("id")), binding(List.of("id")));
        assertThatThrownBy(() -> rules.classify(row(Map.of("id", 8)), identity("src"), observation, identity, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("linear.sourceKeyScope");
        assertThatThrownBy(() -> rules.classify(row(Map.of("id", "7")), identity("src"), observation, identity, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("identity.source-key-unavailable");
        var wrongMapping = new ValidationLinearRules.Identity(observation.run(), "other-mapping", observation.root().outputId(),
                binding(List.of("id")), binding(List.of("id")));
        assertThatThrownBy(() -> rules.classify(row(Map.of("id", 7)), identity("src"), observation, wrongMapping, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("linear.identityRunScope");
        var wrongOutput = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), "other-output",
                binding(List.of("id")), binding(List.of("id")));
        assertThatThrownBy(() -> rules.classify(row(Map.of("id", 7)), identity("src"), observation, wrongOutput, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("linear.identityRunScope");
        var old = observation.run();
        var other = new ValidationRunIdentity(old.owner(), old.taskId(), "another-run", old.mode(), old.specHash(),
                old.keyMappingRevision(), old.comparisonRulesVersion());
        var foreign = new ValidationLinearRules.Identity(other, observation.root().mappingId(), observation.root().outputId(), binding(List.of("id")), binding(List.of("id")));
        assertThatThrownBy(() -> rules.classify(row(Map.of("id", 7)), identity("src"), observation, foreign, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("linear.identityRunScope");
    }

    @Test
    void nestedAndLiteralDottedEndpointPathsKeepTheirDistinctMeaning() {
        var rules = compile("transforms: [{ id: renamed, from: src.orders, type: map, fields: { user: $profile } }]\n"
                + "serve: { from: renamed, sync: [{ id: output, source: target }] }\n");
        var observation = observation();
        var original = row(Map.of("profile", Map.of("id", 7), "user.id", 9));
        var identity = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(), binding(List.of("profile", "id")), binding(List.of("user", "id")));
        var physical = new ValidationPhysicalIdentity("src", "db", null, "orders", List.of(List.of("profile", "id")), key());
        assertThat(rules.classify(original, physical, observation, identity, () -> {}).row().disposition())
                .isEqualTo(ValidationRelationRows.Disposition.INCLUDED);
        var literal = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(), binding(List.of("profile", "id")), binding(List.of("user.id")));
        assertThatThrownBy(() -> rules.classify(original, physical, observation, literal, () -> {}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("identity.projected-key-changed");
    }

    @Test
    void aLaterCompoundComponentCannotChangeOrDisappearBehindAnEqualFirstComponent() {
        var parts = List.of(new ValidationKeyBinding.Component(List.of("first"), ValidationKey.Kind.EXACT_NUMBER, java.util.Set.of("exact-number")),
                new ValidationKeyBinding.Component(List.of("second"), ValidationKey.Kind.EXACT_NUMBER, java.util.Set.of("exact-number")));
        var binding = new ValidationKeyBinding("compound", parts, java.util.Set.of(), ROW);
        var key = new ValidationKey(List.of(ValidationKey.Part.number(7), ValidationKey.Part.number(11)));
        var observation = new ValidationObservation(observation().run(), new ValidationRoot("mapping", "output", key, List.of()), 1);
        var physical = new ValidationPhysicalIdentity("src", "db", null, "orders", List.of(List.of("first"), List.of("second")), key);
        var identity = new ValidationLinearRules.Identity(observation.run(), "mapping", "output", binding, binding);
        for (String assignment : List.of("99", "false")) {
            var rules = compile("transforms: [{ id: change_tail, from: src.orders, type: map, fields: { second: " + assignment
                    + " } }]\nserve: { from: change_tail, sync: [{ id: output, source: target }] }\n");
            assertThatThrownBy(() -> rules.classify(row(Map.of("first", 7, "second", 11)), physical, observation, identity, () -> {}))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("field", List.of("second")));
        }
    }

    private static ValidationKeyBinding binding(List<String> path) {
        return new ValidationKeyBinding("explicit-key", List.of(new ValidationKeyBinding.Component(path,
                ValidationKey.Kind.EXACT_NUMBER, java.util.Set.of("exact-number"))), java.util.Set.of("json"), ROW);
    }

    private static ValidationRootCheck read(ValidationLinearRules rules, Cursor source, Cursor target) {
        var observation = observation();
        return ValidationRootReader.read(observation, ValidationMethod.FULL_COLUMN,
                new ValidationRootReader.Query("source", "complete", source::open),
                new ValidationRootReader.Query("target", "complete", target::open),
                new ValidationRelationRows.Limits(65536, ROW), new ValidationRootCheck.Limits(1048576, 100, 100, 20, 2),
                Clock.fixed(Instant.EPOCH, ZoneOffset.UTC), () -> {},
                original -> rules.classify(original, identity("src"), observation,
                        new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(), binding(List.of("id")), binding(List.of("id"))), () -> {}), row -> new ValidationRootReader.Target(row, identity("target")), (pair, root) ->
                    ValidationFieldCheck.check(observation, List.of(), pair.source().working(), pair.target().working(),
                            pair.sourceSample(), pair.targetSample(), Optional.empty(), String::equals,
                            new ValidationValueComparison.TraversalLimits(ROW, 10000), root));
    }
    private static ValidationLinearRules compile(String body) { return ValidationLinearRules.compile(declarations(body), LIMITS); }
    private static ValidationDeclarations.Inspection declarations(String body) {
        PipelineResource pipeline = (PipelineResource) parse("kind: pipeline\nid: pipeline\nsource: src\n" + body);
        Resource source = parse("kind: source\nid: src\nconnector: mysql\nmode: cdc\nconfig: {}\ntables: [orders, archive]\n");
        Resource target = parse("kind: source\nid: target\nconnector: mongodb\nconfig: {}\n");
        Resource rename = parse("kind: transform\nid: rename_price\ntype: map\nfields: { price: $amount, tag: checked }\n");
        return ValidationDeclarations.inspect(pipeline, Map.of("src", source, "target", target, "rename_price", rename),
                new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.SYNC, "output"), ValidationDeclarations.Limits.defaults());
    }
    private static Resource parse(String body) { return new DslParser().parse("version: tapstate/v1\n" + body); }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7))); }
    private static ValidationObservation observation() {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(), List.of()), 1);
    }
    private static ValidationPhysicalIdentity identity(String source) {
        return new ValidationPhysicalIdentity(source, "db", null, "orders", List.of(List.of("id")), key());
    }
    private static final class Cursor implements ValidationRowCursor {
        private final List<ValidationValues.Row> rows;
        private int index;
        private int closedRows;
        private int opened;
        private boolean closed;
        private Cursor(List<ValidationValues.Row> rows) { this.rows = rows; }
        private ValidationRowCursor open() { opened++; return this; }
        @Override public Optional<Row> next() {
            if (index == rows.size()) return Optional.empty();
            int ordinal = index++;
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return rows.get(ordinal); }
                @Override public void close() { closedRows++; }
            });
        }
        @Override public void close() { closed = true; }
    }
}
