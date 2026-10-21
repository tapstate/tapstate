package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static io.tapstate.core.validation.ValidationRelationRows.Disposition.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationRootReaderTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 150, 20);
    private static final ValidationRelationRows.Limits READ = new ValidationRelationRows.Limits(65536, ROW);
    private static final Limits RESULT = new Limits(1024 * 1024, 100, 100, 20, 2);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

    @Test void completeMapThenFilterClassificationUsesEveryRawRowBeforeChoosingAUniquePair() {
        var source = new Cursor(List.of(row(Map.of("id", 7, "amount", 2)), row(Map.of("id", 7, "amount", 9))));
        var target = new Cursor(List.of(row(Map.of("id", 7, "price", 9))));
        var checks = new AtomicInteger();
        var checked = read(source, target, mappedFilter(), (pair, root) -> {
            checks.incrementAndGet();
            assertThat(source.closed && target.closed).isTrue();
            assertThat(pair.source().original().fields()).containsEntry("amount", 9).doesNotContainKey("price");
            assertThat(pair.source().working().fields()).containsEntry("price", 9).doesNotContainKey("amount");
            compare(pair, root);
        });
        assertThat(checked.observedAgreement()).isTrue();
        assertThat(checked.counts()).doesNotContainKey(Kind.SOURCE_DUPLICATE);
        assertThat(source.rowsClosed).isEqualTo(2);
        assertThat(checks).hasValue(1);
        assertThat(checked.reads()).hasSize(2);
    }

    @Test void absenceAndExclusionUseWholeRootPresenceFindingsAndNeverCallContent() {
        for (boolean excluded : List.of(false, true)) {
            var source = new Cursor(excluded ? List.of(row(Map.of("id", 7, "amount", 2))) : List.of());
            var target = new Cursor(List.of(row(Map.of("id", 7, "price", 9))));
            var result = read(source, target, mappedFilter(), (pair, root) -> fail("unexpected content"));
            assertThat(result.participation()).isEqualTo(ValidationCheckEvent.Participation.SHOULD_NOT_EXIST);
            assertThat(result.counts()).containsExactly(entry(Kind.TARGET_EXTRA, 1L));
            assertThat(result.findings().keySet()).allMatch(finding -> finding.location().wholeRoot());
            assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.NOT_APPLICABLE).containsEntry(Check.VALUES, Status.NOT_APPLICABLE);
        }
        var missing = read(new Cursor(List.of(row(Map.of("id", 7)))), new Cursor(List.of()), ValidationRootReaderTest::included,
                (pair, root) -> fail("unexpected content"));
        assertThat(missing.counts()).containsExactly(entry(Kind.TARGET_MISSING, 1L));
        assertThat(read(new Cursor(List.of()), new Cursor(List.of()), ValidationRootReaderTest::included,
                (pair, root) -> fail("unexpected content")).observedAgreement()).isTrue();
    }

    @Test void participatingDuplicatesNeverChooseAWinnerEvenWhenRowsAreEqual() {
        var source = new Cursor(Collections.nCopies(20000, row(Map.of("id", 7))));
        var target = new Cursor(List.of(row(Map.of("id", 7))));
        var result = read(source, target, ValidationRootReaderTest::included, (pair, root) -> fail("ambiguous content"));
        assertThat(source.rowsClosed).isEqualTo(20000);
        assertThat(target.opened).isZero();
        assertThat(result.targetPresence()).isEqualTo(TargetPresence.NOT_READ);
        assertThat(result.counts()).containsExactly(entry(Kind.SOURCE_DUPLICATE, 1L));
        assertThat(result.findings().keySet()).singleElement().satisfies(finding -> assertThat(finding.multiplicity())
                .contains(new Multiplicity(20000, true)));
        assertThat(result.checks().values()).containsOnly(Status.BLOCKED);
    }

    @Test void unknownParticipationKeepsOriginalSamplesCountsAndScopeInsteadOfReportingTargetExtra() {
        var source = new Cursor(Collections.nCopies(50, row(Map.of("id", 7))));
        var target = new Cursor(List.of(row(Map.of("id", 7))));
        var result = read(source, target, mappedFilter(), (pair, root) -> fail("unknown content"));
        assertThat(target.opened).isZero();
        assertThat(source.rowsClosed).isEqualTo(50);
        assertThat(result.counts()).containsExactly(entry(Kind.SOURCE_FIELD_MISSING, 50L));
        assertThat(result.samples()).hasSize(2);
        assertThat(result.samplesTruncated()).isTrue();
        assertThat(result.samples()).allSatisfy(sample -> {
            assertThat(sample.source().orElseThrow().row().fields()).containsExactly(entry("id", 7));
            assertThat(sample.source().orElseThrow().readId()).isEqualTo("source");
        });
        assertThat(result.reads()).singleElement().satisfies(read -> assertThat(read.side()).isEqualTo(Side.SOURCE));
        assertThat(result.notCompared()).extracting(Blocked::check).containsExactlyInAnyOrder(Check.PARTICIPATION, Check.STRUCTURE, Check.VALUES);
    }

    @Test void knownRepeatedParticipantsRemainALowerBoundWhenAnotherRowsParticipationIsUnknown() {
        var source = new Cursor(List.of(row(Map.of("id", 7, "amount", 9)), row(Map.of("id", 7)), row(Map.of("id", 7, "amount", 10))));
        var result = read(source, new Cursor(List.of()), mappedFilter(), (pair, root) -> fail("ambiguous content"));
        assertThat(result.counts()).containsEntry(Kind.SOURCE_DUPLICATE, 1L).containsEntry(Kind.SOURCE_FIELD_MISSING, 1L);
        assertThat(result.findings().keySet()).anySatisfy(finding -> assertThat(finding.multiplicity()).contains(new Multiplicity(2, false)));
    }

    @Test void everyTargetRowCountsAndDuplicatesNeverReachThePayloadComparator() {
        for (boolean exists : List.of(false, true)) {
            var source = new Cursor(exists ? List.of(row(Map.of("id", 7))) : List.of());
            var target = new Cursor(Collections.nCopies(100, row(Map.of("id", 7))));
            var result = read(source, target, ValidationRootReaderTest::included, (pair, root) -> fail("duplicate content"));
            assertThat(target.rowsClosed).isEqualTo(100);
            assertThat(result.counts()).containsEntry(Kind.TARGET_DUPLICATE, 1L);
            assertThat(result.counts().containsKey(Kind.TARGET_EXTRA)).isEqualTo(!exists);
            assertThat(result.notApplicableScopes()).hasSize(exists ? 2 : 0);
            assertThat(result.observedAgreement()).isFalse();
        }
    }

    @Test void fullFieldContentFindsExtraBusinessFieldsAndKeepsPhysicalReadFacts() {
        var source = new Cursor(List.of(row(Map.of("id", 7, "amount", 9))));
        var target = new Cursor(List.of(row(Map.of("id", 7, "price", 10, "extra", true))));
        var result = read(source, target, mappedFilter(), ValidationRootReaderTest::compare);
        assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.TARGET_VALUE, 1L, Kind.TARGET_EXTRA, 1L));
        assertThat(result.samples().getFirst().source().orElseThrow().row().fields()).containsKey("amount").doesNotContainKey("price");
        assertThat(result.samples().getFirst().target().orElseThrow().row().fields()).containsKey("extra");
    }

    @Test void uniqueKeyModeNeverCreatesValueChecksAndStillRejectsDuplicateTargets() {
        var source = new Cursor(List.of(row(Map.of("id", 7, "amount", 1))));
        var target = new Cursor(List.of(row(Map.of("id", 7, "amount", 9))));
        var result = read(source, target, ValidationRootReaderTest::included, (pair, root) -> {}, ValidationMethod.UNIQUE_KEY, RESULT, () -> {});
        assertThat(result.checks()).containsEntry(Check.VALUES, Status.NOT_APPLICABLE);
        assertThat(result.observedAgreement()).isTrue();
    }

    @Test void aLateReadOrCleanupFailureCannotReturnAPartialRootOrOpenTheTarget() {
        for (boolean cleanup : List.of(false, true)) {
            var source = new Cursor(List.of(row(Map.of("id", 7))));
            var failed = failure(ValidationError.READ_FAILED);
            if (cleanup) source.cleanup = failed; else source.endFailure = failed;
            var target = new Cursor(List.of());
            assertThat(catchThrowable(() -> read(source, target, mappedFilter(), (pair, root) -> fail("content")))).isSameAs(failed);
            assertThat(source.closed).isTrue();
            assertThat(target.opened).isZero();
        }
    }

    @Test void lostAuthorityAfterEitherReadOrTheComparatorDiscardsTheRoot() {
        for (int phase = 0; phase < 3; phase++) {
            var authorized = new AtomicBoolean(true);
            var source = new Cursor(List.of(row(Map.of("id", 7))));
            var target = new Cursor(List.of(row(Map.of("id", 7))));
            if (phase == 0) source.afterClose = () -> authorized.set(false);
            if (phase == 1) target.afterClose = () -> authorized.set(false);
            int selected = phase;
            var lost = failure(ValidationError.OWNER_NOT_AUTHORIZED);
            assertThat(catchThrowable(() -> read(source, target, ValidationRootReaderTest::included,
                    (pair, root) -> { if (selected == 2) authorized.set(false); }, ValidationMethod.FULL_COLUMN, RESULT,
                    () -> { if (!authorized.get()) throw lost; }))).isSameAs(lost);
            assertThat(source.closed).isTrue();
            assertThat(target.opened).isEqualTo(phase == 0 ? 0 : 1);
        }
    }

    @Test void executionFailuresAreNotReclassifiedAndFatalCleanupStillDominates() {
        var source = new Cursor(List.of(row(Map.of("id", 7))));
        var fatal = new AssertionError("fatal cleanup");
        source.cleanup = fatal;
        var original = failure(ValidationError.RULE_UNSUPPORTED);
        var target = new Cursor(List.of());
        assertThat(catchThrowable(() -> read(source, target, row -> { throw original; }, (pair, root) -> {}))).isSameAs(fatal);
        assertThat(List.of(fatal.getSuppressed())).contains(original);
        assertThat(target.opened).isZero();
    }

    @Test void sourceFindingBudgetFailureAndCallbackErrorsCannotYieldAResult() {
        var source = new Cursor(List.of(row(Map.of("id", 7, "f", "a")), row(Map.of("id", 7, "f", "b"))));
        var target = new Cursor(List.of());
        assertThatThrownBy(() -> read(source, target, row -> uncertain(row, new Finding(Kind.SOURCE_FIELD_MISSING,
                new Location(observation().root(), List.of(row.fields().get("f"))))), (pair, root) -> {}, ValidationMethod.FULL_COLUMN,
                new Limits(1024 * 1024, 1, 100, 20, 0), () -> {})).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThat(source.closed).isTrue();
        assertThat(target.opened).isZero();
        var fatal = new AssertionError("comparison failure");
        assertThat(catchThrowable(() -> read(new Cursor(List.of(row(Map.of("id", 7)))), new Cursor(List.of(row(Map.of("id", 7)))),
                ValidationRootReaderTest::included, (pair, root) -> { throw fatal; }))).isSameAs(fatal);
    }

    @Test void uncertaintyRequiresKnownSourceCausesAndNoKnownDispositionCanCarryThem() {
        var value = row(Map.of("id", 7));
        assertThatThrownBy(() -> new ValidationRootReader.Source(evaluation(value, INDETERMINATE), List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationRootReader.Source(evaluation(value, INCLUDED), List.of(new Finding(Kind.SOURCE_FIELD_INVALID,
                new Location(observation().root(), List.of("x")))))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> uncertain(value, new Finding(Kind.TARGET_EXTRA, new Location(observation().root(), List.of()))))
                .isInstanceOf(TapstateException.class);
    }

    @Test void completedChildIssuesSurviveDecisiveParentPredicatesAndKeepTheirOwnRawSamples() {
        for (boolean participates : List.of(false, true)) {
            var source = new Cursor(List.of(row(Map.of("id", 7))));
            var child = new Cursor(List.of(row(Map.of("id", 7, "raw_child", "kept"))));
            var target = new Cursor(List.of(row(Map.of("id", 7, "items", List.of(Map.of("unproved", true))))));
            var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation().root(), List.of("items", "eligible")));
            var array = new Location(observation().root(), List.of("items"));
            ValidationPredicate.Expr uncertain = new ValidationPredicate.Present(List.of("items"));
            var predicate = ValidationPredicate.compile("parent", participates
                    ? new ValidationPredicate.Or(new ValidationPredicate.Constant(true), uncertain)
                    : new ValidationPredicate.And(new ValidationPredicate.Constant(false), uncertain), new ValidationPredicate.Limits(ROW, 1000, false));
            var contents = new AtomicInteger();
            var result = read(source, target, original -> {
                var rows = ValidationRelationReader.read(observation(), "child", "complete", child.open(), READ, CLOCK, () -> {},
                        raw -> new ValidationRelationReader.Evaluation(INDETERMINATE, raw, Optional.of(identity("child"))));
                var sample = new SampledSide(identity("child"), rows.indeterminateSamples().getFirst().original(), "child");
                var evidence = new ValidationRootReader.SourceEvidence(observation(), List.of(rows.read()),
                        List.of(new ValidationRootReader.SourceIssue(cause, Optional.of(sample), List.of(array))));
                var evaluated = predicate.evaluate(original, List.of(List.of("items")));
                assertThat(evaluated.problem()).isNull();
                return new ValidationRootReader.Source(evaluation(original, evaluated.matches() ? INCLUDED : EXCLUDED), List.of(), evidence);
            }, (pair, root) -> {
                contents.incrementAndGet();
                assertThat(pair.sourceEvidence().issues()).singleElement().satisfies(issue -> assertThat(issue.affected()).containsExactly(array));
                var masks = pair.sourceEvidence().issues().stream().flatMap(issue -> issue.affected().stream()).map(Location::fieldPath).toList();
                var checked = ValidationFieldCheck.checkPartial(observation(), List.of(), pair.source().working(), pair.target().working(),
                        pair.sourceSample(), pair.targetSample(), Optional.empty(), String::equals,
                        new ValidationValueComparison.TraversalLimits(ROW, 10000), masks, root);
                assertThat(checked.differences()).isZero();
            });
            assertThat(child.closed).isTrue(); assertThat(child.rowsClosed).isEqualTo(1);
            assertThat(contents).hasValue(participates ? 1 : 0);
            assertThat(result.participation()).isEqualTo(participates ? ValidationCheckEvent.Participation.SHOULD_EXIST
                    : ValidationCheckEvent.Participation.SHOULD_NOT_EXIST);
            assertThat(result.counts()).containsEntry(Kind.SOURCE_FIELD_MISSING, 1L);
            assertThat(result.counts().containsKey(Kind.TARGET_EXTRA)).isEqualTo(!participates);
            assertThat(result.checks()).containsEntry(Check.PARTICIPATION, Status.COMPLETE).containsEntry(Check.STRUCTURE, Status.BLOCKED);
            assertThat(result.samples()).anySatisfy(sample -> assertThat(sample.source().orElseThrow().row().fields()).containsEntry("raw_child", "kept"));
            assertThat(result.observedAgreement()).isFalse();
        }
    }

    @Test void aRegisteredChildCauseIsNotCountedAgainWhenItAlsoMakesParticipationUnknown() {
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation().root(), List.of("items", "eligible")));
        var target = new Cursor(List.of(row(Map.of("id", 7))));
        var result = read(new Cursor(List.of(row(Map.of("id", 7)))), target, original -> new ValidationRootReader.Source(
                evaluation(original, INDETERMINATE), List.of(cause), new ValidationRootReader.SourceEvidence(observation(),
                        List.of(new ValidationCheckEvent.ReadBoundary("child", "complete", Instant.EPOCH, Instant.EPOCH)),
                        List.of(new ValidationRootReader.SourceIssue(cause, Optional.of(new SampledSide(identity("child"), row(Map.of("raw", true)), "child")),
                                List.of(new Location(observation().root(), List.of("items"))))))), (pair, root) -> fail("unknown root"));
        assertThat(result.counts()).containsExactly(entry(Kind.SOURCE_FIELD_MISSING, 1L));
        assertThat(target.opened).isZero();
        assertThat(result.samples()).singleElement().satisfies(sample -> assertThat(sample.source().orElseThrow().readId()).isEqualTo("child"));
    }

    @Test void aMissingChildReadCannotBindEvenWhenTheDiagnosticSampleBudgetIsZero() {
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation().root(), List.of("items")));
        var target = new Cursor(List.of());
        var evidence = new ValidationRootReader.SourceEvidence(observation(), List.of(), List.of(new ValidationRootReader.SourceIssue(cause,
                Optional.of(new SampledSide(identity("child"), row(Map.of("raw", true)), "unfinished-child")), List.of(cause.location()))));
        assertThatThrownBy(() -> read(new Cursor(List.of(row(Map.of("id", 7)))), target,
                original -> new ValidationRootReader.Source(evaluation(original, EXCLUDED), List.of(), evidence), (pair, root) -> {},
                ValidationMethod.FULL_COLUMN, new Limits(1024 * 1024, 100, 100, 20, 0), () -> {})).isInstanceOf(TapstateException.class);
        // Target reading may have completed, but incomplete source evidence can never produce a root result.
    }

    @Test void lateOuterReadFailureDiscardsAlreadyCompletedChildEvidence() {
        var failed = failure(ValidationError.READ_FAILED);
        var source = new Cursor(List.of(row(Map.of("id", 7)))); source.endFailure = failed;
        var target = new Cursor(List.of());
        var cause = new Finding(Kind.SOURCE_FIELD_INVALID, new Location(observation().root(), List.of("items")));
        assertThat(catchThrowable(() -> read(source, target, original -> new ValidationRootReader.Source(evaluation(original, INCLUDED), List.of(),
                new ValidationRootReader.SourceEvidence(observation(), List.of(new ValidationCheckEvent.ReadBoundary("child", "complete", Instant.EPOCH, Instant.EPOCH)),
                        List.of(new ValidationRootReader.SourceIssue(cause, Optional.empty(), List.of(cause.location()))))),
                (pair, root) -> fail("failed query")))).isSameAs(failed);
        assertThat(source.closed).isTrue(); assertThat(target.opened).isZero();
    }

    @Test void childEvidenceFromAnotherObservationCannotEnterTheCurrentRoot() {
        var original = observation();
        var stale = new ValidationObservation(original.run(), original.root(), original.revision() + 1);
        var target = new Cursor(List.of());
        var evidence = new ValidationRootReader.SourceEvidence(stale,
                List.of(new ValidationCheckEvent.ReadBoundary("child", "complete", Instant.EPOCH, Instant.EPOCH)), List.of());
        assertThatThrownBy(() -> read(new Cursor(List.of(row(Map.of("id", 7)))), target,
                raw -> new ValidationRootReader.Source(evaluation(raw, INCLUDED), List.of(), evidence), (pair, root) -> {}))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
        assertThat(target.opened).isZero();
    }

    @Test void contentEvidenceCannotRetainDiagnosticRowsDroppedByTheReportSampleCap() {
        var cause = new Finding(Kind.SOURCE_FIELD_INVALID, new Location(observation().root(), List.of("items")));
        var largeChild = ValidationValues.ownRow(Map.of("payload", "x".repeat(65536)), new ValidationValues.Limits(256 * 1024, 100, 10));
        var evidence = new ValidationRootReader.SourceEvidence(observation(),
                List.of(new ValidationCheckEvent.ReadBoundary("child", "complete", Instant.EPOCH, Instant.EPOCH)),
                List.of(new ValidationRootReader.SourceIssue(cause, Optional.of(new SampledSide(identity("child"), largeChild, "child")),
                        List.of(cause.location()))));
        var result = read(new Cursor(List.of(row(Map.of("id", 7)))), new Cursor(List.of(row(Map.of("id", 7)))),
                raw -> new ValidationRootReader.Source(evaluation(raw, INCLUDED), List.of(), evidence), (pair, root) -> {
                    assertThat(pair.sourceEvidence().issues()).singleElement().satisfies(issue -> assertThat(issue.sample()).isEmpty());
                }, ValidationMethod.FULL_COLUMN, new Limits(65536, 100, 100, 20, 0), () -> {});
        assertThat(result.samples()).isEmpty(); assertThat(result.samplesTruncated()).isTrue();
        assertThat(result.counts()).containsExactly(entry(Kind.SOURCE_FIELD_INVALID, 1L));
    }

    @Test void anExplicitlyDroppedUnknownContributionKeepsItsDiagnosisWithoutHidingTargetResidue() {
        var cause = new Finding(Kind.SOURCE_FIELD_INVALID, new Location(observation().root(), List.of("items")));
        var map = ValidationStaticMap.compile("drop", new TransformBody.MapProjection(Map.of("items", FieldRule.drop())), ROW);
        var evidence = new ValidationRootReader.SourceEvidence(observation(),
                List.of(new ValidationCheckEvent.ReadBoundary("child", "complete", Instant.EPOCH, Instant.EPOCH)),
                List.of(new ValidationRootReader.SourceIssue(cause, Optional.empty(), List.of())));
        var result = read(new Cursor(List.of(row(Map.of("id", 7)))),
                new Cursor(List.of(row(Map.of("id", 7, "items", List.of(Map.of("old", true)))))), original -> {
                    var projected = map.apply(original, List.of(List.of("items")));
                    assertThat(projected.unavailablePaths()).isEmpty();
                    return new ValidationRootReader.Source(evaluation(projected.row(), INCLUDED), List.of(), evidence);
                }, (pair, root) -> {
                    assertThat(pair.sourceEvidence().issues()).singleElement().satisfies(issue -> assertThat(issue.affected()).isEmpty());
                    compare(pair, root);
                });
        assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_INVALID, 1L, Kind.TARGET_EXTRA, 1L));
        assertThat(result.notCompared()).isEmpty();
        assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.COMPLETE).containsEntry(Check.VALUES, Status.COMPLETE);
        assertThat(result.observedAgreement()).isFalse();
    }

    private static Function<ValidationValues.Row, ValidationRootReader.Source> mappedFilter() {
        var map = ValidationStaticMap.compile("map", new TransformBody.MapProjection(Map.of("price", FieldRule.rename("amount"))), ROW);
        var predicate = ValidationPredicate.compile("filter", new ValidationPredicate.Compare(ValidationPredicate.Operator.GT,
                new ValidationPredicate.Field(List.of("price")), new ValidationPredicate.Constant(5)), new ValidationPredicate.Limits(ROW, 1000, true));
        return original -> {
            var working = map.apply(original);
            var result = predicate.evaluate(working);
            if (result.problem() != null) return uncertain(working, new Finding(result.problem().kind() == ValidationPredicate.ProblemKind.MISSING_FIELD
                    ? Kind.SOURCE_FIELD_MISSING : Kind.SOURCE_FIELD_INVALID, new Location(observation().root(), result.problem().path())));
            return new ValidationRootReader.Source(evaluation(working, result.matches() ? INCLUDED : EXCLUDED), List.of());
        };
    }
    private static ValidationRootReader.Source included(ValidationValues.Row row) { return new ValidationRootReader.Source(evaluation(row, INCLUDED), List.of()); }
    private static ValidationRootReader.Source uncertain(ValidationValues.Row row, Finding cause) { return new ValidationRootReader.Source(evaluation(row, INDETERMINATE), List.of(cause)); }
    private static ValidationRelationReader.Evaluation evaluation(ValidationValues.Row row, ValidationRelationRows.Disposition disposition) {
        return new ValidationRelationReader.Evaluation(disposition, row, Optional.of(identity("source")));
    }
    private static void compare(ValidationRootReader.Pair pair, Builder root) {
        ValidationFieldCheck.check(observation(), List.of(), pair.source().working(), pair.target().working(), pair.sourceSample(), pair.targetSample(),
                Optional.empty(), String::equals, new ValidationValueComparison.TraversalLimits(ROW, 10000), root);
    }
    private static ValidationRootCheck read(Cursor source, Cursor target, Function<ValidationValues.Row, ValidationRootReader.Source> rule, ValidationRootReader.ContentCheck content) {
        return read(source, target, rule, content, ValidationMethod.FULL_COLUMN, RESULT, () -> {});
    }
    private static ValidationRootCheck read(Cursor source, Cursor target, Function<ValidationValues.Row, ValidationRootReader.Source> rule,
            ValidationRootReader.ContentCheck content, ValidationMethod method, Limits limits, Runnable authority) {
        return ValidationRootReader.read(observation(), method, new ValidationRootReader.Query("source", "complete", source::open),
                new ValidationRootReader.Query("target", "complete", target::open), READ, limits, CLOCK, authority, rule,
                row -> new ValidationRootReader.Target(row, identity("target")), content);
    }
    private static ValidationValues.Row row(Map<String, ?> values) { return ValidationValues.ownRow(values, ROW); }
    private static ValidationObservation observation() {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(), List.of()), 1);
    }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7))); }
    private static ValidationPhysicalIdentity identity(String source) { return new ValidationPhysicalIdentity(source, "db", null, "table", List.of(List.of("id")), key()); }
    private static TapstateException failure(ValidationError code) { return new TapstateException(code, Map.of(), null); }
    private static void raise(Throwable error) { if (error instanceof Error fatal) throw fatal; if (error instanceof RuntimeException failure) throw failure; }

    private static final class Cursor implements ValidationRowCursor {
        private final List<ValidationValues.Row> rows;
        private int index;
        private int rowsClosed;
        private int opened;
        private boolean closed;
        private Throwable endFailure;
        private Throwable cleanup;
        private Runnable afterClose = () -> {};
        private Cursor(List<ValidationValues.Row> rows) { this.rows = rows; }
        private ValidationRowCursor open() { opened++; return this; }
        @Override public Optional<Row> next() {
            if (index == rows.size()) { raise(endFailure); return Optional.empty(); }
            int ordinal = index++;
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return rows.get(ordinal); }
                @Override public void close() { rowsClosed++; }
            });
        }
        @Override public void close() { closed = true; afterClose.run(); raise(cleanup); }
    }
}
