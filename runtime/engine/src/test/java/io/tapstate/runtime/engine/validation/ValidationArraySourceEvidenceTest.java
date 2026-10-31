package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationArraySourceEvidenceTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final ValidationObservation ROOT = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
            "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(7), List.of()), 1);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

    @Test void sourceDuplicatesSurviveMissingOrDuplicateTargetRootsAndAreNotCountedTwiceForPresentRoots() {
        var source = source(List.of(member(11, 1), member(12, 1)));
        for (int targets : List.of(0, 1, 2)) {
            var content = new AtomicInteger();
            var result = read(new Cursor(1), targets, ValidationRootReader.SourceEvidence.fromArraySource(ROOT, source, 1024 * 1024), 5,
                    (pair, root) -> {
                        content.incrementAndGet();
                        try (var frame = ValidationNestArrayMembers.forTarget(source)) {
                            frame.add(ValidationNestArrayMembers.Side.TARGET, ROOT, member(11, 1));
                            frame.add(ValidationNestArrayMembers.Side.TARGET, ROOT, member(12, 1));
                            frame.complete(ValidationNestArrayMembers.Side.TARGET, ROOT, pair.targetRead());
                            ValidationNestArrayCheck.check(frame.finish(), root, group -> fail("Duplicated membership"));
                        }
                    }, new AtomicInteger());
            assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_DUPLICATE, 1L,
                    targets == 0 ? Kind.TARGET_MISSING : Kind.TARGET_DUPLICATE, 1L));
            assertThat(content).hasValue(targets == 1 ? 1 : 0);
            assertThat(result.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.SOURCE_DUPLICATE)
                    .singleElement().satisfies(finding -> assertThat(finding.location().scope().ancestors())
                            .containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(1))));
            assertThat(result.reads()).extracting(read -> read.boundary().readId()).contains("child", "root-source", "root-target");
        }
    }

    @Test void sourceRowCollisionsAndDroppedDiagnosticSamplesStillRetainEachBlockedScope() {
        var source = source(List.of(member(11, 1), member(11, 2)));
        var result = read(new Cursor(1), 0, ValidationRootReader.SourceEvidence.fromArraySource(ROOT, source, 1024 * 1024), 0,
                (pair, root) -> fail("Target is absent"), new AtomicInteger());
        assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_ROW_DUPLICATE, 2L, Kind.TARGET_MISSING, 1L));
        assertThat(result.samples()).isEmpty(); assertThat(result.samplesTruncated()).isTrue();
        assertThat(result.notCompared()).hasSize(4);
        assertThat(result.notCompared()).extracting(blocked -> blocked.location().scope().ancestors().getLast().key()).contains(key(1), key(2));
    }

    @Test void uncertainMembershipCausesAndLowerBoundsCanBeImportedThenCheckedWithoutDuplication() {
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(ROOT.root(), List.of("items", "required")));
        ValidationNestArrayMembers.UncertainSource uncertain;
        try (var frame = new ValidationNestArrayMembers(ROOT, List.of("items"), memberLimits())) {
            frame.add(ValidationNestArrayMembers.Side.SOURCE, ROOT, member(11, 1));
            frame.add(ValidationNestArrayMembers.Side.SOURCE, ROOT, member(12, 1));
            frame.complete(ValidationNestArrayMembers.Side.SOURCE, ROOT, boundary("child"));
            uncertain = frame.finishUncertainSource(List.of(cause));
        }
        var result = read(new Cursor(1), 1, ValidationRootReader.SourceEvidence.fromUncertainArraySource(ROOT, uncertain, 1024 * 1024), 5,
                (pair, root) -> ValidationNestArrayCheck.checkUncertainSource(uncertain, root), new AtomicInteger());
        assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_MISSING, 1L, Kind.SOURCE_DUPLICATE, 1L));
        assertThat(result.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.SOURCE_DUPLICATE).singleElement()
                .satisfies(finding -> assertThat(finding.multiplicity()).contains(new Multiplicity(2, false)));
        assertThat(result.notCompared()).anySatisfy(blocked -> assertThat(blocked.location().fieldPath()).containsExactly("items"));
    }

    @Test void parentQueryFailureAfterArrayFactsStillProducesNoRootOrTargetRead() {
        var source = source(List.of(member(11, 1), member(12, 1)));
        var cursor = new Cursor(1);
        cursor.failureAtEof = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "roots"), null);
        var targets = new AtomicInteger();
        assertThat(catchThrowable(() -> read(cursor, 1, ValidationRootReader.SourceEvidence.fromArraySource(ROOT, source, 1024 * 1024), 0,
                (pair, root) -> fail("No complete parent"), targets))).isSameAs(cursor.failureAtEof);
        assertThat(targets).hasValue(0); assertThat(cursor.released).isEqualTo(1); assertThat(cursor.closed).isTrue();
    }

    @Test void foreignObservationAndEvidenceCapacityFailBeforeAnyTargetRead() {
        var source = source(List.of(member(11, 1), member(12, 1)));
        var newer = new ValidationObservation(ROOT.run(), ROOT.root(), 2);
        assertThatThrownBy(() -> ValidationRootReader.SourceEvidence.fromArraySource(newer, source, 1024 * 1024)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> ValidationRootReader.SourceEvidence.fromArraySource(ROOT, source, 1))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        try (var many = new ValidationNestArrayMembers(ROOT, List.of("items"), new ValidationNestArrayMembers.Limits(8 * 1024 * 1024, 300, 600, ROW))) {
            for (int i = 0; i < 257; i++) {
                many.add(ValidationNestArrayMembers.Side.SOURCE, ROOT, member(2 * i, i));
                many.add(ValidationNestArrayMembers.Side.SOURCE, ROOT, member(2 * i + 1, i));
            }
            many.complete(ValidationNestArrayMembers.Side.SOURCE, ROOT, boundary("child"));
            var facts = many.finishSource();
            assertThatThrownBy(() -> ValidationRootReader.SourceEvidence.fromArraySource(ROOT, facts, 8 * 1024 * 1024))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("limit", "rootRead.arrayEvidenceIssues"));
        }
    }

    private static ValidationRootCheck read(Cursor source, int targets, ValidationRootReader.SourceEvidence evidence, int samples,
            ValidationRootReader.ContentCheck content, AtomicInteger targetOpens) {
        return ValidationRootReader.read(ROOT, ValidationMethod.FULL_COLUMN,
                new ValidationRootReader.Query("root-source", "v1", () -> source),
                new ValidationRootReader.Query("root-target", "v1", () -> { targetOpens.incrementAndGet(); return new Cursor(targets); }),
                new ValidationRelationRows.Limits(65536, ROW), new ValidationRootCheck.Limits(2 * 1024 * 1024, 100, 100, 20, samples), CLOCK, () -> {},
                raw -> new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED, raw,
                        Optional.of(identity("source", 7))), List.of(), evidence),
                raw -> new ValidationRootReader.Target(raw, identity("target", 7)), content);
    }
    private static ValidationNestArrayMembers.Source source(List<ValidationNestArrayMembers.Member> rows) {
        try (var members = new ValidationNestArrayMembers(ROOT, List.of("items"), memberLimits())) {
            for (var row : rows) members.add(ValidationNestArrayMembers.Side.SOURCE, ROOT, row);
            members.complete(ValidationNestArrayMembers.Side.SOURCE, ROOT, boundary("child"));
            return members.finishSource();
        }
    }
    private static ValidationNestArrayMembers.Limits memberLimits() { return new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW); }
    private static ValidationNestArrayMembers.Member member(long id, long element) {
        return new ValidationNestArrayMembers.Member(key(id), key(element), identity("source", id),
                ValidationValues.ownRow(Map.of("id", id, "element", element), ROW));
    }
    private static ValidationPhysicalIdentity identity(String source, long id) {
        return new ValidationPhysicalIdentity(source, "db", null, "rows", List.of(List.of("id")), key(id));
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "v1", Instant.EPOCH, Instant.EPOCH); }
    private static final class Cursor implements ValidationRowCursor {
        final int count; int delivered; int released; boolean closed; RuntimeException failureAtEof;
        Cursor(int count) { this.count = count; }
        @Override public Optional<Row> next() {
            if (closed || delivered != released) throw new IllegalStateException("Unreleased row");
            if (delivered == count) { if (failureAtEof != null) throw failureAtEof; return Optional.empty(); }
            int ordinal = delivered++;
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return ValidationValues.ownRow(Map.of("id", 7), ROW); }
                @Override public void close() { released++; }
            });
        }
        @Override public void close() { closed = true; }
    }
}
