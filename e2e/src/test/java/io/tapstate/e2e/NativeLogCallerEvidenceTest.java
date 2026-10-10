package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Complete caller metadata and the original logical limits survive shared phase symbols. */
class NativeLogCallerEvidenceTest {
    private static final NativeLogCallerEvidence.Compatibility SHARED =
            NativeLogCallerEvidence.Compatibility.SHARED_ONLY;
    private static final NativeLogCallerEvidence.Compatibility LEGACY =
            NativeLogCallerEvidence.Compatibility.ALLOW_LEGACY_RECORD_LOCAL;

    @Test
    void everyColumnAndAbsentValueRoundTripsWithoutRepeatingSymbols() {
        var phase = new NativeLogCallerEvidence.Phase("owned");
        List<Object> original = callers("example.Source");
        var first = phase.retain(log(1, original));
        var second = phase.retain(log(2, original));
        var snapshot = phase.snapshot();

        assertThat(snapshot.records()).hasSize(2);
        assertThat(snapshot.table().strings()).containsExactlyElementsOf((List<String>) original.getFirst());
        assertThat(NativeLogCallerEvidence.decode(first.get("callers"), snapshot.table(), SHARED))
                .isEqualTo(original);
        assertThat(NativeLogCallerEvidence.decode(second.get("callers"), snapshot.table(), SHARED))
                .isEqualTo(original);
        assertThat(snapshot.logicalBytes()).isEqualTo(NativeLogCallerEvidence.bytes(snapshot.evidence()));
    }

    @Test
    void exportedJsonRestoresNumericColumnsAndAllReferences() {
        var phase = new NativeLogCallerEvidence.Phase("json");
        phase.retain(log(1, callers("example.Source")));
        Map<?, ?> exported = (Map<?, ?>) JsonReader.parse(JsonWriter.write(phase.snapshot().evidence()));
        var table = NativeLogCallerEvidence.Table.read(exported.get("callerSymbols"));
        Map<?, ?> record = (Map<?, ?>) ((List<?>) exported.get("records")).getFirst();

        assertThat(NativeLogCallerEvidence.decode(record.get("callers"), table, SHARED))
                .isEqualTo(callers("example.Source"));
    }

    @Test
    void snapshotsStayImmutableWhenThePhaseGrowsAndResets() {
        var phase = new NativeLogCallerEvidence.Phase("epochs");
        var first = phase.retain(log(1, callers("first.Source")));
        var frozen = phase.snapshot();
        phase.retain(log(2, callers("second.Source")));

        assertThat(frozen.records()).hasSize(1);
        assertThat(frozen.table().strings()).doesNotContain("second.Source");
        assertThatThrownBy(() -> frozen.table().strings().add("changed"))
                .isInstanceOf(UnsupportedOperationException.class);
        phase.reset();
        phase.retain(log(3, callers("third.Source")));
        var next = phase.snapshot();
        assertThat(next.table().id()).isNotEqualTo(frozen.table().id());
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(first.get("callers"), next.table(), SHARED))
                .isInstanceOf(AssertionError.class).hasMessageContaining("FOREIGN_TABLE");
        assertThat(NativeLogCallerEvidence.decode(first.get("callers"), frozen.table(), SHARED))
                .isEqualTo(callers("first.Source"));
    }

    @Test
    void missingForeignAndMalformedReferencesCannotUseLegacyFallback() {
        var phase = new NativeLogCallerEvidence.Phase("owner");
        var record = phase.retain(log(1, callers("example.Source")));
        var table = phase.snapshot().table();
        Object encoded = record.get("callers");
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(encoded, null, LEGACY))
                .isInstanceOf(AssertionError.class).hasMessageContaining("MISSING_OR_FOREIGN_TABLE");
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(encoded,
                new NativeLogCallerEvidence.Table("other#1", table.strings()), LEGACY))
                .isInstanceOf(AssertionError.class).hasMessageContaining("FOREIGN_TABLE");
        Map<String, Object> bad = new LinkedHashMap<>((Map<String, Object>) encoded);
        bad.put("format", "unknown");
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(bad, table, LEGACY))
                .isInstanceOf(AssertionError.class).hasMessageContaining("REFERENCE_SHAPE");
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(callers("legacy.Source"), null, SHARED))
                .isInstanceOf(AssertionError.class).hasMessageContaining("LEGACY_NOT_SELECTED");
    }

    @Test
    void fractionalOutOfRangeAndIncompleteColumnsAreRejected() {
        var phase = new NativeLogCallerEvidence.Phase("columns");
        Object encoded = phase.retain(log(1, callers("example.Source"))).get("callers");
        var table = phase.snapshot().table();
        Map<String, Object> reference = (Map<String, Object>) encoded;
        List<Object> original = (List<Object>) reference.get("rows");
        for (Object invalid : List.of(0.5, Long.MAX_VALUE, -2L)) {
            List<Object> rows = new ArrayList<>(original); rows.set(0, invalid);
            Map<String, Object> bad = new LinkedHashMap<>(reference); bad.put("rows", rows);
            assertThatThrownBy(() -> NativeLogCallerEvidence.decode(bad, table, SHARED))
                    .isInstanceOf(AssertionError.class);
        }
        List<Object> shortRows = new ArrayList<>(original); shortRows.removeLast();
        Map<String, Object> bad = new LinkedHashMap<>(reference); bad.put("rows", shortRows);
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(bad, table, SHARED))
                .isInstanceOf(AssertionError.class).hasMessageContaining("FRAME_BOUND_OR_SHAPE");
    }

    @Test
    void oversizedLegacyAndTablesFailBeforeTheyCanBecomeSharedEvidence() {
        List<Object> large = callers("x".repeat(20_000));
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(large, null, LEGACY))
                .isInstanceOf(AssertionError.class).hasMessageContaining("CALLER_BYTE_BUDGET");
        assertThatThrownBy(() -> new NativeLogCallerEvidence.Table("large#1", List.of("x".repeat(600_000))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("TABLE_BYTE_BUDGET");
        assertThatThrownBy(() -> NativeLogCallerEvidence.Table.read(Map.of("format", NativeLogCallerEvidence.FORMAT,
                "id", "large#1", "strings", List.of("x".repeat(600_000)))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("TABLE_BYTE_BUDGET");
    }

    @Test
    void recordAndPhaseRejectionLeaveEverySymbolIndexAndRecordUnchanged() {
        var phase = new NativeLogCallerEvidence.Phase("atomic");
        phase.retain(log(1, callers("first.Source")));
        var before = phase.snapshot();
        Map<String, Object> tooLarge = new LinkedHashMap<>(log(2, callers("new.Source")));
        tooLarge.put("message", "x".repeat(20_000));
        assertThatThrownBy(() -> phase.retain(tooLarge)).isInstanceOf(AssertionError.class);
        assertThat(phase.snapshot()).isEqualTo(before);

        Map<String, Object> padding = Map.of("target", "JOB", "message", "x".repeat(15_000));
        while (phase.snapshot().logicalBytes() < 2_040_000) { phase.retain(padding); }
        before = phase.snapshot();
        Map<String, Object> overflowing = new LinkedHashMap<>(log(3, callers("never.Committed")));
        overflowing.put("message", "x".repeat(15_000));
        assertThatThrownBy(() -> phase.retain(overflowing))
                .isInstanceOf(AssertionError.class).hasMessageContaining("PHASE_BYTE_BUDGET");
        assertThat(phase.snapshot()).isEqualTo(before);
        assertThat(before.table().strings()).doesNotContain("never.Committed");
    }

    @Test
    void countLimitAndMissingLogCallerCannotCommitPartialEvidence() {
        var phase = new NativeLogCallerEvidence.Phase("count");
        for (int index = 0; index < NativeLogCallerEvidence.MAX_RECORDS; index++) {
            phase.retain(Map.of("target", "JOB", "index", index));
        }
        var before = phase.snapshot();
        assertThatThrownBy(() -> phase.retain(log(1, callers("never.Committed"))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("RECORD_COUNT_BUDGET");
        assertThat(phase.snapshot()).isEqualTo(before);
        var empty = new NativeLogCallerEvidence.Phase("missing");
        assertThatThrownBy(() -> empty.retain(Map.of("target", "LOG")))
                .isInstanceOf(AssertionError.class).hasMessageContaining("MISSING_LOG_CALLERS");
        assertThat(empty.snapshot().records()).isEmpty();
    }

    @Test
    void openCallsAndCalibrationShareTheSameWholePhaseBudget() {
        var phase = new NativeLogCallerEvidence.Phase("whole");
        phase.retain(log(1, callers("completed.Source")));
        var open = Map.<String, Object>of("entry", log(2, callers("open.Source")), "invocation", 2L);
        var calibration = log(3, callers("calibrated.Source"));
        var snapshot = phase.snapshot(List.of(open), calibration);
        assertThat(snapshot.table().strings()).contains("open.Source", "calibrated.Source");
        assertThat(snapshot.unpairedCalls()).hasSize(1);
        assertThat(snapshot.logicalBytes()).isEqualTo(NativeLogCallerEvidence.bytes(snapshot.evidence()));
        var before = phase.snapshot();
        List<Map<String, Object>> many = java.util.Collections.nCopies(100,
                Map.of("target", "JOB", "message", "x".repeat(8_000)));
        assertThatThrownBy(() -> phase.snapshot(many, Map.of()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("PHASE_BYTE_BUDGET");
        assertThat(phase.snapshot()).isEqualTo(before);
        assertThat(before.table().strings()).doesNotContain("open.Source", "calibrated.Source");
    }

    @Test
    void resolverExportsOnlyReferencedTablesOnceAcrossImmutablePhases() {
        var phase = new NativeLogCallerEvidence.Phase("subset");
        var first = phase.retain(log(1, callers("first.Source")));
        var firstBoundary = phase.snapshot();
        phase.reset();
        var second = phase.retain(log(2, callers("second.Source")));
        var secondBoundary = phase.snapshot();
        var resolver = new NativeLogCallerEvidence.Resolver(LEGACY);
        resolver.register(firstBoundary.table()); resolver.register(firstBoundary.table());
        resolver.register(secondBoundary.table());

        var subset = resolver.evidence(List.of(first, first, second));
        assertThat(subset.get("callerTables"))
                .isEqualTo(List.of(firstBoundary.table().evidence(), secondBoundary.table().evidence()));
        assertThat(subset.get("records")).isEqualTo(List.of(first, first, second));
        assertThat(resolver.evidence(List.of(second)).get("callerTables"))
                .isEqualTo(List.of(secondBoundary.table().evidence()));
        assertThat((List<?>) resolver.evidence(List.of()).get("callerTables")).isEmpty();
        assertThat(resolver.decode(first.get("callers"))).isEqualTo(callers("first.Source"));
        assertThat(resolver.decode(second.get("callers"))).isEqualTo(callers("second.Source"));
    }

    @Test
    void resolverFailsClosedOnMissingConflictingAndMalformedSharedTables() {
        var phase = new NativeLogCallerEvidence.Phase("strict");
        var record = phase.retain(log(1, callers("exact.Source")));
        var table = phase.snapshot().table();
        var resolver = new NativeLogCallerEvidence.Resolver(LEGACY);
        assertThatThrownBy(() -> resolver.decode(record.get("callers")))
                .isInstanceOf(AssertionError.class).hasMessageContaining("MISSING_OR_FOREIGN_TABLE");
        assertThatThrownBy(() -> resolver.evidence(List.of(record)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("MISSING_OR_FOREIGN_TABLE");
        resolver.register(table);
        assertThatThrownBy(() -> resolver.register(new NativeLogCallerEvidence.Table(table.id(), List.of("foreign"))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("CONFLICTING_TABLE");
        assertThat(resolver.decode(record.get("callers"))).isEqualTo(callers("exact.Source"));
        Map<String, Object> malformed = new LinkedHashMap<>((Map<String, Object>) record.get("callers"));
        malformed.put("format", "unknown");
        assertThatThrownBy(() -> resolver.decode(malformed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("REFERENCE_SHAPE");
        assertThat(resolver.decode(callers("legacy.Source"))).isEqualTo(callers("legacy.Source"));
        assertThatThrownBy(() -> new NativeLogCallerEvidence.Resolver(SHARED).decode(callers("legacy.Source")))
                .isInstanceOf(AssertionError.class).hasMessageContaining("LEGACY_NOT_SELECTED");
    }

    @Test
    void prospectiveOpenAndCalibrationRejectionCannotMutateTheCompletedPhase() {
        var phase = new NativeLogCallerEvidence.Phase("joint-atomic");
        phase.retain(log(1, callers("completed.Source")));
        Map<String, Object> padding = Map.of("target", "JOB", "message", "x".repeat(15_000));
        while (phase.snapshot().logicalBytes() < 2_040_000) { phase.retain(padding); }
        var before = phase.snapshot();
        Map<String, Object> largeOpen = new LinkedHashMap<>(log(3, callers("uncommitted.Open")));
        largeOpen.put("message", "x".repeat(15_000));
        var candidate = log(2, callers("uncommitted.Return"));
        var calibration = log(4, callers("uncommitted.Calibration"));
        assertThatThrownBy(() -> phase.retain(candidate, List.of(Map.of("entry", largeOpen)), calibration))
                .isInstanceOf(AssertionError.class).hasMessageContaining("PHASE_BYTE_BUDGET");
        assertThat(phase.snapshot()).isEqualTo(before);
        assertThat(before.table().strings()).doesNotContain("uncommitted.Open", "uncommitted.Return", "uncommitted.Calibration");
    }

    @Test
    void aRawOpenCallerSpanningResetIsReencodedWithoutLosingEitherBoundary() {
        var phase = new NativeLogCallerEvidence.Phase("spanning");
        var entry = log(9, callers("spanning.Source"));
        var before = phase.snapshot(List.of(Map.of("entry", entry, "invocation", 9L)), Map.of());
        phase.reset();
        var returned = phase.retain(entry);
        var after = phase.snapshot();
        assertThat(before.table().id()).isNotEqualTo(after.table().id());
        Map<?, ?> oldEntry = (Map<?, ?>) before.unpairedCalls().getFirst().get("entry");
        assertThat(NativeLogCallerEvidence.decode(oldEntry.get("callers"), before.table(), SHARED))
                .isEqualTo(callers("spanning.Source"));
        assertThat(NativeLogCallerEvidence.decode(returned.get("callers"), after.table(), SHARED))
                .isEqualTo(callers("spanning.Source"));
        assertThatThrownBy(() -> NativeLogCallerEvidence.decode(returned.get("callers"), before.table(), SHARED))
                .isInstanceOf(AssertionError.class).hasMessageContaining("FOREIGN_TABLE");
    }

    private static Map<String, Object> log(long invocation, List<Object> callers) {
        return Map.of("target", "LOG", "invocation", invocation, "callers", callers,
                "scope", Map.of("incarnation", "real-resource", "generation", 7L));
    }

    private static List<Object> callers(String source) {
        return List.of(List.of(source, "read", "()V", "OwnedLoader", "EXACT_ARTIFACT_METHOD", "code-sha", "BOOTSTRAP"),
                List.of(0, 1, 2, 19L, 42L, 3, -1, 4, 5,
                        0, 1, 2, -1L, -1L, 6, -1, 4, -1));
    }
}
