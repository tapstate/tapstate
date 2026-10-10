package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnExpectationsTest {
    @Test
    void everyFrozenMeasuredPlanHasCompleteImmutableSourceAssociations() {
        for (var workload : BenchmarkWorkloadDefinitions.all()) {
            for (var phase : workload.phases()) {
                if (!phase.measured()) continue;
                Fixture fixture = fixture(workload, phase, 233);
                var rows = associate(fixture);
                assertThat(rows).hasSize((int) phase.expectedLogicalOutputChanges());
                assertThat(rows).allSatisfy(row -> {
                    assertThat(row.target()).isIn(phase.targets());
                    assertThat(row.sourceBatchIndex()).isBetween(0, phase.batches().size() - 1);
                    assertThat(row.sourceIssuedAtNanos()).isEqualTo(fixture.batches().get(row.sourceBatchIndex()).issuedAtNanos());
                    assertThat(row.fixedCohort()).isTrue();
                });
                assertThatThrownBy(rows::clear).isInstanceOf(UnsupportedOperationException.class);
            }
        }
    }

    @Test
    void statelessTupleOrderComesFromTheDeclaredFieldNames() {
        Fixture fixture = fixture("stateless", false, "cdc-update", 233);
        assertThat(fixture.calls().getFirst().keyFields()).containsExactly("item_index", "id");
        assertThat(fixture.calls().getFirst().rows().getFirst().keys()).containsExactly(0, 2);
        var rows = associate(fixture);
        assertThat(rows.getFirst().key()).isEqualTo("2:0");
        assertThat(rows.get(1).key()).isEqualTo("2:1");
        assertThat(rows.getLast().key()).isEqualTo("12000:1");
        assertThat(rows.getLast().sourceBatchIndex()).isEqualTo(119);
    }

    @Test
    void oneNativeCallMaySpanSeveralSourceBatchesWithoutMixingClockDomains() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var rows = associate(fixture);
        assertThat(rows.subList(0, 233)).extracting(BenchmarkWriteReturnExpectations.Association::sourceBatchIndex)
                .containsOnly(0, 1, 2);
        assertThat(rows.subList(0, 233)).extracting(BenchmarkWriteReturnExpectations.Association::callSequence)
                .containsOnly(1L);
        assertThat(rows.get(99).sourceBatchIndex()).isZero();
        assertThat(rows.get(100).sourceBatchIndex()).isEqualTo(1);
        assertThat(rows.getFirst().sourceIssuedAtNanos()).isPositive();
        assertThat(rows.getFirst().callObservedNanos()).isNegative();
    }

    @Test
    void inputInsertShapeIsRetainedWhileThePhysicalOracleStillExpectsUpdate() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        assertThat(BenchmarkExpectedChanges.forPhase(fixture.workload(), fixture.phase()).getFirst()
                .forBatch(0).getFirst().kind()).isEqualTo(BenchmarkMongoDeliveryObserver.Kind.UPDATE);
        assertThat(associate(fixture)).extracting(BenchmarkWriteReturnExpectations.Association::inputTapKind)
                .containsOnly(1);
        assertThat(associate(fixture).getFirst().callLastCallbackExitNanos())
                .isEqualTo(fixture.calls().getFirst().lastCallbackExitNanos());
    }

    @Test
    void anUnorderedCallbackExitCannotBeAssociatedAsAnAcknowledgedMeasuredRow() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var call = fixture.calls().getFirst();
        var calls = new ArrayList<>(fixture.calls());
        calls.set(0, new BenchmarkWriteReturnAssembly.FullCall(call.sequence(), call.writer(), call.totalRows(),
                call.beganNanos(), call.observedNanos() + 1, call.observedNanos(), true, 1,
                call.inserted(), call.modified(), call.removed(), call.errors(), call.errorDetails(), call.failureType(),
                call.scope(), call.writerIdentity(), call.stream(), call.target(), call.keyFields(), call.rows()));
        reject(fixture, fixture.batches(), calls, "callback clock order");
    }

    @Test
    void completeStatefulPilotKeepsAllRowsAndMarksOnlyTheOriginalFixedCohort() {
        Fixture fixture = fixture("stateful", true, "cdc-update", 1024);
        var rows = associate(fixture);
        assertThat(rows).hasSize(192_000);
        assertThat(rows.stream().filter(BenchmarkWriteReturnExpectations.Association::fixedCohort).count())
                .isEqualTo(96_000);
        for (String key : List.of("24000", "72001")) {
            assertThat(rows.stream().filter(row -> row.key().equals(key)).toList())
                    .hasSize(2).allSatisfy(row -> assertThat(row.fixedCohort()).isFalse());
        }
        for (String key : List.of("24001", "72000")) {
            assertThat(rows.stream().filter(row -> row.key().equals(key)).toList())
                    .hasSize(2).allSatisfy(row -> assertThat(row.fixedCohort()).isTrue());
        }
    }

    @Test
    void duplicateAndMissingRowsCannotProduceCompleteAssociations() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var duplicate = new ArrayList<>(fixture.calls());
        var call = duplicate.getFirst();
        var rows = new ArrayList<>(call.rows());
        rows.set(1, rows.getFirst());
        duplicate.set(0, changed(call, call.sequence(), call.writerIdentity(), call.target(), call.keyFields(), rows));
        reject(fixture, fixture.batches(), duplicate, "target row is duplicate");

        var missing = new ArrayList<>(fixture.calls());
        var last = missing.getLast();
        missing.set(missing.size() - 1, changed(last, last.sequence(), last.writerIdentity(), last.target(),
                last.keyFields(), last.rows().subList(0, last.rows().size() - 1)));
        reject(fixture, fixture.batches(), missing, "missing target rows");
    }

    @Test
    void foreignRowsAndEndMarkersAreRejectedRatherThanTrimmed() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        for (int unregistered : List.of(900_001, Integer.MAX_VALUE)) {
            var calls = new ArrayList<>(fixture.calls());
            var first = calls.getFirst();
            var rows = new ArrayList<>(first.rows());
            rows.set(0, new BenchmarkWriteReturnLedger.Row(1, List.of(unregistered)));
            calls.set(0, changed(first, first.sequence(), first.writerIdentity(), first.target(), first.keyFields(), rows));
            reject(fixture, fixture.batches(), calls, "not registered in this measured phase");
        }
    }

    @Test
    void aBoundaryTouchOfAnOtherwiseRegisteredKeyCannotBeSilentlyRemoved() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var calls = new ArrayList<>(fixture.calls());
        var first = calls.getFirst();
        calls.add(changed(first, calls.getLast().sequence() + 1, first.writerIdentity(), first.target(),
                first.keyFields(), List.of(first.rows().getFirst())));
        reject(fixture, fixture.batches(), calls, "target row is duplicate");
    }

    @Test
    void foreignPipelineWrongTableAndChangedWriterIdentityAreRejected() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var first = fixture.calls().getFirst();
        var calls = new ArrayList<>(fixture.calls());
        calls.set(0, changed(first, first.sequence(), "pdk.state.bench_copy_other.sink", first.target(),
                first.keyFields(), first.rows()));
        reject(fixture, fixture.batches(), calls, "another target pipeline");
        calls.set(0, changed(first, first.sequence(), first.writerIdentity(), "unregistered_table",
                first.keyFields(), first.rows()));
        reject(fixture, fixture.batches(), calls, "target table is unregistered");
        calls = new ArrayList<>(fixture.calls());
        var second = calls.get(1);
        calls.set(1, changed(second, second.sequence(), second.writerIdentity() + "_changed", second.target(),
                second.keyFields(), second.rows()));
        reject(fixture, fixture.batches(), calls, "writer identity changed");
    }

    @Test
    void repeatedCallSequenceAndDeletedMeasuredRowsAreRejected() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var calls = new ArrayList<>(fixture.calls());
        var second = calls.get(1);
        calls.set(1, changed(second, calls.getFirst().sequence(), second.writerIdentity(), second.target(),
                second.keyFields(), second.rows()));
        reject(fixture, fixture.batches(), calls, "call sequence is missing or duplicate");
        calls = new ArrayList<>(fixture.calls());
        var first = calls.getFirst();
        var rows = new ArrayList<>(first.rows());
        rows.set(0, new BenchmarkWriteReturnLedger.Row(3, rows.getFirst().keys()));
        calls.set(0, changed(first, first.sequence(), first.writerIdentity(), first.target(), first.keyFields(), rows));
        reject(fixture, fixture.batches(), calls, "delete or unsupported Tap kind");
    }

    @Test
    void incompleteDuplicateOrReorderedBatchMetadataHasNoSourceOrigin() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        reject(fixture, fixture.batches().subList(1, fixture.batches().size()), fixture.calls(), "source batch roster");
        var duplicate = new ArrayList<>(fixture.batches());
        duplicate.set(1, new BenchmarkForkEnvironment.BatchResult(0, 1300, 1400));
        reject(fixture, duplicate, fixture.calls(), "source batch index");
        var reordered = new ArrayList<>(fixture.batches());
        Collections.swap(reordered, 0, 1);
        reject(fixture, reordered, fixture.calls(), "source batch index");
    }

    @Test
    void reversedOverlappingOrOverflowingSourceTimesAreRejected() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var reversed = new ArrayList<>(fixture.batches());
        reversed.set(0, new BenchmarkForkEnvironment.BatchResult(0, 1000, 999));
        reject(fixture, reversed, fixture.calls(), "source batch clock order");
        var overlap = new ArrayList<>(fixture.batches());
        overlap.set(1, new BenchmarkForkEnvironment.BatchResult(1, 1050, 1250));
        reject(fixture, overlap, fixture.calls(), "serial source batches overlap");
        var overflow = new ArrayList<>(fixture.batches());
        overflow.set(0, new BenchmarkForkEnvironment.BatchResult(0, Long.MIN_VALUE, Long.MAX_VALUE));
        reject(fixture, overflow, fixture.calls(), "source batch clock order");
    }

    @Test
    void oneExpectedKeyCannotNameTwoDifferentSourceBatches() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var sql = new ArrayList<>(fixture.phase().sql());
        sql.set(1, sql.getFirst());
        var changed = phase(fixture.phase(), fixture.phase().expectedLogicalOutputChanges(), sql, fixture.phase().targets());
        var workload = withPhase(fixture.workload(), changed);
        assertThatThrownBy(() -> BenchmarkWriteReturnExpectations.associate(workload, changed,
                fixture.batches(), fixture.calls())).isInstanceOf(AssertionError.class)
                .hasMessageContaining("ambiguous source batch");
    }

    @Test
    void physicalTableNamesCannotAmbiguouslyIdentifyTwoTargets() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var target = fixture.phase().targets().getFirst();
        var duplicateTable = new BenchmarkWorkloadDefinitions.TargetExpectation(target.pipelineId(),
                BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW, target.table(),
                BenchmarkWorkloadDefinitions.Projection.JOIN, target.rows(), target.checksum());
        var changed = phase(fixture.phase(), fixture.phase().expectedLogicalOutputChanges(), fixture.phase().sql(),
                List.of(target, duplicateTable));
        var workload = withPhase(fixture.workload(), changed);
        assertThatThrownBy(() -> BenchmarkWriteReturnExpectations.associate(workload, changed,
                fixture.batches(), fixture.calls())).isInstanceOf(AssertionError.class)
                .hasMessageContaining("target table is ambiguous");
    }

    @Test
    void keyFieldRegistrationAndNativeCallLimitsRemainExact() {
        Fixture fixture = fixture("stateless", false, "cdc-update", 233);
        var first = fixture.calls().getFirst();
        var calls = new ArrayList<>(fixture.calls());
        calls.set(0, changed(first, first.sequence(), first.writerIdentity(), first.target(), List.of("id"), first.rows()));
        reject(fixture, fixture.batches(), calls, "key field registration differs");
        Fixture oversized = fixture("copy", false, "cdc-update", 1025);
        reject(oversized, oversized.batches(), oversized.calls(), "call row roster differs");
    }

    @Test
    void aRegistrationBeyondTheDerivedRowBoundIsRefusedBeforeAssociation() {
        Fixture fixture = fixture("copy", false, "cdc-update", 233);
        var changed = phase(fixture.phase(), 512L * 512 + 1, fixture.phase().sql(), fixture.phase().targets());
        var workload = withPhase(fixture.workload(), changed);
        assertThatThrownBy(() -> BenchmarkWriteReturnExpectations.associate(workload, changed,
                fixture.batches(), List.of())).isInstanceOf(AssertionError.class)
                .hasMessageContaining("phase registration exceeds the record bound");
    }

    @Test
    void unmeasuredPhasesCannotCreateAnImplicitBoundaryRegistration() {
        var workload = BenchmarkWorkloadDefinitions.byId("copy");
        for (String phase : List.of("snapshot", "warm-up", "terminal")) {
            assertThatThrownBy(() -> BenchmarkWriteReturnExpectations.associate(workload, workload.phase(phase),
                    List.of(), List.of())).isInstanceOf(AssertionError.class).hasMessageContaining("phase is not measured");
        }
    }

    private record Fixture(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkWorkloadDefinitions.Phase phase,
            List<BenchmarkForkEnvironment.BatchResult> batches, List<BenchmarkWriteReturnAssembly.FullCall> calls) { }

    private static Fixture fixture(String id, boolean pilot, String phase, int callSize) {
        var workload = pilot ? BenchmarkWorkloadDefinitions.steadyPilot(id) : BenchmarkWorkloadDefinitions.byId(id);
        return fixture(workload, workload.phase(phase), callSize);
    }

    private static Fixture fixture(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.Phase phase, int callSize) {
        List<BenchmarkForkEnvironment.BatchResult> batches = new ArrayList<>();
        for (int index = 0; index < phase.batches().size(); index++) {
            long issued = 1000L + 200L * index;
            batches.add(new BenchmarkForkEnvironment.BatchResult(index, issued, issued + 100));
        }
        List<BenchmarkWriteReturnAssembly.FullCall> calls = new ArrayList<>();
        int writer = 0;
        for (var plan : BenchmarkExpectedChanges.forPhase(workload, phase)) {
            writer++;
            List<String> fields = switch (plan.target().projection()) {
                case COPY, NEST -> List.of("id");
                case JOIN -> List.of("order_id");
                case STATELESS -> List.of("item_index", "id");
            };
            List<BenchmarkWriteReturnLedger.Row> rows = new ArrayList<>();
            for (var batch : plan.batches()) {
                for (var expected : batch) {
                    String[] key = expected.key().split(":");
                    Map<String, Integer> values = Map.of("id", Integer.parseInt(key[0]),
                            "order_id", Integer.parseInt(key[0]), "item_index", key.length == 2 ? Integer.parseInt(key[1]) : 0);
                    rows.add(new BenchmarkWriteReturnLedger.Row(1, fields.stream().map(values::get).toList()));
                }
            }
            for (int index = 0; index < rows.size(); index += callSize) {
                List<BenchmarkWriteReturnLedger.Row> nativeRows = rows.subList(index, Math.min(index + callSize, rows.size()));
                long sequence = calls.size() + 1L;
                calls.add(new BenchmarkWriteReturnAssembly.FullCall(sequence, writer, nativeRows.size(),
                        -1_000_000 + sequence * 100, -999_975 + sequence * 100, -999_950 + sequence * 100, true, 1,
                        nativeRows.size(), 0, 0, 0, List.of(), "",
                        "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT",
                        "pdk.state." + plan.target().pipelineId() + ".sink", "stream", plan.target().table(), fields, nativeRows));
            }
        }
        return new Fixture(workload, phase, List.copyOf(batches), List.copyOf(calls));
    }

    private static List<BenchmarkWriteReturnExpectations.Association> associate(Fixture fixture) {
        return BenchmarkWriteReturnExpectations.associate(fixture.workload(), fixture.phase(), fixture.batches(), fixture.calls());
    }

    private static BenchmarkWriteReturnAssembly.FullCall changed(BenchmarkWriteReturnAssembly.FullCall call,
            long sequence, String writerIdentity, String target, List<String> fields, List<BenchmarkWriteReturnLedger.Row> rows) {
        return new BenchmarkWriteReturnAssembly.FullCall(sequence, call.writer(), rows.size(), call.beganNanos(), call.lastCallbackExitNanos(), call.observedNanos(),
                true, 1, rows.size(), 0, 0, 0, List.of(), "", call.scope(), writerIdentity, call.stream(), target, fields, rows);
    }

    private static void reject(Fixture fixture, List<BenchmarkForkEnvironment.BatchResult> batches,
            List<BenchmarkWriteReturnAssembly.FullCall> calls, String reason) {
        assertThatThrownBy(() -> BenchmarkWriteReturnExpectations.associate(fixture.workload(), fixture.phase(), batches, calls))
                .isInstanceOf(AssertionError.class).hasMessageContaining(reason);
    }

    private static BenchmarkWorkloadDefinitions.Phase phase(BenchmarkWorkloadDefinitions.Phase original, long changes,
            List<String> sql, List<BenchmarkWorkloadDefinitions.TargetExpectation> targets) {
        return new BenchmarkWorkloadDefinitions.Phase(original.id(), original.stage(), true, changes, sql,
                original.statementsPerBatch(), original.batchInterval(), original.expectedLogicalCoverage(), targets);
    }

    private static BenchmarkWorkloadDefinitions.Workload withPhase(BenchmarkWorkloadDefinitions.Workload original,
            BenchmarkWorkloadDefinitions.Phase replacement) {
        var phases = new ArrayList<>(original.phases());
        phases.set(phases.indexOf(original.phase(replacement.id())), replacement);
        return new BenchmarkWorkloadDefinitions.Workload(original.id(), original.seed(), original.rows(), original.database(),
                original.pipelineIds(), original.sourceChains(), original.setupSql(), phases);
    }
}
