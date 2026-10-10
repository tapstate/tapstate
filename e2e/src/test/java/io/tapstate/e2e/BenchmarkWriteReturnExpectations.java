package io.tapstate.e2e;

import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exact source registrations for a measured phase; association does not qualify either clock domain. */
final class BenchmarkWriteReturnExpectations {
    private static final int MAX_RECORDS = 512 * 512;
    private static final int MAX_FRAMES = 512;
    private static final int MAX_PART_ROWS = 512;
    private static final int MAX_CALL_ROWS = 1024;
    private static final int MAX_WRITERS = 128;

    private BenchmarkWriteReturnExpectations() { }

    /** The input Tap kind is preserved separately from the independent physical change-stream oracle. */
    record Association(BenchmarkWorkloadDefinitions.TargetExpectation target, String key,
            int sourceBatchIndex, long sourceIssuedAtNanos, long sourceCompletedAtNanos,
            long callSequence, int writer, String writerIdentity, String stream,
            long callBeganNanos, long callLastCallbackExitNanos, long callObservedNanos, int inputTapKind, boolean fixedCohort) { }

    private record Origin(BenchmarkWorkloadDefinitions.TargetExpectation target,
            BenchmarkForkEnvironment.BatchResult source) { }
    private record TargetKey(String table, String key) { }

    /**
     * Every supplied row must belong to this measured phase. Boundary or terminal markers require a
     * different explicit registration and are refused here rather than trimmed from the receipt.
     */
    static List<Association> associate(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.Phase phase, List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
            List<BenchmarkWriteReturnAssembly.FullCall> calls) {
        require(workload != null && phase != null && sourceBatches != null && calls != null,
                "input is missing");
        require(phase.measured(), "phase is not measured");
        require(phase.expectedLogicalOutputChanges() >= 0 && phase.expectedLogicalOutputChanges() <= MAX_RECORDS,
                "phase registration exceeds the record bound");
        require(calls.size() <= MAX_FRAMES, "call roster exceeds the frame bound");
        List<BenchmarkForkEnvironment.BatchResult> batches = sourceRoster(phase, sourceBatches);
        List<BenchmarkExpectedChanges.TargetPlan> plans = BenchmarkExpectedChanges.forPhase(workload, phase);
        Map<String, BenchmarkExpectedChanges.TargetPlan> byTable = new LinkedHashMap<>();
        Map<TargetKey, Origin> origins = new LinkedHashMap<>();
        for (BenchmarkExpectedChanges.TargetPlan plan : plans) {
            require(plan.target().table() != null && byTable.putIfAbsent(plan.target().table(), plan) == null,
                    "target table is ambiguous");
            require(workload.pipelineIds().contains(plan.target().pipelineId()), "target pipeline is unregistered");
            require(plan.batches().size() == batches.size(), "target and source batch rosters differ");
            for (int index = 0; index < batches.size(); index++) {
                for (BenchmarkMongoDeliveryObserver.ExpectedChange expected : plan.forBatch(index)) {
                    require(expected.kind() == BenchmarkMongoDeliveryObserver.Kind.UPDATE,
                            "fixed measured registration is not an update");
                    TargetKey key = new TargetKey(plan.target().table(), expected.key());
                    require(origins.size() < MAX_RECORDS && origins.putIfAbsent(key,
                            new Origin(plan.target(), batches.get(index))) == null,
                            "expected target key has an ambiguous source batch");
                }
            }
        }
        require(origins.size() == phase.expectedLogicalOutputChanges(), "registration count differs from phase");

        Set<TargetKey> seen = new HashSet<>();
        Set<Long> sequences = new HashSet<>();
        Map<Integer, String> writers = new LinkedHashMap<>();
        List<Association> associated = new ArrayList<>(origins.size());
        int frames = 0;
        for (BenchmarkWriteReturnAssembly.FullCall call : List.copyOf(calls)) {
            require(call != null && call.sequence() > 0 && sequences.add(call.sequence()),
                    "call sequence is missing or duplicate");
            require(call.writer() > 0 && call.writer() <= MAX_WRITERS, "writer number exceeds its bound");
            require(call.totalRows() > 0 && call.totalRows() <= MAX_CALL_ROWS
                    && call.rows().size() == call.totalRows(), "call row roster differs");
            frames += (call.totalRows() + MAX_PART_ROWS - 1) / MAX_PART_ROWS;
            require(frames <= MAX_FRAMES, "call pieces exceed the frame bound");
            require(ordered(call.beganNanos(), call.lastCallbackExitNanos())
                    && ordered(call.lastCallbackExitNanos(), call.observedNanos()), "call callback clock order is invalid");
            require(call.returnedNormally() && call.callbackCount() > 0 && call.callbackCount() <= MAX_PART_ROWS
                    && call.errors() == 0 && call.errorDetails().isEmpty()
                    && call.failureType() != null && call.failureType().isEmpty()
                    && nonnegativeTotal(call.inserted(), call.modified(), call.removed()) == call.totalRows(),
                    "call is failed, partial or missing callbacks");
            boundedText(call.writerIdentity(), 512, "writer identity");
            boundedText(call.stream(), 512, "stream");
            boundedText(call.target(), 512, "target");
            BenchmarkExpectedChanges.TargetPlan plan = byTable.get(call.target());
            require(plan != null, "call target table is unregistered");
            String prefix = "pdk.state." + plan.target().pipelineId() + ".";
            require(call.writerIdentity().startsWith(prefix) && call.writerIdentity().length() > prefix.length(),
                    "call writer belongs to another target pipeline");
            String priorWriter = writers.putIfAbsent(call.writer(), call.writerIdentity());
            require(priorWriter == null || priorWriter.equals(call.writerIdentity()), "call writer identity changed");
            Set<String> fields = new HashSet<>(call.keyFields());
            require(fields.size() == call.keyFields().size() && fields.equals(keyFields(plan.target().projection())),
                    "call key field registration differs");

            for (BenchmarkWriteReturnLedger.Row row : call.rows()) {
                require(row != null && (row.kind() == 1 || row.kind() == 2),
                        "fixed measured update received a delete or unsupported Tap kind");
                require(row.keys().size() == call.keyFields().size(), "row key tuple differs from field registration");
                Document keyDocument = new Document();
                for (int index = 0; index < row.keys().size(); index++) {
                    Integer value = row.keys().get(index);
                    require(value != null, "row key value is missing");
                    keyDocument.put(call.keyFields().get(index), value);
                }
                // The target's existing extractor determines tuple semantics, not receipt tuple order.
                String logicalKey = plan.keyOf().apply(keyDocument);
                TargetKey key = new TargetKey(call.target(), logicalKey);
                Origin origin = origins.get(key);
                require(origin != null, "row is not registered in this measured phase");
                require(seen.add(key), "target row is duplicate");
                require(associated.size() < MAX_RECORDS, "association exceeds the record bound");
                var source = origin.source();
                associated.add(new Association(origin.target(), logicalKey, source.index(), source.issuedAtNanos(),
                        source.completedAtNanos(), call.sequence(), call.writer(), call.writerIdentity(), call.stream(),
                        call.beganNanos(), call.lastCallbackExitNanos(), call.observedNanos(), row.kind(), workload.inFixedCohort(logicalKey)));
            }
        }
        require(seen.size() == origins.size(), "measured phase has missing target rows");
        return List.copyOf(associated);
    }

    private static List<BenchmarkForkEnvironment.BatchResult> sourceRoster(
            BenchmarkWorkloadDefinitions.Phase phase, List<BenchmarkForkEnvironment.BatchResult> sourceBatches) {
        require(sourceBatches.size() == phase.batches().size(), "source batch roster is incomplete or extra");
        List<BenchmarkForkEnvironment.BatchResult> result = new ArrayList<>(sourceBatches.size());
        BenchmarkForkEnvironment.BatchResult previous = null;
        for (int index = 0; index < sourceBatches.size(); index++) {
            BenchmarkForkEnvironment.BatchResult source = sourceBatches.get(index);
            require(source != null && source.index() == index, "source batch index is duplicate, missing or reordered");
            require(ordered(source.issuedAtNanos(), source.completedAtNanos()), "source batch clock order is invalid");
            require(previous == null || ordered(previous.completedAtNanos(), source.issuedAtNanos()),
                    "serial source batches overlap or move backward");
            result.add(source);
            previous = source;
        }
        return List.copyOf(result);
    }

    private static Set<String> keyFields(BenchmarkWorkloadDefinitions.Projection projection) {
        return switch (projection) {
            case COPY, NEST -> Set.of("id");
            case JOIN -> Set.of("order_id");
            case STATELESS -> Set.of("id", "item_index");
        };
    }

    private static boolean ordered(long began, long completed) {
        try { return Math.subtractExact(completed, began) >= 0; }
        catch (ArithmeticException overflow) { return false; }
    }

    private static long nonnegativeTotal(long inserted, long modified, long removed) {
        if (inserted < 0 || modified < 0 || removed < 0) return -1;
        try { return Math.addExact(Math.addExact(inserted, modified), removed); }
        catch (ArithmeticException overflow) { return -1; }
    }

    private static void boundedText(String value, int maximum, String field) {
        require(value != null && !value.isBlank() && value.length() <= maximum
                && value.getBytes(StandardCharsets.UTF_8).length <= maximum, "call " + field + " is invalid");
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError("return expectations " + reason);
    }
}
