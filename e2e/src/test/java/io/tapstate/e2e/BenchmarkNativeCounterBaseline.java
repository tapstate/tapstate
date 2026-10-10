package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

/** Keeps native publication identity separate from a flat counter's apparent quiet period. */
final class BenchmarkNativeCounterBaseline {
    record Snapshot(String pipeline, String jobId, String memberUuid, String executionId,
                    long publicationStamp, long readStartedAtNanos, long readCompletedAtNanos,
                    Set<String> expectedSinkIdentities, Map<String, Long> counters) {
        Snapshot {
            text(pipeline); text(jobId); text(memberUuid); text(executionId);
            if (publicationStamp <= 0 || Math.subtractExact(readCompletedAtNanos, readStartedAtNanos) < 0) {
                throw new AssertionError("native baseline snapshot has no ordered publication/read identity");
            }
            expectedSinkIdentities = Set.copyOf(expectedSinkIdentities);
            counters = Map.copyOf(counters);
            if (expectedSinkIdentities.isEmpty() || expectedSinkIdentities.size() > 128
                    || counters.isEmpty() || counters.size() > 4096) {
                throw new AssertionError("native baseline snapshot roster exceeds its bound or is missing");
            }
            int bytes = 0;
            long total = 0;
            for (String sink : expectedSinkIdentities) { text(sink); bytes = Math.addExact(bytes, size(sink)); }
            for (var counter : counters.entrySet()) {
                text(counter.getKey());
                if (counter.getValue() < 0) { throw new AssertionError("native baseline counter is negative"); }
                try { total = Math.addExact(total, counter.getValue()); }
                catch (ArithmeticException overflow) { throw new AssertionError("native baseline counter total overflow", overflow); }
                bytes = Math.addExact(bytes, size(counter.getKey()) + Long.BYTES);
            }
            if (bytes > 64 * 1024) { throw new AssertionError("native baseline vector exceeds its byte bound"); }
        }

        long total() {
            long result = 0;
            for (long value : counters.values()) { result = Math.addExact(result, value); }
            return result;
        }
    }

    private final long acknowledgedAtNanos;
    private final long quietNanos;
    private boolean observed;
    private long previousTotal;
    private long unchangedSince;
    private long lastCompletedAt;
    private Snapshot previous;
    private int successors;
    private int attempts;
    private boolean failed;
    private boolean admitted;
    private final List<Snapshot> checkpoints = new ArrayList<>();

    BenchmarkNativeCounterBaseline(long acknowledgedAtNanos, Duration quiet) {
        this.acknowledgedAtNanos = acknowledgedAtNanos;
        quietNanos = quiet.toNanos();
        if (quietNanos <= 0) { throw new AssertionError("native baseline quiet interval must be positive"); }
    }

    boolean observe(Snapshot snapshot, long publishedTotal, long completedAtNanos) {
        if (failed || admitted) { throw new AssertionError("native baseline guard is no longer open"); }
        try { return observeOpen(snapshot, publishedTotal, completedAtNanos); }
        catch (RuntimeException | Error invalid) { failed = true; throw invalid; }
    }

    private boolean observeOpen(Snapshot snapshot, long publishedTotal, long completedAtNanos) {
        if (++attempts > 512) { throw new AssertionError("native baseline poll capacity is exhausted"); }
        if (Math.subtractExact(snapshot.readStartedAtNanos(), acknowledgedAtNanos) < 0
                || Math.subtractExact(completedAtNanos, snapshot.readCompletedAtNanos()) < 0 || publishedTotal < 0) {
            throw new AssertionError("native baseline read precedes its acknowledgement or completion");
        }
        boolean vectorChanged = false;
        if (previous != null) {
            if (Math.subtractExact(snapshot.readStartedAtNanos(), lastCompletedAt) < 0
                    || !previous.pipeline().equals(snapshot.pipeline()) || !previous.jobId().equals(snapshot.jobId())
                    || !previous.memberUuid().equals(snapshot.memberUuid()) || !previous.executionId().equals(snapshot.executionId())
                    || !previous.expectedSinkIdentities().equals(snapshot.expectedSinkIdentities())
                    || !previous.counters().keySet().equals(snapshot.counters().keySet())) {
                throw new AssertionError("native baseline identity, roster or serial request order changed");
            }
            if (snapshot.publicationStamp() < previous.publicationStamp() || publishedTotal < previousTotal) {
                throw new AssertionError("native baseline publication or flat counter moved backward");
            }
            for (var cell : snapshot.counters().entrySet()) {
                if (cell.getValue() < previous.counters().get(cell.getKey())) {
                    throw new AssertionError("native baseline settled counter moved backward");
                }
            }
            vectorChanged = !previous.counters().equals(snapshot.counters());
            if (snapshot.publicationStamp() == previous.publicationStamp() && vectorChanged) {
                throw new AssertionError("native baseline vector changed inside one immutable publication");
            }
            if (snapshot.publicationStamp() > previous.publicationStamp()) {
                successors++;
                if (successors <= 2) { checkpoints.add(snapshot); }
            }
        } else { checkpoints.add(snapshot); }
        if (!observed || publishedTotal != previousTotal || vectorChanged) {
            unchangedSince = completedAtNanos;
        }
        observed = true;
        previous = snapshot;
        previousTotal = publishedTotal;
        lastCompletedAt = completedAtNanos;
        admitted = successors >= 2 && snapshot.total() == publishedTotal
                && Math.subtractExact(completedAtNanos, unchangedSince) >= quietNanos;
        return admitted;
    }

    long total() {
        if (!admitted) { throw new AssertionError("native baseline has not been admitted"); }
        return previousTotal;
    }

    Snapshot snapshot() {
        if (!admitted) { throw new AssertionError("native baseline has not been admitted"); }
        return previous;
    }

    /** Operation series may first appear during the phase; existing settled cells cannot disappear. */
    static long delta(Snapshot before, Snapshot after) {
        if (!before.pipeline().equals(after.pipeline()) || !before.jobId().equals(after.jobId())
                || !before.memberUuid().equals(after.memberUuid()) || !before.executionId().equals(after.executionId())
                || !before.expectedSinkIdentities().equals(after.expectedSinkIdentities())
                || !after.counters().keySet().containsAll(before.counters().keySet())
                || after.publicationStamp() <= before.publicationStamp()
                || Math.subtractExact(after.readStartedAtNanos(), before.readCompletedAtNanos()) < 0) {
            throw new AssertionError("native baseline pair changed execution, roster or order");
        }
        for (var cell : before.counters().entrySet()) {
            if (after.counters().get(cell.getKey()) < cell.getValue()) {
                throw new AssertionError("native baseline pair lost a settled counter");
            }
        }
        return Math.subtractExact(after.total(), before.total());
    }

    Map<String, Object> evidence() {
        var facts = new java.util.LinkedHashMap<String, Object>();
        facts.put("state", failed ? "UNKNOWN" : admitted ? "RECORDED_FRESH_NATIVE_BASELINE" : "PENDING");
        facts.put("acknowledgedAtNanos", acknowledgedAtNanos); facts.put("quietNanos", quietNanos);
        facts.put("successorPublications", successors); facts.put("attempts", attempts);
        facts.put("publicationRule", "LATEST_ONLY_NONOVERLAPPING_COLLECTOR_C0_THEN_TWO_SUCCESSORS_AFTER_ACK");
        facts.put("timestampScope", "OPAQUE_PUBLICATION_LABEL_NO_CROSS_JVM_WALL_SUBTRACTION");
        facts.put("counterCorrespondenceScope", "EXACT_NATIVE_AND_FLAT_TOTAL_FOR_FRESH_BENCHMARK_NO_CONTINUATION_OFFSET");
        facts.put("checkpoints", checkpoints.stream().map(BenchmarkNativeCounterBaseline::snapshotEvidence).toList());
        if (previous != null) {
            facts.put("lastSnapshot", snapshotEvidence(previous)); facts.put("flatTotal", previousTotal);
            facts.put("rawNativeTotal", previous.total()); facts.put("lastCompletedAtNanos", lastCompletedAt);
            facts.put("unchangedSinceNanos", unchangedSince);
        }
        facts.put("performanceAcceptanceEligible", false);
        return Map.copyOf(facts);
    }

    private static Map<String, Object> snapshotEvidence(Snapshot snapshot) {
        return Map.of("pipeline", snapshot.pipeline(), "jobId", snapshot.jobId(), "memberUuid", snapshot.memberUuid(),
                "executionId", snapshot.executionId(), "publicationStamp", snapshot.publicationStamp(),
                "readStartedAtNanos", snapshot.readStartedAtNanos(), "readCompletedAtNanos", snapshot.readCompletedAtNanos(),
                "expectedSinkIdentities", snapshot.expectedSinkIdentities(), "counters", snapshot.counters());
    }

    private static int size(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }
    private static void text(String value) {
        if (value == null || value.isBlank() || value.length() > 512 || size(value) > 512) {
            throw new AssertionError("native baseline identity is missing or exceeds its bound");
        }
    }
}
