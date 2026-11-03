package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.runtime.engine.SinkAckFactory;
import java.util.*;

/**
 * Volatile evidence for one actual Pipeline Jet attempt and its selected terminal outputs. The
 * assembly authenticates each original producer/writer before delivering these local facts. No
 * recovery cursor, row count or Pipeline RUNNING label substitutes for producer/commit receipts.
 * This is initial-load readiness only, not strong validation timing or proof of data agreement.
 */
final class ValidationInitialLoadState {
    enum Mode { SNAPSHOT, NO_INITIAL_LOAD, UNKNOWN }
    enum State { WAITING, READY, UNAVAILABLE }
    record Attempt(long jobId, long executionId) {}
    record Producer(String node, String boot, UUID member, int index) {
        Producer {
            Objects.requireNonNull(node); Objects.requireNonNull(boot); Objects.requireNonNull(member);
            if (node.isBlank() || boot.isBlank() || index < 0 || node.length() > 512 || boot.length() > 512) throw invalid("initial.producer");
        }
    }
    record Source(Mode mode, long epoch) {
        Source { Objects.requireNonNull(mode); if (mode == Mode.SNAPSHOT && epoch < 1) throw invalid("initial.epoch"); }
    }
    record Status(State state, String reason) {}
    private record Bound(String stream, String writer) {}
    private final SinkAckFactory.ExecutionWriters original;
    private final Map<String, Integer> expected;
    private final Map<String, Map<Producer, Source>> producers = new HashMap<>();
    private final Set<String> writers = new HashSet<>(), started = new HashSet<>();
    private final Set<Bound> routes = new HashSet<>();
    private final Map<Bound, Long> landed = new HashMap<>();
    private final Runnable requireCurrent;
    private final long maxBytes;
    private long bytes;
    private final Map<String, String> unavailable = new HashMap<>();

    ValidationInitialLoadState(SinkAckFactory.ExecutionWriters original, Map<String, Integer> producerCounts, long maxBytes, Runnable requireCurrent) {
        this.original = Objects.requireNonNull(original); Objects.requireNonNull(producerCounts); this.requireCurrent = Objects.requireNonNull(requireCurrent);
        if (maxBytes < 1024 || maxBytes > 8L * 1024 * 1024 || original.writers().byOutput().isEmpty()
                || producerCounts.size() > 4096 || original.writers().byOutput().size() > 1024
                || !producerCounts.keySet().equals(original.writers().byStream().keySet())) throw invalid("initial.roster");
        this.maxBytes = maxBytes; bytes = 2048;
        for (String stream : producerCounts.keySet()) { long charge = 128L + 2L * stream.length(); space(charge); bytes += charge; }
        this.expected = Map.copyOf(producerCounts);
        for (var stream : original.writers().byStream().entrySet()) {
            int count = expected.get(stream.getKey()); if (count < 1 || count > 1024) throw invalid("initial.producerCount");
            for (String writer : stream.getValue()) {
                long charge = 320L + 2L * (stream.getKey().length() + writer.length()); space(charge); bytes += charge;
                if (!routes.add(new Bound(stream.getKey(), writer))) throw invalid("initial.duplicateRoute");
                writers.add(writer);
            }
        }
        for (var output : original.writers().byOutput().entrySet()) {
            if (output.getValue().isEmpty()) throw invalid("initial.emptyOutput");
            long nameBytes = 128L + 2L * output.getKey().length(); space(nameBytes); bytes += nameBytes;
            for (var stream : output.getValue().entrySet()) {
                if (stream.getValue().isEmpty()) throw invalid("initial.emptyOutputRoute");
                for (String writer : stream.getValue()) {
                if (!routes.contains(new Bound(stream.getKey(), writer))) throw invalid("initial.outputRoute");
                long charge = 128L + 2L * (stream.getKey().length() + writer.length()); space(charge); bytes += charge;
                }
            }
        }
        space(0);
    }
    SinkAckFactory.ExecutionWriters original() { return original; }

    synchronized boolean source(Attempt attempt, String stream, Producer producer, Source state) {
        if (!same(attempt)) return false; source(stream, producer, state); return true;
    }
    private void source(String stream, Producer producer, Source state) {
        requireCurrent.run(); Objects.requireNonNull(producer); Objects.requireNonNull(state);
        if (!expected.containsKey(stream)) throw invalid("initial.source"); if (unavailable.containsKey(stream)) return;
        var known = producers.computeIfAbsent(stream, ignored -> new HashMap<>()); var previous = known.get(producer);
        if (previous != null) { if (!previous.equals(state)) unavailable.put(stream, "initial.source-changed"); return; }
        if (known.size() == expected.get(stream)) { unavailable.put(stream, "initial.producer-roster-changed"); return; }
        long charge = 384L + 2L * (producer.node().length() + producer.boot().length());
        if (charge > maxBytes - bytes) { unavailable.put(stream, "initial.evidence-capacity"); return; } bytes += charge;
        known.put(producer, state);
        if (state.mode() == Mode.UNKNOWN) unavailable.put(stream, "initial.source-unproven");
    }
    synchronized boolean writerStarted(Attempt attempt, String writer) {
        if (!same(attempt)) return false; writerStarted(writer); return true;
    }
    private void writerStarted(String writer) {
        requireCurrent.run(); if (!writers.contains(writer)) throw invalid("initial.writer"); started.add(writer);
    }
    synchronized boolean bound(Attempt attempt, String stream, String writer, SourceOrder through) {
        if (!same(attempt)) return false; bound(stream, writer, through); return true;
    }
    private void bound(String stream, String writer, SourceOrder through) {
        requireCurrent.run(); Objects.requireNonNull(through); if (unavailable.containsKey(stream)) return;
        if (through.seq() != SourceOrder.SNAPSHOT_SEQ) return;
        var key = new Bound(stream, writer); if (!routes.contains(key)) throw invalid("initial.boundRoute");
        var previous = landed.putIfAbsent(key, through.epoch());
        if (previous != null && previous.longValue() != through.epoch()) unavailable.put(stream, "initial.bound-epoch-changed");
    }
    synchronized Status status(Set<String> selected) {
        requireCurrent.run();
        if (selected == null || selected.isEmpty() || !original.writers().byOutput().keySet().containsAll(selected)) throw invalid("initial.selectedOutputs");
        var needed = new HashMap<String, Set<String>>();
        for (String output : selected) original.writers().byOutput().get(output).forEach((stream, ids) -> needed.computeIfAbsent(stream, ignored -> new HashSet<>()).addAll(ids));
        for (var stream : needed.entrySet()) {
            if (unavailable.containsKey(stream.getKey())) return new Status(State.UNAVAILABLE, unavailable.get(stream.getKey()));
            var known = producers.get(stream.getKey());
            if (known == null || known.size() != expected.get(stream.getKey())) return new Status(State.WAITING, "initial.source-starting");
            Long epoch = null; boolean noInitial = false;
            for (var source : known.values()) {
                if (source.mode() == Mode.UNKNOWN) return new Status(State.UNAVAILABLE, "initial.source-unproven");
                if (source.mode() == Mode.NO_INITIAL_LOAD) noInitial = true;
                else if (epoch == null) epoch = source.epoch();
                else if (epoch.longValue() != source.epoch()) return new Status(State.UNAVAILABLE, "initial.source-epoch-changed");
            }
            if (noInitial && epoch != null) return new Status(State.UNAVAILABLE, "initial.mixed-source-obligation");
            for (String writer : stream.getValue()) {
                if (!started.contains(writer)) return new Status(State.WAITING, "initial.writer-starting");
                Long actual = landed.get(new Bound(stream.getKey(), writer));
                if (epoch == null) { if (actual != null) return new Status(State.UNAVAILABLE, "initial.unexpected-load"); }
                else if (actual == null) return new Status(State.WAITING, "initial.snapshot-landing");
                else if (!actual.equals(epoch)) return new Status(State.UNAVAILABLE, "initial.bound-epoch-changed");
            }
        }
        return new Status(State.READY, null);
    }
    private void space(long added) { if (added > maxBytes - bytes) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation.initialEvidenceBytes"), null); }
    private boolean same(Attempt attempt) { return attempt != null && attempt.jobId() == original.jobId() && attempt.executionId() == original.executionId(); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
}
