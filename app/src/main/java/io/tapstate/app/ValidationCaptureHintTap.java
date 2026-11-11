package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRootHintBatch.ChangeKind;
import io.tapstate.runtime.engine.validation.ValidationRootHintOutbox;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A bounded key-only side tap around an existing capture listener. This does not open another capture,
 * subscribe a live run, authenticate its bindings, change source ACKs, or establish coverage barriers.
 * The controller binds these outboxes to this exact original capture incarnation before installation
 * and closes them on run/capture/owner invalidation. No binding follows a latest run automatically.
 *
 * <p>Extraction happens while the actual images are still borrowed. Install only key extractors whose
 * lossless selected-field behavior, allocation and nonblocking work have already been admitted. The
 * caller reserves each decoder's scratch and outbox envelope plus at most one packet's key references.
 * No business image escapes the call. Ordinary side-channel failures retire their affected bindings;
 * the original listener still receives the identical complete batch and position once. Fatal Errors
 * escape. Exceptions from the original listener keep their original semantics and are never swallowed.
 */
public final class ValidationCaptureHintTap {
    @FunctionalInterface
    public interface Keys {
        Change extract(ChangeKind kind, Map<String, Object> before, Map<String, Object> after);
    }
    public record Binding(ValidationRootHintOutbox outbox, Keys keys) {
        public Binding { Objects.requireNonNull(outbox, "outbox"); Objects.requireNonNull(keys, "keys"); }
    }
    public record Limits(int events, int subscriptions, long keyBytes) {
        public Limits {
            if (events < 1 || events > 4096 || subscriptions < 1 || subscriptions > 16
                    || keyBytes < 1 || keyBytes > 64L * 1024 * 1024) throw invalid("hint.tapLimits");
        }
    }
    private ValidationCaptureHintTap() {}

    /** Preserves the optional starting-position interface instead of adding an anchor request implicitly. */
    public static CaptureListener wrap(CaptureListener listener, List<Binding> bindings, Limits limits) {
        Objects.requireNonNull(listener, "listener"); Objects.requireNonNull(limits, "limits");
        if (bindings == null || bindings.isEmpty() || bindings.size() > limits.subscriptions()) throw invalid("hint.tapBindings");
        var distinct = new IdentityHashMap<ValidationRootHintOutbox, Boolean>();
        for (Binding binding : bindings) {
            Objects.requireNonNull(binding, "binding");
            if (distinct.put(binding.outbox(), Boolean.TRUE) != null) throw invalid("hint.tapDuplicateOutbox");
            if (binding.outbox().limits().batchBytes() <= ValidationRootHintBatch.headerBytes(binding.outbox().run(), binding.outbox().sender())) {
                throw invalid("hint.tapPacketBytes");
            }
        }
        List<Binding> fixed = List.copyOf(bindings);
        return listener instanceof CaptureStartedListener started ? new Started(started, fixed, limits)
                : new Tap(listener, fixed, limits);
    }

    private static class Tap implements CaptureListener {
        private final CaptureListener listener;
        private final List<Binding> bindings;
        private final Limits limits;
        private Tap(CaptureListener listener, List<Binding> bindings, Limits limits) {
            this.listener = listener; this.bindings = bindings; this.limits = limits;
        }
        @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            Objects.requireNonNull(events, "events"); Objects.requireNonNull(position, "position");
            if (events.size() > limits.events()) failAll(limit("callbackEvents"));
            else {
                var budget = new Budget(limits.keyBytes());
                for (Binding binding : bindings) {
                    if (!binding.outbox().isOpen()) continue;
                    try { extract(binding, events, budget); }
                    catch (RuntimeException failure) { binding.outbox().reportFailure(failure); }
                    catch (Error fatal) { binding.outbox().reportFailure(fatal); throw fatal; }
                }
            }
            listener.onBatch(events, position);
        }
        private void extract(Binding binding, List<Envelope> events, Budget budget) {
            var outbox = binding.outbox();
            var packet = new ArrayList<Change>();
            long packetBytes = 0;
            long packetLimit = outbox.limits().batchBytes() - ValidationRootHintBatch.headerBytes(outbox.run(), outbox.sender());
            for (Envelope event : events) {
                if (!outbox.isOpen()) return;
                if (!event.src().equals(outbox.sender().tableId())) continue;
                if (event.op() == Op.DDL) throw invalid("hint.sourceDdl");
                if (event.op() == Op.READ) throw invalid("hint.snapshotEvent");
                if (budget.remaining <= 0) throw limit("callbackKeyBytes");
                ChangeKind kind = switch (event.op()) {
                    case INSERT -> ChangeKind.INSERT;
                    case UPDATE -> ChangeKind.UPDATE;
                    case DELETE -> ChangeKind.DELETE;
                    default -> throw new IllegalStateException("non-row change passed the capture check");
                };
                Change change = Objects.requireNonNull(binding.keys().extract(kind, event.before(), event.after()), "extracted keys");
                if (change.kind() != kind) throw invalid("hint.extractedKind");
                long charge = change.retainedBytes();
                if (charge > budget.remaining) { budget.remaining = 0; throw limit("callbackKeyBytes"); }
                budget.remaining -= charge;
                if (charge > packetLimit) throw limit("packetKeyBytes");
                if (packet.size() == outbox.limits().changes() || charge > packetLimit - packetBytes) {
                    if (outbox.tryOffer(packet) != ValidationRootHintOutbox.Offer.ACCEPTED) return;
                    packet.clear(); packetBytes = 0;
                }
                packet.add(change); packetBytes += charge;
            }
            if (!packet.isEmpty()) outbox.tryOffer(packet);
        }
        private void failAll(Throwable failure) {
            for (Binding binding : bindings) if (binding.outbox().isOpen()) binding.outbox().reportFailure(failure);
        }
        @Override public void onError(Throwable failure) { failAll(failure); listener.onError(failure); }
        @Override public void onAcknowledged(SourcePosition position) { listener.onAcknowledged(position); }
        @Override public void onAcknowledgeFailed(Throwable failure) { listener.onAcknowledgeFailed(failure); }
    }
    private static final class Started extends Tap implements CaptureStartedListener {
        private final CaptureStartedListener listener;
        private Started(CaptureStartedListener listener, List<Binding> bindings, Limits limits) {
            super(listener, bindings, limits); this.listener = listener;
        }
        @Override public void onStart(SourcePosition position) { listener.onStart(position); }
    }
    private static final class Budget {
        private long remaining;
        private Budget(long remaining) { this.remaining = remaining; }
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "hint." + dimension), null);
    }
}
