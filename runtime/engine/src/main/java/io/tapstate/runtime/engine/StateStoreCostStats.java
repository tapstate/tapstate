package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.StateStoreCostProbe.Codec;
import io.tapstate.runtime.engine.StateStoreCostProbe.Operation;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Member-local costs measured at the operator state's cold-layer boundary. A namespace is a map name
 * supplied by the compiled graph, never a value read from a row. Each namespace has a fixed number of
 * counters regardless of how many keys it contains; teardown forgets the counters with the state.
 *
 * <p>One operation is one map-store call, including its encoding or decoding. Duration covers that call
 * through return or throw, and therefore includes a blocked store. Payload bytes are the actual byte
 * arrays handed to or returned by the keyed-state port; they do not claim to count command framing or
 * wire compression. Encoding and decoding count only completed Java serialization calls.
 */
public final class StateStoreCostStats implements StateStoreCostProbe {

    private static final String USER_CONTEXT_KEY = StateStoreCostStats.class.getName();

    private final Map<String, Counters> byNamespace = new ConcurrentHashMap<>();

    /** The member-local counter bank shared by this member's partition threads. */
    public static StateStoreCostStats of(HazelcastInstance member) {
        Objects.requireNonNull(member, "member");
        return (StateStoreCostStats) member.getUserContext()
                .computeIfAbsent(USER_CONTEXT_KEY, ignored -> new StateStoreCostStats());
    }

    @Override
    public void completed(String namespace, Operation operation, long nanos, long payloadBytes) {
        if (nanos < 0 || payloadBytes < 0) {
            throw new IllegalArgumentException("state-store costs cannot be negative");
        }
        OperationCounters counted = counters(namespace).operations.get(Objects.requireNonNull(operation));
        counted.completed.increment();
        counted.durationNanos.add(nanos);
        counted.payloadBytes.add(payloadBytes);
    }

    @Override
    public void failed(String namespace, Operation operation, long nanos) {
        if (nanos < 0) {
            throw new IllegalArgumentException("state-store duration cannot be negative");
        }
        OperationCounters counted = counters(namespace).operations.get(Objects.requireNonNull(operation));
        counted.failed.increment();
        counted.durationNanos.add(nanos);
    }

    @Override
    public void serialized(String namespace, Codec codec, long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("serialized bytes cannot be negative");
        }
        CodecCounters counted = counters(namespace).codecs.get(Objects.requireNonNull(codec));
        counted.completed.increment();
        counted.bytes.add(bytes);
    }

    /** Empty means no cold-layer operation or codec completion was observed for this namespace. */
    public Optional<Reading> reading(String namespace) {
        Counters counters = byNamespace.get(Objects.requireNonNull(namespace));
        if (counters == null) {
            return Optional.empty();
        }
        Map<Operation, OperationReading> operations = new EnumMap<>(Operation.class);
        counters.operations.forEach((operation, counted) -> {
            long completed = counted.completed.sum();
            long failed = counted.failed.sum();
            if (completed != 0 || failed != 0) {
                operations.put(operation, new OperationReading(completed, failed,
                        counted.durationNanos.sum(), counted.payloadBytes.sum()));
            }
        });
        Map<Codec, CodecReading> codecs = new EnumMap<>(Codec.class);
        counters.codecs.forEach((codec, counted) -> {
            long completed = counted.completed.sum();
            if (completed > 0) {
                codecs.put(codec, new CodecReading(completed, counted.bytes.sum()));
            }
        });
        return Optional.of(new Reading(Map.copyOf(operations), Map.copyOf(codecs)));
    }

    public void forget(String namespace) {
        byNamespace.remove(Objects.requireNonNull(namespace));
    }

    private Counters counters(String namespace) {
        return byNamespace.computeIfAbsent(Objects.requireNonNull(namespace), ignored -> new Counters());
    }

    public record Reading(Map<Operation, OperationReading> operations, Map<Codec, CodecReading> codecs) {
    }

    public record OperationReading(long completed, long failed, long durationNanos, long payloadBytes) {
    }

    public record CodecReading(long completed, long bytes) {
    }

    private static final class Counters {
        private final EnumMap<Operation, OperationCounters> operations = new EnumMap<>(Operation.class);
        private final EnumMap<Codec, CodecCounters> codecs = new EnumMap<>(Codec.class);

        private Counters() {
            for (Operation operation : Operation.values()) {
                operations.put(operation, new OperationCounters());
            }
            for (Codec codec : Codec.values()) {
                codecs.put(codec, new CodecCounters());
            }
        }
    }

    private static final class OperationCounters {
        private final LongAdder completed = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final LongAdder durationNanos = new LongAdder();
        private final LongAdder payloadBytes = new LongAdder();
    }

    private static final class CodecCounters {
        private final LongAdder completed = new LongAdder();
        private final LongAdder bytes = new LongAdder();
    }
}
