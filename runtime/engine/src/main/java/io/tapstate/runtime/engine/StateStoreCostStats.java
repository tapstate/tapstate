package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.metrics.Metric;
import io.tapstate.runtime.engine.StateStoreCostProbe.Codec;
import io.tapstate.runtime.engine.StateStoreCostProbe.Operation;
import io.tapstate.runtime.engine.StateStoreCostMetricNames.Kind;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private static final Logger LOG = Logger.getLogger(StateStoreCostStats.class.getName());

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
        counters(namespace).completed(Objects.requireNonNull(operation), nanos, payloadBytes);
    }

    @Override
    public void failed(String namespace, Operation operation, long nanos) {
        if (nanos < 0) {
            throw new IllegalArgumentException("state-store duration cannot be negative");
        }
        counters(namespace).failed(Objects.requireNonNull(operation), nanos);
    }

    @Override
    public void serialized(String namespace, Codec codec, long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("serialized bytes cannot be negative");
        }
        counters(namespace).serialized(Objects.requireNonNull(codec), bytes);
    }

    /** Empty means no cold-layer operation or codec completion was observed for this namespace. */
    public Optional<Reading> reading(String namespace) {
        Counters counters = byNamespace.get(Objects.requireNonNull(namespace));
        if (counters == null) {
            return Optional.empty();
        }
        return Optional.of(counters.snapshot());
    }

    /** Captures the per-member baseline before a new job starts handling state. */
    Baseline baseline(String namespace) {
        Counters counters = counters(namespace);
        return new Baseline(namespace, counters, counters.snapshot());
    }

    /** Publishes one job's complete member-local delta through handles created on its processor thread. */
    boolean attach(Baseline baseline, Map<Kind, Metric> handles) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(handles, "handles");
        if (byNamespace.get(baseline.namespace) != baseline.counters) {
            return false;
        }
        return baseline.counters.attach(baseline.reading, handles);
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

    static final class Baseline {
        private final String namespace;
        private final Counters counters;
        private final Reading reading;

        private Baseline(String namespace, Counters counters, Reading reading) {
            this.namespace = namespace;
            this.counters = counters;
            this.reading = reading;
        }
    }

    private static final class Counters {
        private final EnumMap<Operation, OperationCounters> operations = new EnumMap<>(Operation.class);
        private final EnumMap<Codec, CodecCounters> codecs = new EnumMap<>(Codec.class);
        private Registration registration;

        private Counters() {
            for (Operation operation : Operation.values()) {
                operations.put(operation, new OperationCounters());
            }
            for (Codec codec : Codec.values()) {
                codecs.put(codec, new CodecCounters());
            }
        }

        synchronized void completed(Operation operation, long nanos, long payloadBytes) {
            OperationCounters counted = operations.get(operation);
            counted.completed.increment();
            counted.durationNanos.add(nanos);
            counted.payloadBytes.add(payloadBytes);
            publish(StateStoreCostMetricNames.forOperation(operation));
        }

        synchronized void failed(Operation operation, long nanos) {
            OperationCounters counted = operations.get(operation);
            counted.failed.increment();
            counted.durationNanos.add(nanos);
            publish(StateStoreCostMetricNames.forOperation(operation));
        }

        synchronized void serialized(Codec codec, long bytes) {
            CodecCounters counted = codecs.get(codec);
            counted.completed.increment();
            counted.bytes.add(bytes);
            publish(StateStoreCostMetricNames.forCodec(codec));
        }

        synchronized Reading snapshot() {
            Map<Operation, OperationReading> measuredOperations = new EnumMap<>(Operation.class);
            operations.forEach((operation, counted) -> {
                long completed = counted.completed.sum();
                long failed = counted.failed.sum();
                if (completed != 0 || failed != 0) {
                    measuredOperations.put(operation, new OperationReading(completed, failed,
                            counted.durationNanos.sum(), counted.payloadBytes.sum()));
                }
            });
            Map<Codec, CodecReading> measuredCodecs = new EnumMap<>(Codec.class);
            codecs.forEach((codec, counted) -> {
                long completed = counted.completed.sum();
                if (completed > 0) {
                    measuredCodecs.put(codec, new CodecReading(completed, counted.bytes.sum()));
                }
            });
            return new Reading(Map.copyOf(measuredOperations), Map.copyOf(measuredCodecs));
        }

        synchronized boolean attach(Reading baseline, Map<Kind, Metric> handles) {
            Reading current = snapshot();
            for (Kind kind : Kind.values()) {
                if (kind == Kind.READY || kind == Kind.CLUSTER_SIZE) {
                    continue;
                }
                long difference = kind.value(current) - kind.value(baseline);
                if (difference < 0 || !handles.containsKey(kind)) {
                    return false;
                }
            }
            try {
                for (Kind kind : Kind.values()) {
                    if (kind != Kind.READY && kind != Kind.CLUSTER_SIZE) {
                        handles.get(kind).set(kind.value(current) - kind.value(baseline));
                    }
                }
                handles.get(Kind.READY).set(1);
                registration = new Registration(baseline, Map.copyOf(handles));
                return true;
            } catch (RuntimeException failed) {
                invalidate(handles, failed);
                return false;
            }
        }

        private void publish(Kind[] kinds) {
            Registration active = registration;
            if (active == null) {
                return;
            }
            try {
                for (Kind kind : kinds) {
                    long difference = value(kind) - kind.value(active.baseline);
                    if (difference < 0) {
                        invalidate(active.handles, new IllegalStateException("state cost counter moved backward"));
                        registration = null;
                        return;
                    }
                    active.handles.get(kind).set(difference);
                }
            } catch (RuntimeException failed) {
                invalidate(active.handles, failed);
                registration = null;
            }
        }

        private long value(Kind kind) {
            if (kind == Kind.READY || kind == Kind.CLUSTER_SIZE) {
                return 0;
            }
            if (kind.operation() != null) {
                OperationCounters counted = operations.get(kind.operation());
                return switch (kind.field()) {
                    case COMPLETED -> counted.completed.sum();
                    case FAILED -> counted.failed.sum();
                    case DURATION_NANOS -> counted.durationNanos.sum();
                    case PAYLOAD_BYTES -> counted.payloadBytes.sum();
                    default -> 0;
                };
            }
            CodecCounters counted = codecs.get(kind.codec());
            return switch (kind.field()) {
                case COMPLETED -> counted.completed.sum();
                case BYTES -> counted.bytes.sum();
                default -> 0;
            };
        }

        private static void invalidate(Map<Kind, Metric> handles, RuntimeException failure) {
            try {
                Metric ready = handles.get(Kind.READY);
                if (ready != null) {
                    ready.set(0);
                }
            } catch (RuntimeException ignored) {
                // Metric publication must not change the outcome of a cold-store operation.
            }
            LOG.log(Level.WARNING, "Could not publish state-store cost metrics", failure);
        }
    }

    private record Registration(Reading baseline, Map<Kind, Metric> handles) { }

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
