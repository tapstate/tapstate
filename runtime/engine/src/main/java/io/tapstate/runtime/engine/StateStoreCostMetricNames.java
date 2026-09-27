package io.tapstate.runtime.engine;

import io.tapstate.runtime.engine.StateStoreCostProbe.Codec;
import io.tapstate.runtime.engine.StateStoreCostProbe.Operation;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

/** Names one job metric per member, cold-store namespace, and measured cost component. */
final class StateStoreCostMetricNames {

    static final String PREFIX = "stateStoreCost.";
    static final String VERTEX = ":state-store-cost";

    enum Kind {
        READY(Field.READY),
        CLUSTER_SIZE(Field.CLUSTER_SIZE),
        LOAD_COMPLETED(Operation.LOAD, Field.COMPLETED),
        LOAD_FAILED(Operation.LOAD, Field.FAILED),
        LOAD_DURATION_NANOS(Operation.LOAD, Field.DURATION_NANOS),
        LOAD_PAYLOAD_BYTES(Operation.LOAD, Field.PAYLOAD_BYTES),
        LOAD_ALL_COMPLETED(Operation.LOAD_ALL, Field.COMPLETED),
        LOAD_ALL_FAILED(Operation.LOAD_ALL, Field.FAILED),
        LOAD_ALL_DURATION_NANOS(Operation.LOAD_ALL, Field.DURATION_NANOS),
        LOAD_ALL_PAYLOAD_BYTES(Operation.LOAD_ALL, Field.PAYLOAD_BYTES),
        SAVE_COMPLETED(Operation.SAVE, Field.COMPLETED),
        SAVE_FAILED(Operation.SAVE, Field.FAILED),
        SAVE_DURATION_NANOS(Operation.SAVE, Field.DURATION_NANOS),
        SAVE_PAYLOAD_BYTES(Operation.SAVE, Field.PAYLOAD_BYTES),
        DELETE_COMPLETED(Operation.DELETE, Field.COMPLETED),
        DELETE_FAILED(Operation.DELETE, Field.FAILED),
        DELETE_DURATION_NANOS(Operation.DELETE, Field.DURATION_NANOS),
        DELETE_PAYLOAD_BYTES(Operation.DELETE, Field.PAYLOAD_BYTES),
        ENCODE_COMPLETED(Codec.ENCODE, Field.COMPLETED),
        ENCODE_BYTES(Codec.ENCODE, Field.BYTES),
        DECODE_COMPLETED(Codec.DECODE, Field.COMPLETED),
        DECODE_BYTES(Codec.DECODE, Field.BYTES);

        private final Operation operation;
        private final Codec codec;
        private final Field field;

        Kind(Operation operation, Field field) {
            this.operation = operation;
            this.codec = null;
            this.field = field;
        }

        Kind(Codec codec, Field field) {
            this.operation = null;
            this.codec = codec;
            this.field = field;
        }

        Kind(Field field) {
            this.operation = null;
            this.codec = null;
            this.field = field;
        }

        Operation operation() {
            return operation;
        }

        Codec codec() {
            return codec;
        }

        Field field() {
            return field;
        }

        long value(StateStoreCostStats.Reading reading) {
            if (field == Field.READY || field == Field.CLUSTER_SIZE) {
                return 0;
            }
            if (operation != null) {
                StateStoreCostStats.OperationReading measured = reading.operations().get(operation);
                if (measured == null) {
                    return 0;
                }
                return switch (field) {
                    case READY -> throw new IllegalStateException("readiness is not an operation cost");
                    case CLUSTER_SIZE -> throw new IllegalStateException("cluster size is not an operation cost");
                    case COMPLETED -> measured.completed();
                    case FAILED -> measured.failed();
                    case DURATION_NANOS -> measured.durationNanos();
                    case PAYLOAD_BYTES -> measured.payloadBytes();
                    case BYTES -> throw new IllegalStateException("operation cost has no codec bytes");
                };
            }
            StateStoreCostStats.CodecReading measured = reading.codecs().get(codec);
            if (measured == null) {
                return 0;
            }
            return field == Field.COMPLETED ? measured.completed() : measured.bytes();
        }
    }

    record Reading(Kind kind, String namespace) { }

    enum Field { READY, CLUSTER_SIZE, COMPLETED, FAILED, DURATION_NANOS, PAYLOAD_BYTES, BYTES }

    private static final Map<Operation, Kind[]> OPERATION_KINDS = new EnumMap<>(Operation.class);
    private static final Map<Codec, Kind[]> CODEC_KINDS = new EnumMap<>(Codec.class);

    static {
        for (Operation operation : Operation.values()) {
            OPERATION_KINDS.put(operation, Arrays.stream(Kind.values())
                    .filter(kind -> kind.operation == operation).toArray(Kind[]::new));
        }
        for (Codec codec : Codec.values()) {
            CODEC_KINDS.put(codec, Arrays.stream(Kind.values())
                    .filter(kind -> kind.codec == codec).toArray(Kind[]::new));
        }
    }

    private StateStoreCostMetricNames() {
    }

    static Kind[] forOperation(Operation operation) {
        return OPERATION_KINDS.get(operation);
    }

    static Kind[] forCodec(Codec codec) {
        return CODEC_KINDS.get(codec);
    }

    static String nameOf(Kind kind, String namespace) {
        return PREFIX + kind.name().toLowerCase(Locale.ROOT) + "." + namespace;
    }

    static Reading readingOf(String metric) {
        if (!metric.startsWith(PREFIX)) {
            return null;
        }
        int split = metric.indexOf('.', PREFIX.length());
        if (split < 0 || split == metric.length() - 1) {
            return null;
        }
        String kind = metric.substring(PREFIX.length(), split).toUpperCase(Locale.ROOT);
        try {
            return new Reading(Kind.valueOf(kind), metric.substring(split + 1));
        } catch (IllegalArgumentException unknown) {
            return null;
        }
    }
}
