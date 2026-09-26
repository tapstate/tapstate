package io.tapstate.core.lifecycle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * One retained, best-effort lifecycle or telemetry event. Its incarnation and execution owner are
 * internal storage fields; a public read face projects only the diagnostic fields it is allowed to show.
 */
public record PipelineEvent(
        String id,
        String pipelineId,
        String pipelineIncarnationId,
        long executionGeneration,
        Kind kind,
        Instant occurredAt,
        PipelineState beforeState,
        PipelineState afterState,
        ObservationFailure failure,
        String reason,
        Gap gap) {

    public enum Kind {
        STATE_CHANGED,
        FAILURE,
        EXECUTION_RESTARTED,
        EXECUTION_RECOVERED,
        TELEMETRY_DEGRADED,
        TELEMETRY_RESTORED,
        CLEANUP_INCOMPLETE,
        TELEMETRY_GAP
    }

    public enum GapReason {
        QUEUE_FULL,
        WRITE_FAILURE,
        SHUTDOWN
    }

    /** The known interval and cause set of lost events, carried only by a gap marker. */
    public record Gap(Instant from, Instant to, List<GapReason> reasons) {
        public Gap {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            from = from.truncatedTo(ChronoUnit.MILLIS);
            to = to.truncatedTo(ChronoUnit.MILLIS);
            Objects.requireNonNull(reasons, "reasons");
            if (to.isBefore(from) || reasons.isEmpty()) {
                throw new IllegalArgumentException("a telemetry gap needs an ordered interval and at least one reason");
            }
            // A closed enum set gives one stable order and removes duplicate reports of the same cause.
            reasons = List.copyOf(EnumSet.copyOf(reasons));
        }
    }

    public PipelineEvent {
        id = nonBlank(id, "id");
        pipelineId = nonBlank(pipelineId, "pipelineId");
        pipelineIncarnationId = nonBlank(pipelineIncarnationId, "pipelineIncarnationId");
        if (executionGeneration <= 0) {
            throw new IllegalArgumentException("an event execution generation is positive");
        }
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(occurredAt, "occurredAt");
        occurredAt = occurredAt.truncatedTo(ChronoUnit.MILLIS);
        if ((kind == Kind.TELEMETRY_GAP) != (gap != null)) {
            throw new IllegalArgumentException("a telemetry gap marker alone carries a gap interval");
        }
        if (kind == Kind.FAILURE && failure == null) {
            throw new IllegalArgumentException("a failure event carries its coded failure");
        }
        if (failure != null) {
            nonBlank(failure.code(), "failure code");
        }
        if (reason != null) {
            nonBlank(reason, "reason");
        }
    }

    /** A stable opaque identity for retries of one gap marker within one execution. */
    public static String gapId(String pipelineId, String pipelineIncarnationId,
            long executionGeneration, Instant gapFrom) {
        nonBlank(pipelineId, "pipelineId");
        nonBlank(pipelineIncarnationId, "pipelineIncarnationId");
        if (executionGeneration <= 0) {
            throw new IllegalArgumentException("an event execution generation is positive");
        }
        Objects.requireNonNull(gapFrom, "gapFrom");
        gapFrom = gapFrom.truncatedTo(ChronoUnit.MILLIS);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            add(digest, pipelineId);
            add(digest, pipelineIncarnationId);
            add(digest, Long.toString(executionGeneration));
            add(digest, gapFrom.toString());
            return "gap-" + java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("the runtime has no SHA-256 digest", unavailable);
        }
    }

    private static void add(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static String nonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is not blank");
        }
        return value;
    }
}
