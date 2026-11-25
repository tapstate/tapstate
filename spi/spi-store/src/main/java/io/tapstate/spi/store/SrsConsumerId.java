package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** One source node's progress identity, distinct from the pipeline's job and snapshot handoff identity. */
public record SrsConsumerId(String pipelineId, String sourceId) {

    private static final String PREFIX = "~srs-consumer:v1:";

    public SrsConsumerId {
        requireId(pipelineId, "pipelineId");
        requireId(sourceId, "sourceId");
    }

    public static SrsConsumerId of(String pipelineId, String sourceId) {
        return new SrsConsumerId(pipelineId, sourceId);
    }

    /** Length-delimited coordinates cannot collide when either id itself contains a delimiter. */
    public String value() {
        return PREFIX + pipelineId.length() + ":" + pipelineId + sourceId.length() + ":" + sourceId;
    }

    /** The owning pipeline, including for a legacy record keyed by its pipeline id alone. */
    public static String pipelineOf(String consumerId) {
        return decode(consumerId).map(SrsConsumerId::pipelineId).orElse(consumerId);
    }

    /** Empty for legacy pipeline-only progress, whose source node cannot be inferred. */
    public static Optional<String> sourceOf(String consumerId) {
        return decode(consumerId).map(SrsConsumerId::sourceId);
    }

    public static boolean belongsTo(String consumerId, String pipelineId) {
        return pipelineOf(consumerId).equals(Objects.requireNonNull(pipelineId, "pipelineId"));
    }

    private static Optional<SrsConsumerId> decode(String consumerId) {
        requireId(consumerId, "consumerId");
        if (!consumerId.startsWith(PREFIX)) {
            return Optional.empty();
        }
        int pipelineEnd = componentEnd(consumerId, PREFIX.length());
        int pipelineStart = consumerId.indexOf(':', PREFIX.length()) + 1;
        int sourceEnd = componentEnd(consumerId, pipelineEnd);
        int sourceStart = consumerId.indexOf(':', pipelineEnd) + 1;
        if (sourceEnd != consumerId.length()) {
            throw new IllegalArgumentException("source-scoped consumer id has trailing characters");
        }
        return Optional.of(new SrsConsumerId(
                consumerId.substring(pipelineStart, pipelineEnd),
                consumerId.substring(sourceStart, sourceEnd)));
    }

    private static int componentEnd(String value, int start) {
        int separator = value.indexOf(':', start);
        if (separator == start || separator < 0) {
            throw new IllegalArgumentException("source-scoped consumer id has no component length");
        }
        String rawLength = value.substring(start, separator);
        if (!rawLength.chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException("source-scoped consumer id has an invalid component length");
        }
        final int length;
        try {
            length = Integer.parseInt(rawLength);
        } catch (NumberFormatException invalidLength) {
            throw new IllegalArgumentException("source-scoped consumer id has an invalid component length", invalidLength);
        }
        if (length <= 0 || length > value.length() - separator - 1) {
            throw new IllegalArgumentException("source-scoped consumer id has an invalid component boundary");
        }
        return separator + 1 + length;
    }

    private static void requireId(String id, String name) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
    }
}
