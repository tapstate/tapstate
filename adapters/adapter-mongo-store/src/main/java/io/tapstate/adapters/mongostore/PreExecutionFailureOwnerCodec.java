package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PreExecutionFailure;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.util.Map;
import java.util.OptionalLong;
import org.bson.Document;

/** A bounded private owner discriminator; existing execution descriptors keep their original encoding. */
final class PreExecutionFailureOwnerCodec {
    static final String KIND = "ownerKind", REFUSAL = "PRE_EXECUTION_FAILURE", OWNER = "diagnosticOwner";
    static final int VERSION = 1;
    private PreExecutionFailureOwnerCodec() { }

    static Document encode(PreExecutionFailure.Owner owner) {
        Document frontier = owner.generationFrontier().isEmpty() ? new Document("presence", "ABSENT")
                : new Document("presence", "PRESENT").append("value", owner.generationFrontier().getAsLong());
        Document encoded = new Document("version", VERSION).append("pipelineId", owner.pipelineId())
                .append("clusterId", owner.clusterId()).append("pipelineIncarnationId", owner.pipelineIncarnationId())
                .append("checkpointEpoch", owner.checkpointEpoch()).append("checkpointDigest", owner.checkpointDigest())
                .append("desiredDigest", owner.desiredDigest()).append("artifactLineageDigest", owner.artifactLineageDigest())
                .append("generationFrontier", frontier);
        if (owner.writer() != null) {
            var writer = owner.writer();
            encoded.append("writer", new Document("clusterId", writer.key().clusterId())
                    .append("type", writer.key().type().name()).append("resourceId", writer.key().resourceId())
                    .append("nodeId", writer.owner().nodeId()).append("bootId", writer.owner().bootId())
                    .append("claimGeneration", writer.claimGeneration()).append("executionGeneration", writer.executionGeneration())
                    .append("topologyRevision", writer.topologyRevision()));
        }
        return encoded;
    }

    static PreExecutionFailure.Owner read(String pipelineId, Document descriptor) {
        try {
            if (!REFUSAL.equals(descriptor.get(KIND)) || descriptor.containsKey("executionGeneration")) {
                throw new IllegalArgumentException("invalid private refusal owner kind");
            }
            Document value = document(descriptor.get(OWNER));
            if (!(value.get("version") instanceof Integer version) || version != VERSION) {
                throw new IllegalArgumentException("unsupported private refusal owner version");
            }
            Document frontier = document(value.get("generationFrontier"));
            OptionalLong generation = switch (string(frontier.get("presence"))) {
                case "ABSENT" -> {
                    if (frontier.containsKey("value")) { throw new IllegalArgumentException("absent frontier has no number"); }
                    yield OptionalLong.empty();
                }
                case "PRESENT" -> OptionalLong.of(number(frontier.get("value")));
                default -> throw new IllegalArgumentException("unknown frontier presence");
            };
            WorkloadClaimFence writer = null;
            if (value.containsKey("writer")) {
                Document guard = document(value.get("writer"));
                writer = new WorkloadClaimFence(new WorkloadClaimKey(string(guard.get("clusterId")),
                        WorkloadClaimType.valueOf(string(guard.get("type"))), string(guard.get("resourceId"))),
                        new WorkloadOwner(string(guard.get("nodeId")), string(guard.get("bootId"))),
                        number(guard.get("claimGeneration")), number(guard.get("executionGeneration")),
                        number(guard.get("topologyRevision")));
            }
            var owner = new PreExecutionFailure.Owner(string(value.get("pipelineId")), string(value.get("clusterId")),
                    string(value.get("pipelineIncarnationId")), number(value.get("checkpointEpoch")),
                    string(value.get("checkpointDigest")), string(value.get("desiredDigest")),
                    string(value.get("artifactLineageDigest")), generation, writer);
            if (!pipelineId.equals(owner.pipelineId())
                    || !owner.pipelineIncarnationId().equals(descriptor.get("pipelineIncarnationId"))) {
                throw new IllegalArgumentException("private refusal owner belongs to another pipeline");
            }
            return owner;
        } catch (IllegalArgumentException | NullPointerException malformed) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", "latest diagnostic owner"), malformed);
        }
    }

    private static Document document(Object value) {
        if (!(value instanceof Document document)) { throw new IllegalArgumentException("expected an owner document"); }
        return document;
    }
    private static String string(Object value) {
        if (!(value instanceof String string) || string.isBlank()) { throw new IllegalArgumentException("expected an owner string"); }
        return string;
    }
    private static long number(Object value) {
        if (!(value instanceof Long || value instanceof Integer)) { throw new IllegalArgumentException("expected an integral owner number"); }
        return ((Number) value).longValue();
    }
}
