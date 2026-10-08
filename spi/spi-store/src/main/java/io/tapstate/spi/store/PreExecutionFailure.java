package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/** Private ownership of a real refusal that completed before a new execution was admitted. */
public final class PreExecutionFailure {
    private PreExecutionFailure() { }

    public record Owner(String pipelineId, String clusterId, String pipelineIncarnationId,
            long checkpointEpoch, String checkpointDigest, String desiredDigest, String artifactLineageDigest,
            OptionalLong generationFrontier, WorkloadClaimFence writer) implements ObservationStore.Owner {
        public Owner {
            required(pipelineId); required(clusterId); required(pipelineIncarnationId);
            digest(checkpointDigest); digest(desiredDigest); digest(artifactLineageDigest);
            Objects.requireNonNull(generationFrontier, "generationFrontier");
            if (checkpointEpoch < 1 || generationFrontier.isPresent() && generationFrontier.getAsLong() <= 0) {
                throw new IllegalArgumentException("a refusal owner requires a factual checkpoint and actual frontier");
            }
            if (writer != null && (!clusterId.equals(writer.key().clusterId())
                    || !pipelineId.equals(writer.key().resourceId())
                    || writer.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                    || writer.executionGeneration() != generationFrontier.orElse(0L))) {
                throw new IllegalArgumentException("a refusal writer must name its captured actuation authority");
            }
        }
    }

    /** Captured before validation from the exact artifacts used by that attempt; no later rebinding. */
    public record Attempt(String pipelineId, String clusterId, String pipelineIncarnationId,
            CheckpointDoc originalCheckpoint, DesiredState desired, Map<String, String> artifactHashes,
            OptionalLong generationFrontier, WorkloadClaimFence writer) {
        public Attempt {
            required(pipelineId); required(clusterId); required(pipelineIncarnationId);
            Objects.requireNonNull(originalCheckpoint, "originalCheckpoint");
            Objects.requireNonNull(desired, "desired");
            artifactHashes = Map.copyOf(Objects.requireNonNull(artifactHashes, "artifactHashes"));
            Objects.requireNonNull(generationFrontier, "generationFrontier");
            if (!pipelineId.equals(originalCheckpoint.pipelineId()) || !pipelineId.equals(desired.pipelineId())
                    || desired.targetState() != PipelineState.RUNNING || !artifactHashes.containsKey(pipelineId)) {
                throw new IllegalArgumentException("a refusal attempt belongs to its captured pipeline inputs");
            }
            artifactHashes.forEach((id, hash) -> { required(id); digest(hash); });
        }

        public Receipt failed(CheckpointDoc completed) {
            if (!pipelineId.equals(completed.pipelineId()) || StateJson.parse(completed.stateJson()) != PipelineState.FAILED
                    || completed.epoch() != Math.addExact(originalCheckpoint.epoch(), 1L)) {
                throw new IllegalArgumentException("a refusal receipt needs the original failed checkpoint transition");
            }
            return new Receipt(new Owner(pipelineId, clusterId, pipelineIncarnationId, completed.epoch(),
                    checkpointDigest(completed), desiredDigest(desired), lineageDigest(artifactHashes),
                    generationFrontier, writer), completed, desired, artifactHashes);
        }
    }

    /** Transient cold-write proof; persisted ownership retains digests rather than a second state ledger. */
    public record Receipt(Owner owner, CheckpointDoc checkpoint, DesiredState desired, Map<String, String> artifactHashes) {
        public Receipt {
            Objects.requireNonNull(owner, "owner"); Objects.requireNonNull(checkpoint, "checkpoint");
            Objects.requireNonNull(desired, "desired");
            artifactHashes = Map.copyOf(Objects.requireNonNull(artifactHashes, "artifactHashes"));
            if (!artifactHashes.containsKey(owner.pipelineId())) {
                throw new IllegalArgumentException("a refusal receipt must retain its pipeline artifact guard");
            }
            artifactHashes.forEach((id, hash) -> { required(id); digest(hash); });
            if (!owner.pipelineId().equals(checkpoint.pipelineId()) || !owner.pipelineId().equals(desired.pipelineId())
                    || desired.targetState() != PipelineState.RUNNING || owner.checkpointEpoch() != checkpoint.epoch()
                    || StateJson.parse(checkpoint.stateJson()) != PipelineState.FAILED
                    || !owner.checkpointDigest().equals(checkpointDigest(checkpoint))
                    || !owner.desiredDigest().equals(desiredDigest(desired))
                    || !owner.artifactLineageDigest().equals(lineageDigest(artifactHashes))) {
                throw new IllegalArgumentException("a refusal receipt must retain its complete original proof");
            }
        }
    }

    public static String checkpointDigest(CheckpointDoc checkpoint) {
        return hash("checkpoint", data -> {
            text(data, checkpoint.pipelineId()); data.writeLong(checkpoint.epoch()); text(data, checkpoint.stateJson());
        });
    }

    public static String desiredDigest(DesiredState desired) {
        return hash("desired", data -> {
            text(data, desired.pipelineId()); text(data, desired.targetState().name()); text(data, desired.revision());
            data.writeBoolean(desired.purgeState()); text(data, desired.assemblyRevision()); data.writeBoolean(desired.reassemble());
            data.writeBoolean(desired.rebuiltAtStateEpoch() != null);
            if (desired.rebuiltAtStateEpoch() != null) { data.writeLong(desired.rebuiltAtStateEpoch()); }
        });
    }

    public static String lineageDigest(Map<String, String> hashes) {
        return hash("artifact-lineage", data -> {
            data.writeLong(hashes.size());
            for (var entry : new TreeMap<>(hashes).entrySet()) { text(data, entry.getKey()); text(data, entry.getValue()); }
        });
    }

    @FunctionalInterface private interface Encoding { void write(DataOutputStream data) throws IOException; }
    private static String hash(String domain, Encoding encoding) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var data = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
                text(data, "tapstate/pre-execution-failure/" + domain + "/v1"); encoding.write(data);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException | IOException impossible) {
            throw new IllegalStateException("the required in-memory SHA-256 encoder is unavailable", impossible);
        }
    }
    private static void text(DataOutputStream data, String value) throws IOException {
        data.writeBoolean(value != null);
        if (value != null) { byte[] bytes = value.getBytes(StandardCharsets.UTF_8); data.writeLong(bytes.length); data.write(bytes); }
    }
    private static void required(String value) {
        if (Objects.requireNonNull(value, "identity").isBlank()) { throw new IllegalArgumentException("identity must not be blank"); }
    }
    private static void digest(String value) {
        if (!Objects.requireNonNull(value, "digest").matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("a refusal digest must be canonical SHA-256");
        }
    }
}
