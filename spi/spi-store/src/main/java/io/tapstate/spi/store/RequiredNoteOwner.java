package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineNode;
import java.util.Objects;
import java.util.Optional;

/** Immutable logical identity of necessary connector notes inside one storage binding. */
public record RequiredNoteOwner(long bindingVersion, String namespace, Kind kind, String ownerId,
        long stateGeneration) {
    public enum Kind { PIPELINE, CHAIN, GLOBAL }

    public static String namespace(PipelineNode node, PipelineNode.Purpose purpose) {
        return node == null ? null : "pdk.state." + node.physicalAddress(purpose);
    }

    public static RequiredNoteOwner forNode(long bindingVersion, PipelineNode node,
            PipelineNode.Purpose purpose, long stateGeneration) {
        Objects.requireNonNull(node, "node");
        return new RequiredNoteOwner(bindingVersion, namespace(node, purpose), Kind.PIPELINE,
                node.pipelineId(), stateGeneration);
    }

    public static RequiredNoteOwner forArtifact(long bindingVersion, PipelineNode node,
            PipelineNode.Purpose purpose, long stateGeneration, String sha256, long intentRevision) {
        Objects.requireNonNull(node, "node");
        if (sha256 == null || !sha256.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("invalid note artifact hash");
        return new RequiredNoteOwner(bindingVersion, namespace(node, purpose) + ".artifact-" + sha256 + "-" + intentRevision,
                Kind.PIPELINE, node.pipelineId(), stateGeneration);
    }

    /** Whether this private identity belongs to the fixed connector purpose. */
    public boolean hasPurpose(PipelineNode.Purpose purpose) {
        return kind == Kind.PIPELINE && namespace.startsWith(pipelinePrefix(ownerId, purpose));
    }

    public Optional<PipelineNode> pipelineNode(PipelineNode.Purpose purpose) {
        return hasPurpose(purpose)
                ? Optional.of(new PipelineNode(ownerId,
                        namespace.substring(pipelinePrefix(ownerId, purpose).length())))
                : Optional.empty();
    }

    private static String pipelinePrefix(String ownerId, PipelineNode.Purpose purpose) {
        return "pdk.state." + ownerId + "." + Objects.requireNonNull(purpose, "purpose").segment() + ".";
    }

    public RequiredNoteOwner {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(ownerId, "ownerId");
        if (bindingVersion < 1 || namespace.isBlank() || ownerId.isBlank()
                || (kind == Kind.PIPELINE ? stateGeneration < 1 : stateGeneration != 0)) {
            throw new IllegalArgumentException("invalid required-note owner");
        }
        boolean validNamespace = switch (kind) {
            case PIPELINE -> java.util.Arrays.stream(PipelineNode.Purpose.values()).anyMatch(purpose -> {
                String prefix = pipelinePrefix(ownerId, purpose);
                return namespace.startsWith(prefix) && namespace.length() > prefix.length();
            });
            case CHAIN -> namespace.equals("pdk.chain." + ownerId);
            case GLOBAL -> namespace.equals("pdk.global-state");
        };
        if (!validNamespace) throw new IllegalArgumentException("namespace does not belong to required-note owner");
    }
}
