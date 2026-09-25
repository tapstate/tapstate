package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;

import java.util.Map;
import java.util.Objects;

/** One resource write and the atomic condition that must hold for it to take effect. */
public record ArtifactWrite(Resource resource, Intent intent, String expectedContentHash,
        Map<String, String> readPreconditions, String pipelineIncarnationCandidate) {

    /** The condition evaluated by the store together with the resource write. */
    public enum Intent {
        CREATE_ONLY,
        REPLACE_ONLY,
        UPSERT
    }

    public ArtifactWrite {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(intent, "intent");
        readPreconditions = Map.copyOf(readPreconditions);
        if (intent == Intent.REPLACE_ONLY && expectedContentHash == null) {
            throw new IllegalArgumentException("replace-only writes require an expected content hash");
        }
        if (intent != Intent.REPLACE_ONLY && expectedContentHash != null) {
            throw new IllegalArgumentException("only replace-only writes may declare an expected content hash");
        }
        if (pipelineIncarnationCandidate != null && !(resource instanceof PipelineResource)) {
            throw new IllegalArgumentException("only pipeline writes may carry an incarnation candidate");
        }
    }

    /** Creates a write that succeeds only while no artifact uses this resource id. */
    public static ArtifactWrite createOnly(Resource resource) {
        return new ArtifactWrite(resource, Intent.CREATE_ONLY, null, Map.of(), null);
    }

    /** Creates a write that succeeds only while this resource id still holds {@code expectedContentHash}. */
    public static ArtifactWrite replaceOnly(Resource resource, String expectedContentHash) {
        return new ArtifactWrite(resource, Intent.REPLACE_ONLY, expectedContentHash, Map.of(), null);
    }

    /** Creates an unconditional apply write for CLI batch semantics. */
    public static ArtifactWrite upsert(Resource resource) {
        return new ArtifactWrite(resource, Intent.UPSERT, null, Map.of(), null);
    }

    public ArtifactWrite guardedBy(Map<String, String> preconditions) {
        return new ArtifactWrite(resource, intent, expectedContentHash, preconditions,
                pipelineIncarnationCandidate);
    }

    /** Supplies an identity for a newly inserted pipeline; replacing one never changes its identity. */
    public ArtifactWrite withPipelineIncarnationCandidate(String candidate) {
        return new ArtifactWrite(resource, intent, expectedContentHash, readPreconditions,
                Objects.requireNonNull(candidate, "candidate"));
    }
}
