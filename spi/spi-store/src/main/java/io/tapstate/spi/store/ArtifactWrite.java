package io.tapstate.spi.store;

import io.tapstate.core.model.Resource;

import java.util.Objects;

/** One resource write and the atomic condition that must hold for it to take effect. */
public record ArtifactWrite(Resource resource, Intent intent, String expectedContentHash) {

    /** The condition evaluated by the store together with the resource write. */
    public enum Intent {
        CREATE_ONLY,
        REPLACE_ONLY,
        UPSERT
    }

    public ArtifactWrite {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(intent, "intent");
        if (intent == Intent.REPLACE_ONLY && expectedContentHash == null) {
            throw new IllegalArgumentException("replace-only writes require an expected content hash");
        }
        if (intent != Intent.REPLACE_ONLY && expectedContentHash != null) {
            throw new IllegalArgumentException("only replace-only writes may declare an expected content hash");
        }
    }

    /** Creates a write that succeeds only while no artifact uses this resource id. */
    public static ArtifactWrite createOnly(Resource resource) {
        return new ArtifactWrite(resource, Intent.CREATE_ONLY, null);
    }

    /** Creates a write that succeeds only while this resource id still holds {@code expectedContentHash}. */
    public static ArtifactWrite replaceOnly(Resource resource, String expectedContentHash) {
        return new ArtifactWrite(resource, Intent.REPLACE_ONLY, expectedContentHash);
    }

    /** Creates an unconditional apply write for CLI batch semantics. */
    public static ArtifactWrite upsert(Resource resource) {
        return new ArtifactWrite(resource, Intent.UPSERT, null);
    }
}
