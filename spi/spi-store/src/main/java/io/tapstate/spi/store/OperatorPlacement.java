package io.tapstate.spi.store;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** One immutable hot and cold operator-state address, independent of execution attempts. */
public record OperatorPlacement(PipelineIntentScope scope, String database, Resource resource,
        String stepId, String pathId, String namespace) implements Serializable {
    public enum Resource { NEST_STATE, JOIN_FACTS, JOIN_DIMENSIONS, JOIN_INDEXES, JOIN_WRITERS }

    public OperatorPlacement {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(resource, "resource");
        if (database == null || database.isBlank() || stepId == null || stepId.isBlank()
                || pathId == null || pathId.isBlank() || namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("operator placement requires complete coordinates");
        }
    }

    /** The map name is derived from every physical coordinate; identity is never parsed out of it. */
    public String mapName() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                text(out, scope.clusterId()); out.writeLong(scope.bindingVersion());
                text(out, scope.cacheIncarnation()); text(out, scope.pipelineId()); out.writeLong(scope.stateGeneration());
                text(out, database); text(out, resource.name()); text(out, stepId); text(out, pathId); text(out, namespace);
            }
            return (resource == Resource.NEST_STATE ? "nest." : "join.")
                    + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (IOException | NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static void text(DataOutputStream out, String value) throws IOException {
        out.writeInt(value.length()); out.writeChars(value);
    }
}
