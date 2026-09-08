package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** One immutable, shared projection rendered by every workbench tab. */
record WorkbenchSnapshot(
        long contextGeneration,
        long requestSequence,
        WorkbenchSessionSnapshot session,
        WorkbenchOverviewSnapshot overview,
        WorkbenchWorkspaceSnapshot workspace,
        WorkbenchResourceListSnapshot sources,
        WorkbenchResourceListSnapshot pipelines) {

    WorkbenchSnapshot {
        if (contextGeneration < 0) {
            throw new IllegalArgumentException("Context generation must not be negative");
        }
        if (requestSequence <= 0) {
            throw new IllegalArgumentException("Request sequence must be positive");
        }
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(overview, "overview");
        Objects.requireNonNull(workspace, "workspace");
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(pipelines, "pipelines");
    }

    /** Compatibility identity used by the refresh runtime until its data source is wired. */
    WorkbenchSnapshot(long contextGeneration, long requestSequence) {
        this(
                contextGeneration,
                requestSequence,
                WorkbenchSessionSnapshot.empty(),
                WorkbenchOverviewSnapshot.empty(),
                WorkbenchWorkspaceSnapshot.empty(),
                WorkbenchResourceListSnapshot.empty("source"),
                WorkbenchResourceListSnapshot.empty("pipeline"));
    }

    Identity identity() {
        return new Identity(contextGeneration, requestSequence);
    }

    record Identity(long contextGeneration, long requestSequence) {
        Identity {
            if (contextGeneration < 0) {
                throw new IllegalArgumentException("Context generation must not be negative");
            }
            if (requestSequence <= 0) {
                throw new IllegalArgumentException("Request sequence must be positive");
            }
        }
    }
}

/** Sanitized session metadata; credentials and transport failures never enter the snapshot. */
record WorkbenchSessionSnapshot(
        Path workspaceRoot,
        Optional<String> contextName,
        Optional<ResolvedContext.Source> contextSource,
        WorkbenchConnection connection,
        WorkbenchAuthentication authentication,
        Optional<String> principal,
        Optional<URI> landingNode,
        String versions) {

    WorkbenchSessionSnapshot {
        Objects.requireNonNull(workspaceRoot, "workspaceRoot");
        Objects.requireNonNull(contextName, "contextName");
        Objects.requireNonNull(contextSource, "contextSource");
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(authentication, "authentication");
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(landingNode, "landingNode");
        Objects.requireNonNull(versions, "versions");
        landingNode = landingNode.map(WorkbenchSessionSnapshot::sanitizeLandingNode);
    }

    static WorkbenchSessionSnapshot empty() {
        return new WorkbenchSessionSnapshot(
                Path.of("."),
                Optional.empty(),
                Optional.empty(),
                WorkbenchConnection.NO_CONTEXT,
                WorkbenchAuthentication.NOT_APPLICABLE,
                Optional.empty(),
                Optional.empty(),
                "");
    }

    private static URI sanitizeLandingNode(URI endpoint) {
        Objects.requireNonNull(endpoint, "landing endpoint");
        if (endpoint.isOpaque() || endpoint.getScheme() == null || endpoint.getHost() == null) {
            throw new IllegalArgumentException("Landing endpoint must have a scheme and host");
        }
        try {
            return new URI(
                    endpoint.getScheme(),
                    null,
                    endpoint.getHost(),
                    endpoint.getPort(),
                    null,
                    null,
                    null);
        } catch (URISyntaxException invalidEndpoint) {
            throw new IllegalArgumentException("Landing endpoint cannot be sanitized", invalidEndpoint);
        }
    }
}

enum WorkbenchConnection {
    NO_CONTEXT,
    OFFLINE,
    CONNECTED
}

enum WorkbenchAuthentication {
    NOT_APPLICABLE,
    SIGNED_OUT,
    SIGNED_IN,
    MACHINE
}

/** Whether absence in the remote artifact collection has actually been established. */
sealed interface WorkbenchRemoteState
        permits WorkbenchRemoteState.Available,
                WorkbenchRemoteState.NotConfigured,
                WorkbenchRemoteState.SignedOut,
                WorkbenchRemoteState.Offline,
                WorkbenchRemoteState.Rejected,
                WorkbenchRemoteState.Diagnostic {

    record Available(int artifactCount) implements WorkbenchRemoteState {
        public Available {
            if (artifactCount < 0) {
                throw new IllegalArgumentException("Artifact count must not be negative");
            }
        }
    }

    record NotConfigured() implements WorkbenchRemoteState {
    }

    record SignedOut() implements WorkbenchRemoteState {
    }

    record Offline() implements WorkbenchRemoteState {
    }

    record Rejected(String code, String message) implements WorkbenchRemoteState {
        public Rejected {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(message, "message");
        }
    }

    record Diagnostic(TapstateErrorCode code, Map<String, String> arguments) implements WorkbenchRemoteState {
        public Diagnostic {
            Objects.requireNonNull(code, "code");
            arguments = Map.copyOf(arguments);
        }
    }
}

record WorkbenchOverviewSnapshot(
        List<WorkbenchKindCount> kinds,
        WorkbenchAlignmentCounts alignment) {

    WorkbenchOverviewSnapshot {
        kinds = List.copyOf(kinds);
        Objects.requireNonNull(alignment, "alignment");
    }

    static WorkbenchOverviewSnapshot empty() {
        return new WorkbenchOverviewSnapshot(
                WorkbenchProjection.VISIBLE_KINDS.stream()
                        .map(kind -> new WorkbenchKindCount(kind, 0, OptionalInt.empty()))
                        .toList(),
                new WorkbenchAlignmentCounts(0, 0, 0, 0, 0, 0));
    }
}

record WorkbenchKindCount(String kind, int localCount, OptionalInt remoteCount) {
    WorkbenchKindCount {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(remoteCount, "remoteCount");
        if (localCount < 0 || remoteCount.orElse(0) < 0) {
            throw new IllegalArgumentException("Kind counts must not be negative");
        }
    }
}

record WorkbenchAlignmentCounts(
        int localOnly,
        int remoteOnly,
        int inSync,
        int drifted,
        int invalidLocal,
        int unknown) {

    WorkbenchAlignmentCounts {
        if (localOnly < 0 || remoteOnly < 0 || inSync < 0 || drifted < 0
                || invalidLocal < 0 || unknown < 0) {
            throw new IllegalArgumentException("Alignment counts must not be negative");
        }
    }
}

record WorkbenchWorkspaceSnapshot(
        WorkbenchRemoteState remoteState,
        List<WorkbenchArtifactRow> rows) {

    WorkbenchWorkspaceSnapshot {
        Objects.requireNonNull(remoteState, "remoteState");
        rows = List.copyOf(rows);
    }

    static WorkbenchWorkspaceSnapshot empty() {
        return new WorkbenchWorkspaceSnapshot(new WorkbenchRemoteState.NotConfigured(), List.of());
    }
}

record WorkbenchResourceListSnapshot(
        String kind,
        WorkbenchRemoteState remoteState,
        List<WorkbenchArtifactRow> rows) {

    WorkbenchResourceListSnapshot {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(remoteState, "remoteState");
        rows = List.copyOf(rows);
        if (rows.stream().anyMatch(row -> !row.key().kind().equals(kind))) {
            throw new IllegalArgumentException("Resource list rows must match its kind");
        }
    }

    static WorkbenchResourceListSnapshot empty(String kind) {
        return new WorkbenchResourceListSnapshot(
                kind, new WorkbenchRemoteState.NotConfigured(), List.of());
    }
}

record WorkbenchArtifactKey(String kind, String id) {
    WorkbenchArtifactKey {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(id, "id");
        if (kind.isBlank() || id.isBlank()) {
            throw new IllegalArgumentException("Artifact identity must not be blank");
        }
    }
}

record WorkbenchArtifactRow(
        WorkbenchArtifactKey key,
        List<WorkbenchLocalArtifact> local,
        List<WorkbenchRemoteArtifact> remote,
        WorkbenchAlignment alignment) {

    WorkbenchArtifactRow {
        Objects.requireNonNull(key, "key");
        local = List.copyOf(local);
        remote = List.copyOf(remote);
        Objects.requireNonNull(alignment, "alignment");
    }
}

record WorkbenchLocalArtifact(
        Path relativePath,
        Optional<String> declaredKind,
        boolean readable,
        boolean misplaced,
        boolean comparable) {

    WorkbenchLocalArtifact {
        Objects.requireNonNull(relativePath, "relativePath");
        Objects.requireNonNull(declaredKind, "declaredKind");
    }

    boolean valid() {
        return readable && !misplaced && comparable;
    }
}

record WorkbenchRemoteArtifact(boolean readable, boolean comparable) {
}

enum WorkbenchAlignment {
    LOCAL_ONLY,
    REMOTE_ONLY,
    IN_SYNC,
    DRIFTED,
    INVALID_LOCAL,
    UNKNOWN
}
