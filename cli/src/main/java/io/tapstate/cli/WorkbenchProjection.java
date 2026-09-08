package io.tapstate.cli;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** Pure local/remote projection for one workbench refresh. */
final class WorkbenchProjection {

    private WorkbenchProjection() {
    }

    static WorkbenchSnapshot project(
            long contextGeneration,
            long requestSequence,
            WorkbenchSessionSnapshot session,
            List<WorkspaceScan.Artifact> localArtifacts,
            WorkbenchRemoteState remoteState,
            List<RemoteArtifact> remoteArtifacts) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(localArtifacts, "localArtifacts");
        Objects.requireNonNull(remoteState, "remoteState");
        Objects.requireNonNull(remoteArtifacts, "remoteArtifacts");

        List<WorkspaceScan.Artifact> localCopy = List.copyOf(localArtifacts);
        List<RemoteArtifact> remoteCopy = List.copyOf(remoteArtifacts);
        if (!(remoteState instanceof WorkbenchRemoteState.Available) && !remoteCopy.isEmpty()) {
            throw new IllegalArgumentException("Remote artifacts require an available remote listing");
        }
        if (remoteState instanceof WorkbenchRemoteState.Available available
                && available.artifactCount() != remoteCopy.size()) {
            throw new IllegalArgumentException("Available artifact count must match the remote listing");
        }

        Map<WorkbenchArtifactKey, MutableRow> merged = new LinkedHashMap<>();
        CanonicalWriter writer = new CanonicalWriter();
        for (WorkspaceScan.Artifact artifact : localCopy) {
            Objects.requireNonNull(artifact, "local artifact");
            WorkbenchArtifactKey key = new WorkbenchArtifactKey(artifact.kind(), artifact.id());
            merged.computeIfAbsent(key, MutableRow::new)
                    .local.add(projectLocal(session.workspaceRoot(), artifact, writer));
        }
        for (RemoteArtifact artifact : remoteCopy) {
            Objects.requireNonNull(artifact, "remote artifact");
            WorkbenchArtifactKey key = new WorkbenchArtifactKey(artifact.kind(), artifact.id());
            merged.computeIfAbsent(key, MutableRow::new)
                    .remote.add(projectRemote(artifact));
        }

        List<WorkbenchArtifactRow> rows = merged.values().stream()
                .map(row -> row.freeze(remoteState instanceof WorkbenchRemoteState.Available))
                .sorted(rowOrder())
                .toList();
        WorkbenchWorkspaceSnapshot workspace = new WorkbenchWorkspaceSnapshot(remoteState, rows);
        WorkbenchResourceListSnapshot sources = resourceList("source", remoteState, rows);
        WorkbenchResourceListSnapshot pipelines = resourceList("pipeline", remoteState, rows);

        return new WorkbenchSnapshot(
                contextGeneration,
                requestSequence,
                session,
                overview(rows, remoteState instanceof WorkbenchRemoteState.Available, remoteCopy),
                workspace,
                sources,
                pipelines);
    }

    private static ProjectedLocal projectLocal(
            Path workspaceRoot,
            WorkspaceScan.Artifact artifact,
            CanonicalWriter writer) {
        Resource resource = artifact.resource();
        Optional<String> declaredKind = resource == null
                ? Optional.empty()
                : Optional.of(resource.kind());
        String canonical = null;
        if (resource != null) {
            try {
                canonical = writer.write(resource);
            } catch (RuntimeException unwritable) {
                canonical = null;
            }
        }
        return new ProjectedLocal(
                new WorkbenchLocalArtifact(
                        relativePath(workspaceRoot, artifact.file()),
                        declaredKind,
                        resource != null,
                        artifact.misplaced(),
                        canonical != null),
                canonical);
    }

    private static ProjectedRemote projectRemote(RemoteArtifact artifact) {
        String canonical = artifact.canonicalForm();
        return new ProjectedRemote(
                new WorkbenchRemoteArtifact(artifact.readable(), artifact.readable() && canonical != null),
                canonical);
    }

    private static Path relativePath(Path workspaceRoot, Path file) {
        Path root = workspaceRoot.toAbsolutePath().normalize();
        Path absoluteFile = file.toAbsolutePath().normalize();
        return absoluteFile.startsWith(root)
                ? root.relativize(absoluteFile)
                : file.getFileName();
    }

    private static Comparator<WorkbenchArtifactRow> rowOrder() {
        return Comparator
                .comparingInt((WorkbenchArtifactRow row) -> kindOrder(row.key().kind()))
                .thenComparing(row -> row.key().kind())
                .thenComparing(row -> row.key().id())
                .thenComparing(row -> row.local().stream()
                        .map(local -> local.relativePath().toString())
                        .min(String::compareTo)
                        .orElse(""));
    }

    private static int kindOrder(String kind) {
        int index = WorkspaceScan.KINDS.indexOf(kind);
        return index >= 0 ? index : WorkspaceScan.KINDS.size();
    }

    private static WorkbenchResourceListSnapshot resourceList(
            String kind,
            WorkbenchRemoteState remoteState,
            List<WorkbenchArtifactRow> rows) {
        return new WorkbenchResourceListSnapshot(
                kind,
                remoteState,
                rows.stream().filter(row -> row.key().kind().equals(kind)).toList());
    }

    private static WorkbenchOverviewSnapshot overview(
            List<WorkbenchArtifactRow> rows,
            boolean remoteAvailable,
            List<RemoteArtifact> remoteArtifacts) {
        List<String> orderedKinds = new ArrayList<>(WorkspaceScan.KINDS);
        rows.stream()
                .map(row -> row.key().kind())
                .filter(kind -> !WorkspaceScan.KINDS.contains(kind))
                .distinct()
                .sorted()
                .forEach(orderedKinds::add);
        List<WorkbenchKindCount> kinds = orderedKinds.stream()
                .map(kind -> new WorkbenchKindCount(
                        kind,
                        count(rows, kind, true),
                        remoteAvailable
                                ? OptionalInt.of(count(remoteArtifacts, kind))
                                : OptionalInt.empty()))
                .toList();
        return new WorkbenchOverviewSnapshot(kinds, new WorkbenchAlignmentCounts(
                count(rows, WorkbenchAlignment.LOCAL_ONLY),
                count(rows, WorkbenchAlignment.REMOTE_ONLY),
                count(rows, WorkbenchAlignment.IN_SYNC),
                count(rows, WorkbenchAlignment.DRIFTED),
                count(rows, WorkbenchAlignment.INVALID_LOCAL),
                count(rows, WorkbenchAlignment.UNKNOWN)));
    }

    private static int count(List<WorkbenchArtifactRow> rows, String kind, boolean local) {
        return (int) rows.stream()
                .filter(row -> row.key().kind().equals(kind))
                .filter(row -> local ? !row.local().isEmpty() : !row.remote().isEmpty())
                .count();
    }

    private static int count(List<RemoteArtifact> artifacts, String kind) {
        return (int) artifacts.stream().filter(artifact -> artifact.kind().equals(kind)).count();
    }

    private static int count(List<WorkbenchArtifactRow> rows, WorkbenchAlignment alignment) {
        return (int) rows.stream().filter(row -> row.alignment() == alignment).count();
    }

    private static final class MutableRow {
        private final WorkbenchArtifactKey key;
        private final List<ProjectedLocal> local = new ArrayList<>();
        private final List<ProjectedRemote> remote = new ArrayList<>();

        private MutableRow(WorkbenchArtifactKey key) {
            this.key = key;
        }

        private WorkbenchArtifactRow freeze(boolean remoteAvailable) {
            local.sort(Comparator.comparing(value -> value.metadata().relativePath().toString()));
            WorkbenchAlignment alignment;
            if (local.size() > 1 || local.stream().anyMatch(value -> !value.metadata().valid())) {
                alignment = WorkbenchAlignment.INVALID_LOCAL;
            } else if (!remoteAvailable) {
                alignment = WorkbenchAlignment.UNKNOWN;
            } else if (remote.size() > 1 || remote.stream().anyMatch(value -> !value.metadata().comparable())) {
                alignment = WorkbenchAlignment.UNKNOWN;
            } else if (local.isEmpty()) {
                alignment = WorkbenchAlignment.REMOTE_ONLY;
            } else if (remote.isEmpty()) {
                alignment = WorkbenchAlignment.LOCAL_ONLY;
            } else if (local.getFirst().canonical().equals(remote.getFirst().canonical())) {
                alignment = WorkbenchAlignment.IN_SYNC;
            } else {
                alignment = WorkbenchAlignment.DRIFTED;
            }
            return new WorkbenchArtifactRow(
                    key,
                    local.stream().map(ProjectedLocal::metadata).toList(),
                    remote.stream().map(ProjectedRemote::metadata).toList(),
                    alignment);
        }
    }

    /** Holds sensitive comparison text only within the projection call. */
    private record ProjectedLocal(WorkbenchLocalArtifact metadata, String canonical) {
    }

    /** Holds sensitive comparison text only within the projection call. */
    private record ProjectedRemote(WorkbenchRemoteArtifact metadata, String canonical) {
    }
}
