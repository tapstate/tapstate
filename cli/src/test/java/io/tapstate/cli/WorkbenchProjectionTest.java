package io.tapstate.cli;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkbenchProjectionTest {

    private static final Path ROOT = Path.of("/workspace");

    @Test
    void classifiesExactCanonicalEqualityAndDifferenceWithoutNormalization() {
        WorkspaceScan.Artifact equal = artifact("source", "equal.tap.yml", source("equal"));
        WorkspaceScan.Artifact drifted = artifact("source", "drifted.tap.yml", source("drifted"));
        String equalCanonical = canonical(equal.resource());
        String driftedCanonical = canonical(drifted.resource());

        WorkbenchSnapshot snapshot = project(
                List.of(equal, drifted),
                new WorkbenchRemoteState.Available(2),
                List.of(
                        new RemoteArtifact("equal", "source", equalCanonical),
                        new RemoteArtifact("drifted", "source", driftedCanonical + " ")));

        assertThat(workspaceRow(snapshot, "source", "equal").alignment()).isEqualTo(WorkbenchAlignment.IN_SYNC);
        assertThat(workspaceRow(snapshot, "source", "drifted").alignment()).isEqualTo(WorkbenchAlignment.DRIFTED);
    }

    @Test
    void comparesSecretBearingCanonicalFormsWithoutRetainingTheirText() {
        String secret = "never-retain-password-token-7d9f";
        WorkspaceScan.Artifact local = artifact("source", "secured.tap.yml", parse("""
                version: tapstate/v1
                kind: source
                id: secured
                connector: mysql
                config:
                  password: %s
                  apiToken: %s
                """.formatted(secret, secret)));

        WorkbenchSnapshot snapshot = project(
                List.of(local),
                new WorkbenchRemoteState.Available(1),
                List.of(new RemoteArtifact("secured", "source", canonical(local.resource()))));

        assertThat(workspaceRow(snapshot, "source", "secured").alignment()).isEqualTo(WorkbenchAlignment.IN_SYNC);
        assertThat(snapshot.toString()).doesNotContain(secret);
        assertThat(retainedStrings(snapshot)).noneMatch(value -> value.contains(secret));
    }

    @Test
    void unknownRemoteContentCannotBeClaimedAsComparableOrRemoteOnly() {
        WorkspaceScan.Artifact local = artifact("source", "local.tap.yml", source("local"));

        WorkbenchSnapshot snapshot = project(
                List.of(local),
                new WorkbenchRemoteState.Available(3),
                List.of(
                        new RemoteArtifact("local", "source", null, false),
                        new RemoteArtifact("unreadable", "source", "ignored", false),
                        new RemoteArtifact("null-canonical", "source", null, true)));

        assertThat(workspaceRow(snapshot, "source", "local").alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
        assertThat(serverRow(snapshot.sources(), "unreadable").alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
        assertThat(serverRow(snapshot.sources(), "null-canonical").alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
    }

    @Test
    void invalidLocalStateWinsForMalformedMisplacedAndDuplicateArtifacts() {
        WorkspaceScan.Artifact malformed = new WorkspaceScan.Artifact(
                "source", ROOT.resolve("source/broken.tap.yml"), null);
        WorkspaceScan.Artifact misplaced = artifact("source", "misplaced.tap.yml", pipeline("misplaced"));
        WorkspaceScan.Artifact duplicateOne = artifact("source", "duplicate-a.tap.yml", source("duplicate"));
        WorkspaceScan.Artifact duplicateTwo = artifact("source", "duplicate-b.tap.yml", source("duplicate"));

        WorkbenchSnapshot snapshot = project(
                List.of(duplicateTwo, misplaced, malformed, duplicateOne),
                new WorkbenchRemoteState.Available(3),
                List.of(
                        new RemoteArtifact("broken", "source", "anything"),
                        new RemoteArtifact("misplaced", "source", "anything"),
                        new RemoteArtifact("duplicate", "source", "anything")));

        assertThat(workspaceRow(snapshot, "source", "broken").alignment()).isEqualTo(WorkbenchAlignment.INVALID_LOCAL);
        assertThat(workspaceRow(snapshot, "source", "misplaced").alignment()).isEqualTo(WorkbenchAlignment.INVALID_LOCAL);
        assertThat(workspaceRow(snapshot, "source", "duplicate").alignment()).isEqualTo(WorkbenchAlignment.INVALID_LOCAL);
        assertThat(workspaceRow(snapshot, "source", "duplicate").local()).hasSize(2);
    }

    @Test
    void duplicateRemoteIdentityIsUnknown() {
        WorkspaceScan.Artifact local = artifact("pipeline", "orders.tap.yml", pipeline("orders"));
        String canonical = canonical(local.resource());

        WorkbenchSnapshot snapshot = project(
                List.of(local),
                new WorkbenchRemoteState.Available(2),
                List.of(
                        new RemoteArtifact("orders", "pipeline", canonical),
                        new RemoteArtifact("orders", "pipeline", canonical)));

        WorkbenchArtifactRow row = workspaceRow(snapshot, "pipeline", "orders");
        assertThat(row.alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
        assertThat(row.remote()).hasSize(2);
    }

    @Test
    void sameIdInDifferentKindsNeverPairs() {
        WorkspaceScan.Artifact source = artifact("source", "shared.tap.yml", source("shared"));

        WorkbenchSnapshot snapshot = project(
                List.of(source),
                new WorkbenchRemoteState.Available(1),
                List.of(new RemoteArtifact("shared", "pipeline", canonical(pipeline("shared")))));

        assertThat(workspaceRow(snapshot, "source", "shared").alignment()).isEqualTo(WorkbenchAlignment.LOCAL_ONLY);
        assertThat(serverRow(snapshot.pipelines(), "shared").alignment()).isEqualTo(WorkbenchAlignment.REMOTE_ONLY);
    }

    @Test
    void unavailableRemoteStateKeepsLocalRowsButMakesTheirAlignmentUnknown() {
        WorkspaceScan.Artifact local = artifact("source", "local.tap.yml", source("local"));

        for (WorkbenchRemoteState unavailable : List.of(
                new WorkbenchRemoteState.NotConfigured(),
                new WorkbenchRemoteState.SignedOut(),
                new WorkbenchRemoteState.Offline(),
                new WorkbenchRemoteState.Rejected("forbidden", "Access denied"))) {
            WorkbenchSnapshot snapshot = project(List.of(local), unavailable, List.of());

            assertThat(snapshot.workspace().remoteState()).isEqualTo(unavailable);
            assertThat(workspaceRow(snapshot, "source", "local").alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
        }
    }

    @Test
    void diagnosticRemoteStateIsProjectedDirectlyWithoutDroppingLocalRows() {
        WorkspaceScan.Artifact local = artifact("source", "local.tap.yml", source("local"));
        Map<String, String> arguments = new java.util.LinkedHashMap<>();
        arguments.put("operation", "list");
        WorkbenchRemoteState.Diagnostic diagnostic =
                new WorkbenchRemoteState.Diagnostic(ProjectionError.LOAD_FAILED, arguments);

        WorkbenchSnapshot snapshot = project(List.of(local), diagnostic, List.of());
        arguments.put("operation", "changed");

        assertThat(snapshot.workspace().remoteState()).isEqualTo(
                new WorkbenchRemoteState.Diagnostic(
                        ProjectionError.LOAD_FAILED, Map.of("operation", "list")));
        assertThat(workspaceRow(snapshot, "source", "local").alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
        assertThatThrownBy(() -> diagnostic.arguments().put("operation", "changed"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void successfulListingProvesLocalAndRemoteAbsence() {
        WorkspaceScan.Artifact local = artifact("source", "local.tap.yml", source("local"));

        WorkbenchSnapshot snapshot = project(
                List.of(local),
                new WorkbenchRemoteState.Available(1),
                List.of(new RemoteArtifact("remote", "pipeline", canonical(pipeline("remote")))));

        assertThat(workspaceRow(snapshot, "source", "local").alignment()).isEqualTo(WorkbenchAlignment.LOCAL_ONLY);
        assertThat(serverRow(snapshot.pipelines(), "remote").alignment()).isEqualTo(WorkbenchAlignment.REMOTE_ONLY);
    }

    @Test
    void scopesWorkspaceServerTabsAndOverviewToTheirOwnedResourceSets() {
        List<WorkspaceScan.Artifact> local = List.of(
                artifact("transform", "z.tap.yml", transform("z")),
                artifact("source", "z.tap.yml", source("z")),
                artifact("pipeline", "a.tap.yml", pipeline("a")),
                artifact("source", "a.tap.yml", source("a")));

        WorkbenchSnapshot snapshot = project(local, new WorkbenchRemoteState.Available(0), List.of());

        assertThat(snapshot.workspace().rows())
                .extracting(row -> row.key().kind() + "/" + row.key().id())
                .containsExactly("source/a", "source/z", "pipeline/a");
        assertThat(snapshot.sources().rows()).isEmpty();
        assertThat(snapshot.pipelines().rows()).isEmpty();
        assertThat(snapshot.overview().kinds())
                .extracting(WorkbenchKindCount::kind)
                .containsExactly("source", "pipeline");
        assertThat(snapshot.overview().kinds().get(0).localCount()).isEqualTo(2);
        assertThat(snapshot.overview().kinds().get(1).localCount()).isEqualTo(1);
        assertThat(snapshot.overview().alignment()).isEqualTo(
                new WorkbenchAlignmentCounts(3, 0, 0, 0, 0, 0));
    }

    @Test
    void snapshotAndEveryProjectedCollectionAreImmutableCopies() {
        List<WorkspaceScan.Artifact> local = new ArrayList<>();
        local.add(artifact("source", "local.tap.yml", source("local")));
        List<RemoteArtifact> remote = new ArrayList<>();
        remote.add(new RemoteArtifact("remote", "pipeline", canonical(pipeline("remote"))));

        WorkbenchSnapshot snapshot = project(local, new WorkbenchRemoteState.Available(1), remote);
        local.clear();
        remote.clear();

        assertThat(snapshot.workspace().rows()).hasSize(1);
        assertThat(snapshot.pipelines().rows()).hasSize(1);
        assertThatThrownBy(() -> snapshot.workspace().rows().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> snapshot.sources().rows().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> snapshot.overview().kinds().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> workspaceRow(snapshot, "source", "local").local().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> serverRow(snapshot.pipelines(), "remote").remote().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void keepsOnlySanitizedSessionMetadataAndRetainsCompatibilityConstructor() {
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                ROOT,
                Optional.of("dev"),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.SIGNED_IN,
                Optional.of("alice"),
                Optional.of(URI.create("https://tapstate.example")),
                "tapstate 1.2.3");

        WorkbenchSnapshot snapshot = WorkbenchProjection.project(
                2, 7, session, List.of(), new WorkbenchRemoteState.Available(0), List.of());
        WorkbenchSnapshot compatibility = new WorkbenchSnapshot(2, 7);

        assertThat(snapshot.session()).isEqualTo(session);
        assertThat(compatibility.contextGeneration()).isEqualTo(2);
        assertThat(compatibility.requestSequence()).isEqualTo(7);
        assertThat(compatibility.workspace().rows()).isEmpty();
    }

    @Test
    void sessionConstructorSanitizesLandingEndpointIncludingIpv6() {
        String secret = "uri-password-token-31ac";
        URI leaking = URI.create(
                "https://alice:" + secret + "@[2001:db8::7]:9443/admin?token=" + secret + "#fragment");

        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                ROOT,
                Optional.of("dev"),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.SIGNED_IN,
                Optional.of("alice"),
                Optional.of(leaking),
                "tapstate test");

        assertThat(leaking.toString()).contains(secret);
        assertThat(session.landingNode()).contains(URI.create("https://[2001:db8::7]:9443"));
        assertThat(session.toString())
                .doesNotContain("alice:", secret, "/admin", "token=", "#fragment");
    }

    @Test
    void excludesUnsupportedKindsAndRemoteOnlyRowsFromOverviewAndWorkspace() {
        List<RemoteArtifact> remote = List.of(
                new RemoteArtifact("dup", "beta", "one"),
                new RemoteArtifact("x", "alpha", "alpha"),
                new RemoteArtifact("y", "beta", "beta"),
                new RemoteArtifact("dup", "beta", "two"));

        WorkbenchSnapshot snapshot = project(
                List.of(), new WorkbenchRemoteState.Available(remote.size()), remote);

        assertThat(snapshot.overview().kinds())
                .extracting(WorkbenchKindCount::kind)
                .containsExactly("source", "pipeline");
        assertThat(snapshot.overview().kinds().stream()
                .map(WorkbenchKindCount::remoteCount)
                .mapToInt(count -> count.orElseThrow())
                .sum()).isZero();
        assertThat(snapshot.workspace().rows()).isEmpty();
        assertThat(snapshot.overview().alignment())
                .isEqualTo(new WorkbenchAlignmentCounts(0, 0, 0, 0, 0, 0));
        assertThat(snapshot.sources().rows()).isEmpty();
        assertThat(snapshot.pipelines().rows()).isEmpty();
    }

    private static WorkbenchSnapshot project(
            List<WorkspaceScan.Artifact> local,
            WorkbenchRemoteState remoteState,
            List<RemoteArtifact> remote) {
        return WorkbenchProjection.project(1, 1, session(), local, remoteState, remote);
    }

    private static WorkbenchSessionSnapshot session() {
        return new WorkbenchSessionSnapshot(
                ROOT,
                Optional.empty(),
                Optional.empty(),
                WorkbenchConnection.NO_CONTEXT,
                WorkbenchAuthentication.NOT_APPLICABLE,
                Optional.empty(),
                Optional.empty(),
                "tapstate test");
    }

    private static WorkspaceScan.Artifact artifact(String structuralKind, String file, Resource resource) {
        return new WorkspaceScan.Artifact(structuralKind, ROOT.resolve(structuralKind).resolve(file), resource);
    }

    private static Resource source(String id) {
        return parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                """.formatted(id));
    }

    private static Resource pipeline(String id) {
        return parse("""
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src
                """.formatted(id));
    }

    private static Resource transform(String id) {
        return parse("""
                version: tapstate/v1
                kind: transform
                id: %s
                type: map
                fields:
                  id: true
                """.formatted(id));
    }

    private static Resource parse(String yaml) {
        return new DslParser().parse(yaml);
    }

    private static String canonical(Resource resource) {
        return new CanonicalWriter().write(resource);
    }

    private static WorkbenchArtifactRow workspaceRow(WorkbenchSnapshot snapshot, String kind, String id) {
        return snapshot.workspace().rows().stream()
                .filter(candidate -> candidate.key().equals(new WorkbenchArtifactKey(kind, id)))
                .findFirst()
                .orElseThrow();
    }

    private static WorkbenchArtifactRow serverRow(WorkbenchResourceListSnapshot snapshot, String id) {
        return snapshot.rows().stream()
                .filter(candidate -> candidate.key().id().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private static List<String> retainedStrings(Object value) {
        List<String> strings = new ArrayList<>();
        collectStrings(value, strings);
        return strings;
    }

    private static void collectStrings(Object value, List<String> strings) {
        if (value == null) {
            return;
        }
        if (value instanceof String string) {
            strings.add(string);
            return;
        }
        if (value instanceof Path || value instanceof URI || value instanceof Enum<?>
                || value instanceof Number || value instanceof Boolean || value instanceof java.util.OptionalInt) {
            strings.add(value.toString());
            return;
        }
        if (value instanceof Optional<?> optional) {
            optional.ifPresent(item -> collectStrings(item, strings));
            return;
        }
        if (value instanceof Iterable<?> iterable) {
            iterable.forEach(item -> collectStrings(item, strings));
            return;
        }
        if (value instanceof Map<?, ?> map) {
            map.forEach((key, item) -> {
                collectStrings(key, strings);
                collectStrings(item, strings);
            });
            return;
        }
        if (value.getClass().isRecord()) {
            for (RecordComponent component : value.getClass().getRecordComponents()) {
                try {
                    collectStrings(component.getAccessor().invoke(value), strings);
                } catch (ReflectiveOperationException inaccessible) {
                    throw new AssertionError(inaccessible);
                }
            }
            return;
        }
        strings.add(value.toString());
    }

    private enum ProjectionError implements TapstateErrorCode {
        LOAD_FAILED;

        @Override
        public String code() {
            return "test.load-failed";
        }

        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of("operation");
        }
    }
}
