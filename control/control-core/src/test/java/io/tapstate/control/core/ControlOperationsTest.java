package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ControlOperationsTest {

    private final OperationRegistry registry = ControlOperations.registry();

    @Test
    void registersExactlyTheL1OperationSet() {
        assertThat(registry.ids())
                .containsExactlyInAnyOrder(
                        "system.version",
                        "artifact.apply",
                        "artifact.validate",
                        "artifact.get",
                        "artifact.list",
                        "artifact.delete",
                        "source.create",
                        "source.draft",
                        "source.list",
                        "source.get",
                        "source.update",
                        "source.delete",
                        "connection.test",
                        "connection.test-result",
                        "connection.discover-schema",
                        "connection.schema",
                        "connector.register",
                        "connector.list",
                        "connector.get",
                        "connector.icon",
                        "data-browser.collections",
                        "data-browser.find",
                        "data-browser.stats",
                        "cluster.members",
                        "pipeline.start",
                        "pipeline.stop",
                        "pipeline.pause",
                        "pipeline.resume",
                        "pipeline.status",
                        "pipeline.metrics",
                        "pipeline.snapshot",
                        "pipeline.logs",
                        "user.create",
                        "user.passwd",
                        "user.list",
                        "token.create",
                        "token.revoke",
                        "token.list");
    }

    @Test
    void scopesMatchTheOperationInventory() {
        assertThat(registry.resolve("artifact.apply").scope()).isEqualTo(Scope.WRITE);
        assertThat(registry.resolve("artifact.validate").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("artifact.get").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("artifact.list").scope()).isEqualTo(Scope.READ);
        // artifact.delete removes a stored resource for good, so it is the most consequential write in
        // the domain rather than a lesser one; nothing about it is read-only.
        assertThat(registry.resolve("artifact.delete").scope()).isEqualTo(Scope.WRITE);
        assertThat(registry.resolve("source.create").scope()).isEqualTo(Scope.WRITE);
        assertThat(registry.resolve("source.draft").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("source.list").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("source.get").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("source.update").scope()).isEqualTo(Scope.WRITE);
        assertThat(registry.resolve("source.delete").scope()).isEqualTo(Scope.WRITE);
        // connection.test persists its result for later query, so it is a state-mutating write.
        assertThat(registry.resolve("connection.test").scope()).isEqualTo(Scope.WRITE);
        // connection.test-result reads back the latest persisted result; it mutates nothing, so it is read.
        assertThat(registry.resolve("connection.test-result").scope()).isEqualTo(Scope.READ);
        // connection.discover-schema persists the discovered source model for later query, so it is a
        // state-mutating write.
        assertThat(registry.resolve("connection.discover-schema").scope()).isEqualTo(Scope.WRITE);
        // connection.schema reads back the latest persisted source model; it mutates nothing, so it is read.
        assertThat(registry.resolve("connection.schema").scope()).isEqualTo(Scope.READ);
        // connector.register ingests a connector artifact into the distribution store, so it is a
        // state-mutating write.
        assertThat(registry.resolve("connector.register").scope()).isEqualTo(Scope.WRITE);
        // connector.list reads registered authoring candidates from the online catalog view; it mutates
        // nothing, so it is read.
        assertThat(registry.resolve("connector.list").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("connector.get").scope()).isEqualTo(Scope.READ);
        assertThat(registry.resolve("connector.icon").scope()).isEqualTo(Scope.READ);
        // the three data-browser verbs look at what a declared source's own database holds. They read
        // through to the connector and persist nothing at all — not even the result, unlike the two
        // connection probes — so they are read-scoped.
        for (String id : List.of("data-browser.collections", "data-browser.find", "data-browser.stats")) {
            assertThat(registry.resolve(id).scope()).as(id).isEqualTo(Scope.READ);
        }
        // cluster.members reads live topology; it is authenticated like every registry operation, but
        // needs no write or admin privilege.
        assertThat(registry.resolve("cluster.members").scope()).isEqualTo(Scope.READ);
        // the four pipeline lifecycle verbs write desired state, so they are write-scoped.
        for (String id : List.of("pipeline.start", "pipeline.stop", "pipeline.pause", "pipeline.resume")) {
            assertThat(registry.resolve(id).scope()).as(id).isEqualTo(Scope.WRITE);
        }
        // the pipeline observation reads (status/metrics/snapshot store-backed, logs node-local) are all
        // read faces; read-scoped, unaudited.
        for (String id : List.of("pipeline.status", "pipeline.metrics", "pipeline.snapshot", "pipeline.logs")) {
            assertThat(registry.resolve(id).scope()).as(id).isEqualTo(Scope.READ);
        }
        for (String id : List.of("user.create", "user.passwd", "user.list", "token.create", "token.revoke", "token.list")) {
            assertThat(registry.resolve(id).scope()).as(id).isEqualTo(Scope.ADMIN);
        }
    }

    @Test
    void auditFlagMarksOnlyTheStateMutatingOperations() {
        for (String id :
                List.of(
                        "artifact.apply",
                        "artifact.delete",
                        "source.create",
                        "source.update",
                        "source.delete",
                        "connection.test",
                        "connection.discover-schema",
                        "connector.register",
                        "pipeline.start",
                        "pipeline.stop",
                        "pipeline.pause",
                        "pipeline.resume",
                        "user.create",
                        "user.passwd",
                        "token.create",
                        "token.revoke")) {
            assertThat(registry.resolve(id).audited()).as(id).isTrue();
        }
        for (String id : List.of(
                "system.version",
                "artifact.get",
                "artifact.list",
                "artifact.validate",
                "source.draft",
                "source.list",
                "source.get",
                "connection.test-result",
                "connection.schema",
                "connector.list",
                "connector.get",
                "connector.icon",
                "data-browser.collections",
                "data-browser.find",
                "data-browser.stats",
                "cluster.members",
                "user.list",
                "token.list",
                "pipeline.status",
                "pipeline.metrics",
                "pipeline.snapshot",
                "pipeline.logs")) {
            assertThat(registry.resolve(id).audited()).as(id).isFalse();
        }
    }

    @Test
    void theRegistryOpensEveryL1OperationOnTheCliFace() {
        // A scope statement about the registry alone: the CLI face opens every registered operation and
        // clips none of them. Whether each one has a verb behind it is not knowable from here
        // — control-core cannot see the CLI — and is gated where both are visible, in arch-tests.
        assertThat(registry.exposedOn(Frontend.CLI)).hasSize(37);
        assertThat(registry.all()).allSatisfy(op ->
                assertThat(op.exposure()).as(op.id()).containsEntry(Frontend.CLI, Maturity.CURRENT));
    }

    @Test
    void everyOperationIsStagedAtTheOneShippedStageOnEveryFaceItIsOpenOn() {
        // One stage across the whole registry, not one per face. A second stage in here is what makes a
        // face's surface depend on which ceiling it happened to name, which is the thing the ceilingless
        // exposedOn(Frontend) exists to remove — an entry left at another stage would put it back.
        assertThat(registry.all()).allSatisfy(op ->
                assertThat(op.exposure().values()).as(op.id()).containsOnly(Maturity.CURRENT));
    }

    @Test
    void mcpFaceIsTheOnlineAuthoringClosurePlusTheReadFaceAndRestExposureRemainsEmpty() {
        // The read face joins on the same terms as everything else here — a mark on the registry entry.
        // The three are read-scoped, so a caller holding no write capability still gets all three.
        assertThat(registry.exposedOn(Frontend.MCP))
                .extracting(Operation::id)
                .containsExactlyInAnyOrder(
                        "system.version",
                        "connector.list", "connector.get",
                        "source.draft",
                        "connection.test", "connection.test-result",
                        "connection.discover-schema", "connection.schema",
                        "artifact.validate", "artifact.apply", "artifact.delete", "artifact.get",
                        "pipeline.start", "pipeline.stop", "pipeline.status",
                        "pipeline.metrics", "pipeline.snapshot", "pipeline.logs",
                        "data-browser.collections", "data-browser.find", "data-browser.stats");
        // Deliberately the widest ceiling, not the shipped one: REST carries no operation at any stage,
        // which is a stronger statement than "none has reached the stage we ship".
        assertThat(registry.exposedOn(Frontend.REST, Maturity.GA)).isEmpty();
    }

    /**
     * The precondition a removal demands has to be obtainable on the same face that offers the removal.
     * artifact.delete requires a content hash, and a remote model cannot compute SHA-256 for itself, so
     * its only route to one is reading the artifact here. Pinning the implication — "delete requires the
     * hash" therefore "get is exposed on this face" — is what stops the two from drifting apart: the
     * removal shipped on MCP while the read stayed CLI-only, which left the verb callable in principle
     * and unusable in fact for every kind whose structured read does not carry a hash of its own.
     */
    @Test
    void theMcpFaceCarriesAReadForThePreconditionItsRemovalDemands() {
        Map<String, Object> deleteRequest =
                ControlApiSchema.resolve(registry.resolve("artifact.delete").schema().params());
        List<?> required = (List<?>) deleteRequest.get("required");
        assertThat(required.stream().map(String::valueOf).toList()).contains("expectedContentHash");

        Operation get = registry.resolve("artifact.get");
        // The stage is the one the build ships at, not a stage this assertion names: what it pins is
        // that the read is on the face at all.
        assertThat(get.exposure()).containsEntry(Frontend.MCP, Maturity.CURRENT);
        assertThat(get.scope())
                .as("the read that supplies a precondition must not itself need write access")
                .isEqualTo(Scope.READ);
        assertThat(get.audited())
                .as("a read leaves no audit record")
                .isFalse();
    }

    /**
     * artifact.delete is the first operation on the MCP face that destroys a named resource, so its
     * exposure is pinned on its own rather than left to the set assertion above: a remote model may reach
     * it only in a session that was explicitly started with write access, which is what the WRITE scope
     * buys — the sidecar filters by scope, so a delete silently re-scoped to READ would become callable
     * in every read-only session.
     */
    @Test
    void theDestructiveArtifactVerbIsWriteScopedOnTheMcpFace() {
        Operation delete = registry.resolve("artifact.delete");
        assertThat(delete.exposure()).containsEntry(Frontend.MCP, Maturity.CURRENT);
        assertThat(delete.scope())
                .as("a READ-scoped delete would be exposed in read-only MCP sessions")
                .isEqualTo(Scope.WRITE);
        assertThat(delete.schema()).as("an MCP operation must carry a schema ref").isNotNull();
        assertThat(delete.description()).as("an MCP tool must carry a description").isNotBlank();
    }
}
