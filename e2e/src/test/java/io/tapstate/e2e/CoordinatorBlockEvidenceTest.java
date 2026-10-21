package io.tapstate.e2e;

import io.tapstate.spi.store.DerivedSchema;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorBlockEvidenceTest {
    private static final String KEY = "owned.source.orders";
    private static final String TAG = "tapstate/owned-boot";
    private static final DerivedSchema ORIGINAL = new DerivedSchema(0, Map.of("id", "STRING NULL"),
            "original-statement", "original-input", "source-copy-1");
    private static final Map<String, String> COLUMNS = Map.of("id", "STRING NULL", "note", "STRING NULL");

    @Test void recognizesBothDriverUpdateEnvelopesWithoutRetainingTheirContents() {
        Document update = update();
        Document direct = operation(update);
        Document batched = operation(new Document("updates", List.of(update)));
        var counts = CoordinatorBlockEvidence.operationCounts(List.of(direct, batched), List.of(TAG), KEY, ORIGINAL, COLUMNS);
        assertThat(counts).containsEntry("active", 2).containsEntry("tagged", 2).containsEntry("exactMutation", 2);
        assertThat(counts.values()).allMatch(value -> value == 2);
        assertThat(counts.toString()).doesNotContain(KEY, TAG, "original-statement", "changed-input", "source-copy-1");
    }

    @Test void separatesMissingOperationForeignBootAndWrongLane() {
        assertThat(CoordinatorBlockEvidence.operationCounts(List.of(), List.of(TAG), KEY, ORIGINAL, COLUMNS))
                .allSatisfy((name, count) -> assertThat(count).isZero());
        Document foreign = operation(update()).append("appName", "tapstate/foreign-boot");
        Document wrongKey = operation(update().append("q", new Document("_id", "foreign-lane")));
        var counts = CoordinatorBlockEvidence.operationCounts(List.of(foreign, wrongKey), List.of(TAG), KEY, ORIGINAL, COLUMNS);
        assertThat(counts).containsEntry("active", 2).containsEntry("tagged", 1).containsEntry("keyMatched", 0)
                .containsEntry("exactMutation", 0);
    }

    @Test void distinguishesSameShapeRefreshFromARealSchemaDelta() {
        Document update = update();
        update.get("u", Document.class).get("$set", Document.class).put("versions", List.of(version()));
        var counts = CoordinatorBlockEvidence.operationCounts(List.of(operation(update)), List.of(TAG), KEY, ORIGINAL, COLUMNS);
        assertThat(counts).containsEntry("keyMatched", 1).containsEntry("twoVersions", 0).containsEntry("exactMutation", 0);
    }

    @Test void rejectsWrongProvenanceVersionAndColumnsIndependently() {
        for (String field : List.of("statement", "derivedBy", "derivedFrom", "version", "columns")) {
            Document version = version();
            version.put(field, switch (field) {
                case "derivedFrom" -> ORIGINAL.derivedFrom();
                case "version" -> 2L;
                case "columns" -> List.of(new Document("name", "id").append("type", "STRING NULL"));
                default -> "unmatched";
            });
            Document update = update();
            update.get("u", Document.class).get("$set", Document.class).put("versions", List.of(new Document(), version));
            assertThat(CoordinatorBlockEvidence.operationCounts(List.of(operation(update)), List.of(TAG), KEY, ORIGINAL, COLUMNS))
                    .containsEntry("twoVersions", 1).containsEntry("exactMutation", 0);
        }
    }

    @Test void doesNotEchoUnknownQueueFieldsOrStatus() {
        assertThat(CoordinatorBlockEvidence.queueFacts(null)).isEqualTo(Map.of("present", false));
        var facts = CoordinatorBlockEvidence.queueFacts(new Document("status", "PRIVATE_MARKER")
                .append("attempt", 0).append("permit", new Document("payload", "PRIVATE_MARKER"))
                .append("server", "PRIVATE_MARKER"));
        assertThat(facts).containsEntry("status", "UNRECOGNIZED").containsEntry("permitPresent", true)
                .containsEntry("successorPresent", false).containsEntry("attemptZero", true);
        assertThat(facts.toString()).doesNotContain("PRIVATE_MARKER", "payload", "server");
        assertThat(CoordinatorBlockEvidence.queueFacts(new Document("status", "REBUILDING")))
                .containsEntry("status", "REBUILDING");
    }

    @Test void takeoverReceiptHasOnlyFixedBooleanFields() {
        var facts = CoordinatorBlockEvidence.takeoverFacts(Map.of("newLiveOwner", true,
                "permitPresent", true, "PRIVATE_MARKER", true));
        assertThat(facts).containsEntry("newLiveOwner", true).containsEntry("permitPresent", true)
                .containsEntry("nativeCohortReady", false).containsEntry("lastObservationOnly", true)
                .containsEntry("observationPresent", false);
        assertThat(facts).containsOnlyKeys("schema", "lastObservationOnly", "observationPresent", "newLiveOwner", "nativeCohortReady",
                "quorumReady", "itemPresent", "permitPresent", "originalReservationPresent");
        assertThat(facts.toString()).doesNotContain("PRIVATE_MARKER");
        assertThat(CoordinatorBlockEvidence.takeoverFacts(Map.of("newLiveOwner", false, "nativeCohortReady", false,
                "quorumReady", false, "itemPresent", false, "permitPresent", false, "originalReservationPresent", false)))
                .containsEntry("observationPresent", true);
    }

    @Test void admissionReceiptSeparatesLeaseRetirementEligibilityAndNativePublication() {
        Document first = publisher("a"), second = publisher("b");
        Document pipeline = publisher("a").append("executionClaimGeneration", 1L)
                .append("resourceType", "PIPELINE_ACTUATION").append("resourceId", "owned-pipeline")
                .append("retiredAuthorizationUntil", new Date(900));
        Document recovery = publisher("b");
        var facts = CoordinatorBlockEvidence.admissionFacts(new Date(1000),
                new Document("nextEligibleAt", new Date(1000)), pipeline, recovery, List.of(first, second, pipeline),
                List.of(registration("a", first), registration("b", second)),
                new Document("revision", 7L).append("profileGeneration", 3L), Set.of("a", "b"));
        assertThat(facts).containsEntry("pipelineLeaseLive", true).containsEntry("pipelineControlAdvanced", true)
                .containsEntry("pipelineControlAdvancedRepeatedly", false).containsEntry("retirementDue", true)
                .containsEntry("queueEligible", true).containsEntry("recoveryLeaseLive", true)
                .containsEntry("pipelineAuthorityStableAcrossReads", true).containsEntry("recoveryOwnerBootMatchesView", true)
                .containsEntry("nativeViewPresentCount", 2).containsEntry("nativeViewFreshCount", 2)
                .containsEntry("nativeViewExactMemberSetCount", 2).containsEntry("nativeViewScopeCurrentCount", 2)
                .containsEntry("nativePublisherLiveCurrentCount", 2).containsEntry("nativeSamplesMatch", true);
        assertThat(facts.values()).allMatch(value -> value instanceof Boolean || value instanceof Integer);
        assertThat(facts.toString()).doesNotContain("PRIVATE_MARKER", "private-boot", "private-hash");
    }

    @Test void admissionReceiptDoesNotConfuseIndependentNativeFailures() {
        Document first = publisher("a"), second = publisher("b");
        Document a = registration("a", first), b = registration("b", second);
        b.get("nativeView", Document.class).put("validUntil", new Date(1000));
        b.get("nativeView", Document.class).put("members", List.of(new Document("nodeId", "b")));
        b.get("nativeView", Document.class).put("topologyRevision", 6L);
        b.get("nativeView", Document.class).get("publisherClaim", Document.class).put("claimGeneration", 1L);
        var facts = CoordinatorBlockEvidence.admissionFacts(new Date(1000), new Document("nextEligibleAt", new Date(1100)),
                publisher("a").append("executionClaimGeneration", 1L).append("claimGeneration", 4L)
                        .append("retiredAuthorizationUntil", new Date(1100)),
                publisher("b").append("leaseUntil", new Date(1000)), List.of(first, second), List.of(a, b),
                new Document("revision", 7L).append("profileGeneration", 3L), Set.of("a", "b"));
        assertThat(facts).containsEntry("queueEligible", false).containsEntry("retirementDue", false)
                .containsEntry("pipelineControlAdvancedRepeatedly", true).containsEntry("recoveryLeaseLive", false)
                .containsEntry("nativeViewFreshCount", 1).containsEntry("nativeViewExactMemberSetCount", 1)
                .containsEntry("nativeViewScopeCurrentCount", 1).containsEntry("nativePublisherLiveCurrentCount", 1)
                .containsEntry("nativeSamplesMatch", false);
    }

    @Test void missingAdmissionObservationsRemainExplicit() {
        var facts = CoordinatorBlockEvidence.admissionFacts(null, null, null, null, List.of(), List.of(), null, Set.of("a", "b"));
        assertThat(facts).containsEntry("serverClockPresent", false).containsEntry("pipelineLeasePresent", false)
                .containsEntry("retirementHorizonPresent", false).containsEntry("recoveryClaimPresent", false)
                .containsEntry("nativeViewPresentCount", 0).containsEntry("nativeSamplesMatch", false);
    }

    @Test void ownedOperationDomainsNeverExposeOtherCommandsOrNamespaces() {
        var counts = CoordinatorBlockEvidence.operationDomains(List.of(
                new Document("ns", "owned.derived").append("op", "update").append("waitingForLock", true),
                new Document("ns", "owned.PRIVATE_MARKER").append("op", "command").append("command", new Document("secret", "PRIVATE_MARKER")),
                new Document("ns", "PRIVATE_MARKER").append("op", "PRIVATE_MARKER").append("waitingForFlowControl", true)),
                "owned", "owned.derived");
        assertThat(counts).containsEntry("active", 3).containsEntry("schemaNamespace", 1)
                .containsEntry("otherOwnedStoreNamespace", 1).containsEntry("outsideOwnedStoreNamespace", 1)
                .containsEntry("waitingForLock", 1).containsEntry("waitingForFlowControl", 1)
                .containsEntry("update", 1).containsEntry("command", 1).containsEntry("otherOperation", 1);
        assertThat(counts.toString()).doesNotContain("PRIVATE_MARKER", "secret", "owned.derived");
    }

    @Test void acquisitionTopologyRefreshDoesNotReportAnAuthorityChange() {
        Document pipeline = publisher("a").append("resourceType", "PIPELINE_ACTUATION").append("resourceId", "owned-pipeline");
        Document refreshed = new Document(pipeline).append("topologyRevision", 8L);
        var same = CoordinatorBlockEvidence.admissionFacts(new Date(1000), null, pipeline, null,
                List.of(refreshed), List.of(), null, Set.of("a"));
        assertThat(same).containsEntry("pipelineAuthorityStableAcrossReads", true);
        refreshed.put("claimGeneration", 3L);
        var changed = CoordinatorBlockEvidence.admissionFacts(new Date(1000), null, pipeline, null,
                List.of(refreshed), List.of(), null, Set.of("a"));
        assertThat(changed).containsEntry("pipelineAuthorityStableAcrossReads", false);
    }

    @Test void armComparisonDistinguishesTopologyReacquisitionFromControllerReplacement() {
        var arm = new CoordinatorBlockEvidence.Arm(2L, 7L, new Date(900));
        var topology = CoordinatorBlockEvidence.armFacts(arm, new Document("claimGeneration", 2)
                .append("topologyRevision", 8L).append("retiredAuthorizationUntil", new Date(1200)));
        assertThat(topology).containsEntry("snapshotPresent", true).containsEntry("retirementComparable", true)
                .containsEntry("retirementMoved", true).containsEntry("controllerChanged", false).containsEntry("topologyChanged", true);
        var controller = CoordinatorBlockEvidence.armFacts(arm, new Document("claimGeneration", 3L)
                .append("topologyRevision", 7L).append("retiredAuthorizationUntil", new Date(1200)));
        assertThat(controller).containsEntry("controllerChanged", true).containsEntry("topologyChanged", false);
        assertThat(topology.values()).allMatch(value -> value instanceof Boolean);
    }

    @Test void missingArmAndMalformedComparisonFieldsRemainUnknown() {
        assertThat(CoordinatorBlockEvidence.armFacts(null, null)).allSatisfy((key, value) -> assertThat(value).isFalse());
        var facts = CoordinatorBlockEvidence.armFacts(new CoordinatorBlockEvidence.Arm(2L, 7L, new Date(900)),
                new Document("claimGeneration", "PRIVATE_MARKER").append("topologyRevision", "PRIVATE_MARKER")
                        .append("retiredAuthorizationUntil", "PRIVATE_MARKER"));
        assertThat(facts).containsEntry("snapshotPresent", true).containsEntry("controllerComparable", false)
                .containsEntry("topologyComparable", false).containsEntry("retirementComparable", false);
        assertThat(facts.toString()).doesNotContain("PRIVATE_MARKER");
    }

    private static Document publisher(String node) {
        return new Document("clusterId", "PRIVATE_MARKER").append("resourceType", "NODE_SESSION").append("resourceId", node)
                .append("ownerNodeId", node).append("ownerBootId", "private-boot-" + node).append("claimGeneration", 2L)
                .append("executionGeneration", 0L).append("topologyRevision", 7L).append("profileGeneration", 3L)
                .append("leaseUntil", new Date(2000));
    }

    private static Document registration(String node, Document publisher) {
        return new Document("nodeId", node).append("profileGeneration", 3L).append("hash", "private-hash")
                .append("nativeView", new Document("schemaVersion", 1).append("publisherClaim", new Document(publisher))
                        .append("profile", new Document("generation", 3L).append("hash", "private-hash"))
                        .append("topologyRevision", 7L).append("cohortHash", "PRIVATE_MARKER").append("validUntil", new Date(2000))
                        .append("members", List.of(new Document("nodeId", "a"), new Document("nodeId", "b"))));
    }

    private static Document operation(Document command) { return new Document("appName", TAG).append("command", command); }
    private static Document update() {
        return new Document("q", new Document("_id", KEY)).append("u",
                new Document("$set", new Document("versions", List.of(new Document(), version()))));
    }
    private static Document version() {
        return new Document("version", 1L).append("statement", ORIGINAL.statement()).append("derivedBy", ORIGINAL.derivedBy())
                .append("derivedFrom", "changed-input").append("columns", List.of(
                        new Document("name", "id").append("type", "STRING NULL"),
                        new Document("name", "note").append("type", "STRING NULL")));
    }
}
