package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterExecutionContextDocumentsTest {
    @Test
    void originalBootAndRuntimeUuidSurviveSerializationAndDoNotBecomeTheCurrentOwner() {
        Map<String, ClusterExecutionMember> original = Map.of("a", new ClusterExecutionMember("a", "original-boot", "original-uuid"));
        Document record = claim().append("executionMembers", MongoClusterCapacityStore.executionMemberDocuments(original));
        var read = MongoWorkloadClaimStore.readDocument(Document.parse(record.toJson()));
        assertThat(read.originalMembersPresent(original)).contains(true);
        assertThat(read.originalMembersPresent(Map.of("a", new ClusterExecutionMember("a", "current-owner-boot", "current-uuid"))))
                .contains(false);
    }

    @Test
    void aMissingOriginalCohortRemainsUnknown() {
        assertThat(MongoWorkloadClaimStore.readDocument(claim()).originalMembersPresent(
                Map.of("a", new ClusterExecutionMember("a", "current-owner-boot", "current-uuid")))).isEmpty();
    }

    @Test
    void partialAndDuplicateOriginalCohortsAreDiagnosableCorruption() {
        Document partial = claim().append("executionNodeIds", List.of("a", "b")).append("executionMembers", List.of(member()));
        assertUnreadable(partial);
        assertUnreadable(claim().append("executionMembers", List.of(member(), member())));
        assertUnreadable(claim().append("executionMembers", List.of(new Document("nodeId", "a").append("bootId", "original-boot"))));
    }

    private static void assertUnreadable(Document document) {
        assertThatThrownBy(() -> MongoWorkloadClaimStore.readDocument(document)).isInstanceOfSatisfying(TapstateException.class,
                error -> {
                    assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                    assertThat(error.args()).containsEntry("field", "executionMembers");
                });
    }

    private static Document member() {
        return new Document("nodeId", "a").append("bootId", "original-boot").append("memberUuid", "original-uuid");
    }

    private static Document claim() {
        return new Document("_id", "claim").append("clusterId", "east").append("resourceType", "PIPELINE_ACTUATION")
                .append("resourceId", "orders").append("ownerNodeId", "a").append("ownerBootId", "current-owner-boot")
                .append("claimGeneration", 8L).append("executionGeneration", 42L).append("topologyRevision", 2L)
                .append("leaseUntil", Date.from(Instant.parse("2026-10-10T01:00:00Z")))
                .append("contextExecutionGeneration", 42L).append("executionClaimGeneration", 7L)
                .append("executionNodeIds", List.of("a"));
    }
}
