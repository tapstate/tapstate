package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ExecutionProfile;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterRecoveryAuthorityDocumentsTest {
    private static final Instant NOW = Instant.parse("2026-10-10T01:00:00Z");
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("east", 1, new ExecutionProfile(1, Map.of("build", "one")));
    private static final ClusterRecoveryKey KEY = new ClusterRecoveryKey("east", "orders", "inc-a");

    @Test
    void aNewHolderCannotConsumeTheOriginalsUnexpiredPromise() {
        Document pipeline = context().append("claimGeneration", 2L).append("retiredAuthorizationUntil", Date.from(NOW.plusSeconds(20)));
        var waiting = MongoClusterRecoveryStore.originalAuthority(event(), PROFILE, pipeline, new Document(), NOW);
        assertThat(waiting.retired()).isFalse();
        assertThat(waiting.nextEligibleAt()).isEqualTo(NOW.plusSeconds(20));
        assertThat(MongoClusterRecoveryStore.originalAuthority(event(), PROFILE, pipeline, new Document(), NOW.plusSeconds(20)).retired())
                .isTrue();
    }

    @Test
    void anOldInitialNoExecutionScalarCannotRetireTheCurrentIssuer() {
        Document pipeline = context().append("claimGeneration", 1L).append("retiredAuthorizationUntil", Date.from(Instant.EPOCH));
        assertThat(MongoClusterRecoveryStore.originalAuthority(event(), PROFILE, pipeline, new Document(), NOW).retired()).isFalse();
    }

    @Test
    void missingRetirementOrMismatchedContextIsNeverRetirementProof() {
        Document pipeline = context().append("claimGeneration", 2L);
        assertThat(MongoClusterRecoveryStore.originalAuthority(event(), PROFILE, pipeline, new Document(), NOW).retired()).isFalse();
        pipeline.append("retiredAuthorizationUntil", Date.from(Instant.EPOCH)).append("contextExecutionGeneration", 40L);
        assertThat(MongoClusterRecoveryStore.originalAuthority(event(), PROFILE, pipeline, new Document(), NOW).retired()).isFalse();
    }

    @Test
    void aNewProfileUsesItsColdBoundaryAndLegacyNeedsItsStoredMarker() {
        var next = new ClusterExecutionProfile("east", 2, PROFILE.profile());
        assertThat(MongoClusterRecoveryStore.originalAuthority(event(), next, context(), new Document(), NOW).retired()).isTrue();
        var legacy = new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.FULL_CLUSTER_RESTART, 41, null, null, null,
                true, next, 2, "intent", Map.of());
        assertThat(MongoClusterRecoveryStore.originalAuthority(legacy, next, context(), new Document(), NOW).retired()).isFalse();
        Document profile = new Document("legacyAuthorityRetiredAt", Date.from(NOW.minusSeconds(1)));
        assertThat(MongoClusterRecoveryStore.originalAuthority(legacy, next, context(), profile, NOW).retired()).isTrue();
        assertThat(MongoClusterRecoveryStore.originalAuthority(legacy, next, context().append("executionProfileVersion", 1), profile, NOW).retired())
                .isFalse();
    }

    private static Document context() {
        return new Document("contextExecutionGeneration", 41L).append("executionClaimGeneration", 1L)
                .append("claimGeneration", 1L).append("leaseUntil", Date.from(NOW.plusSeconds(30)));
    }

    private static ClusterRecoveryEvent event() {
        return new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.MEMBER_LOSS, 41, "revision", 1L, PROFILE, PROFILE,
                2, "intent", Map.of());
    }
}
