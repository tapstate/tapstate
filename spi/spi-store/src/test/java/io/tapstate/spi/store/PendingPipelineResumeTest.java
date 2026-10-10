package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PendingPipelineResumeTest {
    private static final Instant NOW = Instant.parse("2026-10-11T00:00:00Z");
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("east", 1,
            new ExecutionProfile(1, Map.of("build", "one")));

    @Test
    void requestIdentitySurvivesLeaseTopologyRefreshAndReservationLink() {
        var first = request(claim(2, 2, NOW, "boot-a", "uuid-a", "revision"));
        var renewed = request(claim(2, 3, NOW.plusSeconds(30), "boot-a", "uuid-a", "revision")).withReservation("receipt");
        assertThat(first).isNotEqualTo(renewed);
        assertThat(first.sameRequestAs(renewed)).isTrue();
        assertThat(renewed.sameRequestAs(first)).isTrue();
    }

    @Test
    void anotherAuthorityIncarnationCohortOrAcceptedRequestCannotReuseTheMarker() {
        var first = request(claim(2, 2, NOW, "boot-a", "uuid-a", "revision"));
        assertThat(first.sameRequestAs(request(claim(3, 2, NOW, "boot-a", "uuid-a", "revision")))).isFalse();
        assertThat(first.sameRequestAs(request(claim(2, 2, NOW, "boot-next", "uuid-next", "revision")))).isFalse();
        assertThat(first.sameRequestAs(request(claim(2, 2, NOW, "boot-a", "uuid-next", "revision")))).isFalse();
        assertThat(first.sameRequestAs(request(claim(2, 2, NOW, "boot-a", "uuid-a", "other-revision")))).isFalse();
        assertThat(first.sameRequestAs(new PendingPipelineResume(4, "intent", first.originalClaim(), "1", "1"))).isFalse();
        assertThat(first.sameRequestAs(new PendingPipelineResume(3, "other-intent", first.originalClaim(), "1", "1"))).isFalse();
        assertThat(first.sameRequestAs(new PendingPipelineResume(3, "intent", first.originalClaim(), "2", "1"))).isFalse();
    }

    @Test
    void unknownLegacyExecutionCannotBecomeAnExplicitResumeProof() {
        WorkloadClaim legacy = new WorkloadClaim(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("a", "boot-a"), 2, 7, 2, NOW);
        assertThatThrownBy(() -> request(legacy)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUnimplementedStateMarkerWriteNeverFallsBackToOrdinaryCas() {
        AtomicInteger writes = new AtomicInteger();
        StateStore ordinary = new StateStore() {
            @Override public Optional<CheckpointDoc> read(String id) { return Optional.empty(); }
            @Override public void create(String id, String state, Instant touch) {}
            @Override public void delete(String id) {}
            @Override public CasOutcome compareAndSwap(String id, long epoch, String state, Instant touch) {
                writes.incrementAndGet();
                return new CasOutcome.Fenced(epoch);
            }
        };
        assertThatThrownBy(() -> ordinary.compareAndSwap("orders", 2, "RUNNING", NOW,
                request(claim(2, 2, NOW, "boot-a", "uuid-a", "revision"))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
        assertThat(writes).hasValue(0);
        ordinary.compareAndSwap("orders", 2, "RUNNING", NOW, null);
        assertThat(writes).hasValue(1);
    }

    private static PendingPipelineResume request(WorkloadClaim claim) {
        return new PendingPipelineResume(3, "intent", claim, "1", "1");
    }

    private static WorkloadClaim claim(long generation, long topology, Instant lease, String boot, String uuid, String revision) {
        return new WorkloadClaim(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("a", boot), generation, 7, topology, lease, 7, 2, Set.of("a"), 0, false,
                1, PROFILE, 2L, "incarnation", revision, Map.of("a", new ClusterExecutionMember("a", boot, uuid)));
    }
}
