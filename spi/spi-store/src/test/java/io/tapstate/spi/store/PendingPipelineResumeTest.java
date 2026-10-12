package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

    @Test
    void aResumeMarkerRequiresTheCompleteIdentityOfTheExecutionItRetires() {
        WorkloadClaim complete = new Claim().build();
        assertThat(request(complete).originalClaim()).isEqualTo(complete);
        Map<String, ThrowingCallable> incomplete = new LinkedHashMap<>();
        incomplete.put("unwritten epoch", () -> new PendingPipelineResume(0, "intent", complete, "1", "1"));
        incomplete.put("no intent", () -> new PendingPipelineResume(3, null, complete, "1", "1"));
        incomplete.put("blank intent", () -> new PendingPipelineResume(3, " ", complete, "1", "1"));
        incomplete.put("no native job", () -> new PendingPipelineResume(3, "intent", complete, null, "1"));
        incomplete.put("blank native job", () -> new PendingPipelineResume(3, "intent", complete, "", "1"));
        incomplete.put("no runtime execution", () -> new PendingPipelineResume(3, "intent", complete, "1", null));
        incomplete.put("blank runtime execution", () -> new PendingPipelineResume(3, "intent", complete, "1", " "));
        incomplete.put("blank reservation", () -> request(complete).withReservation(" "));
        incomplete.put("recovery claim", () -> request(new Claim().with(claim -> claim.key =
                new WorkloadClaimKey("east", WorkloadClaimType.CLUSTER_RECOVERY, "east")).build()));
        incomplete.put("no profile generation", () -> request(new Claim().with(claim -> claim.profileGeneration = 0).build()));
        incomplete.put("no execution", () -> request(new Claim().with(claim -> {
            claim.executionGeneration = 0;
            claim.contextExecutionGeneration = 0;
        }).build()));
        incomplete.put("context of another execution", () -> request(new Claim().with(claim ->
                claim.contextExecutionGeneration = 6).build()));
        incomplete.put("no execution claim", () -> request(new Claim().with(claim ->
                claim.executionClaimGeneration = 0).build()));
        incomplete.put("no execution profile", () -> request(new Claim().with(claim ->
                claim.executionProfile = null).build()));
        incomplete.put("profile of another generation", () -> request(new Claim().with(claim -> claim.executionProfile =
                new ClusterExecutionProfile("east", 2, new ExecutionProfile(1, Map.of("build", "one")))).build()));
        incomplete.put("no execution topology", () -> request(new Claim().with(claim ->
                claim.executionTopologyRevision = null).build()));
        incomplete.put("no incarnation", () -> request(new Claim().with(claim -> claim.incarnation = null).build()));
        incomplete.put("blank incarnation", () -> request(new Claim().with(claim -> claim.incarnation = " ").build()));
        incomplete.put("no revision", () -> request(new Claim().with(claim -> claim.revision = null).build()));
        incomplete.put("blank revision", () -> request(new Claim().with(claim -> claim.revision = "").build()));
        incomplete.put("no members", () -> request(new Claim().with(claim -> {
            claim.nodeIds = Set.of();
            claim.members = Map.of();
        }).build()));
        incomplete.put("members unknown", () -> request(new Claim().with(claim -> claim.members = Map.of()).build()));
        assertThat(incomplete).hasSize(22);
        incomplete.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pending resume requires a complete current execution identity"));
    }

    @Test
    void theExecutionContextComparesEveryImmutableFactButNotTheLease() {
        WorkloadClaim original = new Claim().build();
        assertThat(PendingPipelineResume.sameExecutionContext(original, new Claim().with(claim -> {
            claim.claimGeneration = 3;
            claim.lease = NOW.plusSeconds(60);
            claim.topology = 5;
        }).build())).isTrue();
        Map<String, WorkloadClaim> changed = new LinkedHashMap<>();
        changed.put("another pipeline", new Claim().with(claim -> claim.key =
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "payments")).build());
        changed.put("another execution", new Claim().with(claim -> {
            claim.executionGeneration = 8;
            claim.contextExecutionGeneration = 8;
        }).build());
        changed.put("another context", new Claim().with(claim -> claim.contextExecutionGeneration = 6).build());
        changed.put("another execution claim", new Claim().with(claim -> claim.executionClaimGeneration = 3).build());
        changed.put("another member set", new Claim().with(claim -> {
            claim.nodeIds = Set.of("b");
            claim.members = Map.of("b", new ClusterExecutionMember("b", "boot-a", "uuid-a"));
        }).build());
        changed.put("another topology", new Claim().with(claim -> claim.executionTopologyRevision = 3L).build());
        changed.put("another incarnation", new Claim().with(claim -> claim.incarnation = "other").build());
        assertThat(changed).hasSize(7);
        changed.forEach((name, other) -> assertThat(PendingPipelineResume.sameExecutionContext(original, other))
                .as(name).isFalse());
        assertThat(PendingPipelineResume.sameExecutionContext(null, original)).isFalse();
        assertThat(PendingPipelineResume.sameExecutionContext(original, null)).isFalse();

        PendingPipelineResume first = request(original);
        assertThat(first.sameRequestAs(null)).isFalse();
        assertThat(first.sameRequestAs(new PendingPipelineResume(3, "intent", original, "1", "2"))).isFalse();
    }

    /** The fields of a complete current pipeline execution claim, changed one at a time by each case. */
    private static final class Claim {
        WorkloadClaimKey key = new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
        long claimGeneration = 2;
        long executionGeneration = 7;
        long topology = 2;
        Instant lease = NOW;
        long contextExecutionGeneration = 7;
        long executionClaimGeneration = 2;
        Set<String> nodeIds = Set.of("a");
        long profileGeneration = 1;
        ClusterExecutionProfile executionProfile = PROFILE;
        Long executionTopologyRevision = 2L;
        String incarnation = "incarnation";
        String revision = "revision";
        Map<String, ClusterExecutionMember> members = Map.of("a", new ClusterExecutionMember("a", "boot-a", "uuid-a"));

        Claim with(Consumer<Claim> change) {
            change.accept(this);
            return this;
        }

        WorkloadClaim build() {
            return new WorkloadClaim(key, new WorkloadOwner("a", "boot-a"), claimGeneration, executionGeneration,
                    topology, lease, contextExecutionGeneration, executionClaimGeneration, nodeIds, 0, false,
                    profileGeneration, executionProfile, executionTopologyRevision, incarnation, revision, members);
        }
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
