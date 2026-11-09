package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.MemberProcess;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An owner frozen inside a write past its lease is taken over within about one lease, not the server's
 * transaction lifetime.
 *
 * <p>Two processes on one host: the owner is a separate JVM holding a transaction open on the claim document,
 * stopped with SIGSTOP; this test takes the expired claim over as another process. A frozen owner is still a
 * cluster member, so nothing ends its transactions on departure. Without ending them before acquiring, the
 * takeover's own transaction conflicts with the open one and retries until the server aborts it after its
 * transaction lifetime (60 s by default here, 70 to 87 s measured on a takeover that waited for it).
 */
@RequiresDocker
class FrozenOwnerTakeoverIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final WorkloadOwner FROZEN = new WorkloadOwner("node-a", "boot-frozen");
    private static final WorkloadOwner TAKER = new WorkloadOwner("node-b", "boot-taker");

    @Test
    void anOwnerFrozenInsideAWritePastItsLeaseIsTakenOverWithinAboutOneLease() {
        String url = MONGO.getReplicaSetUrl();
        var prepared = CachePipelineRig.prepare(url, FROZEN, LEASE, Set.of("node-a", "node-b"));
        try (var owner = prepared.rig();
             var member = MemberProcess.start("frozen-owner", FrozenClaimHolder.class, Map.of(),
                     url, owner.database(), FROZEN.bootId());
             var taker = CachePipelineRig.attach(url, owner.database(), TAKER.bootId())) {
            member.awaitLine("HOLDING", Duration.ofSeconds(60));
            member.freeze();
            assertThat(owner.transactionsOpenBy(FROZEN.bootId())).hasSize(1);
            while (taker.leaseRemainingMillis() > 0) sleep(100);

            long started = System.nanoTime();
            var attempt = taker.claims().acquirePipeline(CachePipelineRig.SCOPE, TAKER, 1, LEASE);
            Duration took = Duration.ofNanos(System.nanoTime() - started);

            assertThat(attempt.acquired()).isTrue();
            assertThat(attempt.claim().owner()).isEqualTo(TAKER);
            assertThat(took).isLessThan(LEASE.plusSeconds(5));
            assertThat(owner.transactionsOpenBy(FROZEN.bootId())).isEmpty();

            // Thawed, the old owner's write cannot land behind the new owner's back.
            member.thaw();
            member.send("commit");
            assertThat(member.awaitLine("", Duration.ofSeconds(30))).startsWith("REFUSED");
            assertThat(owner.claimRow()).doesNotContainKey("heldWrites");
        }
    }

    @Test
    void aLiveOwnersTransactionsAreLeftAlone() {
        String url = MONGO.getReplicaSetUrl();
        var prepared = CachePipelineRig.prepare(url, FROZEN, Duration.ofMinutes(5), Set.of("node-a", "node-b"));
        try (var owner = prepared.rig();
             var member = MemberProcess.start("live-owner", FrozenClaimHolder.class, Map.of(),
                     url, owner.database(), FROZEN.bootId());
             var taker = CachePipelineRig.attach(url, owner.database(), TAKER.bootId())) {
            member.awaitLine("HOLDING", Duration.ofSeconds(60));
            // A refused takeover still has to answer: run it where a hang would fail the test, not stall it.
            var attempt = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> taker.claims().acquirePipeline(CachePipelineRig.SCOPE, TAKER, 1, LEASE));
            sleep(2_000);
            assertThat(owner.transactionsOpenBy(FROZEN.bootId())).hasSize(1);
            member.send("commit");
            assertThat(member.awaitLine("", Duration.ofSeconds(30))).isEqualTo("COMMITTED");
            assertThat(attempt.join().acquired()).isFalse();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
