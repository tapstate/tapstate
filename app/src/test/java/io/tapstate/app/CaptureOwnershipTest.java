package io.tapstate.app;

import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CaptureOwnershipTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofHours(1);

    @Test
    void twoMemberBaselineReadsTwiceWithoutAClaimAndOnceWithCaptureOwnership() {
        InMemoryArtifactStore artifacts = artifactsWith("p");
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        AtomicInteger baselineTails = new AtomicInteger();
        CaptureStarter unclaimed = (spec, passthrough) -> {
            baselineTails.incrementAndGet();
            return run(() -> { });
        };
        StoreBackedPipelineCaptureCoordinator oldNodeA = new StoreBackedPipelineCaptureCoordinator(
                store, unclaimed, new SrsCoordinator(store.meta()), new SnapshotBuffer());
        StoreBackedPipelineCaptureCoordinator oldNodeB = new StoreBackedPipelineCaptureCoordinator(
                store, unclaimed, new SrsCoordinator(store.meta()), new SnapshotBuffer());

        oldNodeA.startCapture("p");
        oldNodeB.startCapture("p");
        assertThat(baselineTails).as("before ownership, both member-local coordinators read the source").hasValue(2);
        oldNodeA.stopCapture("p", false);
        oldNodeB.stopCapture("p", false);

        MemoryClaims raw = new MemoryClaims();
        ClusterMembershipGate gate = eligibleGate();
        AtomicInteger claimedTails = new AtomicInteger();
        CaptureAttacher claimed = (spec, passthrough, startTail) -> {
            if (startTail) {
                claimedTails.incrementAndGet();
            }
            return run(() -> { });
        };
        StoreBackedPipelineCaptureCoordinator nodeA = managed(
                store, claimed, gate, raw, new WorkloadOwner("node-a", "boot-a"));
        StoreBackedPipelineCaptureCoordinator nodeB = managed(
                store, claimed, gate, raw, new WorkloadOwner("node-b", "boot-b"));

        nodeA.startCapture("p");
        nodeB.startCapture("p");

        assertThat(claimedTails).as("the same source/read contract has one capture owner").hasValue(1);
        assertThat(raw.current.key().type()).isEqualTo(WorkloadClaimType.CAPTURE);
        assertThat(raw.current.key().resourceId())
                .startsWith("capture-")
                .isNotEqualTo("p");
        long firstGeneration = raw.current.claimGeneration();

        nodeB.stopCapture("p", false);
        nodeA.stopCapture("p", false);

        nodeB.startCapture("p");
        assertThat(claimedTails).as("the next owner opens one replacement tail").hasValue(2);
        assertThat(raw.current.owner()).isEqualTo(new WorkloadOwner("node-b", "boot-b"));
        assertThat(raw.current.claimGeneration()).isEqualTo(firstGeneration + 1);
        nodeB.stopCapture("p", false);
    }

    @Test
    void twoPipelinesOnOneMemberShareTheTailUntilTheLastAttachmentStops() {
        InMemoryArtifactStore artifacts = artifactsWith("p", "q");
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        ClusterMembershipGate gate = eligibleGate();
        MemoryClaims raw = new MemoryClaims();
        AtomicInteger tails = new AtomicInteger();
        AtomicInteger attachments = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        CaptureAttacher attacher = (spec, passthrough, startTail) -> {
            if (startTail) {
                tails.incrementAndGet();
                return run(closed::incrementAndGet);
            }
            attachments.incrementAndGet();
            return run(() -> { });
        };
        StoreBackedPipelineCaptureCoordinator coordinator = managed(
                store, attacher, gate, raw, new WorkloadOwner("node-a", "boot-a"));

        coordinator.startCapture("p");
        coordinator.startCapture("q");

        assertThat(tails).hasValue(1);
        assertThat(attachments).hasValue(1);

        coordinator.stopCapture("p", false);
        assertThat(closed).as("another pipeline still references this capture").hasValue(0);
        coordinator.stopCapture("q", false);
        assertThat(closed).as("the last local attachment releases the shared tail").hasValue(1);
    }

    private static StoreBackedPipelineCaptureCoordinator managed(
            InMemoryStorePort store,
            CaptureAttacher attacher,
            ClusterMembershipGate gate,
            MemoryClaims raw,
            WorkloadOwner owner) {
        CaptureOwnership ownership = new CaptureOwnership(
                "cluster-a", owner, gate, new ClusterWorkloadClaims(raw, gate), TTL);
        return new StoreBackedPipelineCaptureCoordinator(
                store, attacher, new SrsCoordinator(store.meta()), new SnapshotBuffer(), ownership, RENEW);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static InMemoryArtifactStore artifactsWith(String... pipelineIds) {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(
                "orders-source", null, "mysql", Map.of("host", "db.internal"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        for (String pipelineId : pipelineIds) {
            artifacts.save(new PipelineResource(
                    pipelineId, null, List.of(SourceRef.spec("orders-source", true)), null, null,
                    new ServeBlock.Inline(
                            null, FromRef.literal("orders-source"),
                            List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        }
        return artifacts;
    }

    private static CaptureRun run(Runnable close) {
        return new CaptureRun(
                Optional.empty(), false, 0, Optional.empty(), Optional.of(close::run), new CaptureHealth());
    }

    private static final class MemoryClaims implements WorkloadClaimStore {
        private WorkloadClaim current;

        @Override
        public synchronized WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            Instant now = Instant.now();
            if (current == null || current.leaseUntil().compareTo(now) <= 0 || current.owner().equals(owner)) {
                long generation = current == null || current.owner().equals(owner)
                        ? current == null ? 1 : current.claimGeneration()
                        : current.claimGeneration() + 1;
                current = new WorkloadClaim(
                        key, owner, generation, 0, topologyRevision, now.plus(ttl));
                return WorkloadClaimAttempt.acquired(current);
            }
            return WorkloadClaimAttempt.refused(current);
        }

        @Override
        public synchronized Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            if (!expected.equals(current) || current.leaseUntil().isBefore(Instant.now())) {
                return Optional.empty();
            }
            current = new WorkloadClaim(
                    current.key(), current.owner(), current.claimGeneration(), current.executionGeneration(),
                    current.topologyRevision(), Instant.now().plus(ttl));
            return Optional.of(current);
        }

        @Override
        public synchronized boolean release(WorkloadClaim expected) {
            if (!expected.equals(current)) {
                return false;
            }
            current = new WorkloadClaim(
                    current.key(), current.owner(), current.claimGeneration(), current.executionGeneration(),
                    current.topologyRevision(), Instant.EPOCH);
            return true;
        }

        @Override
        public synchronized Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision) {
            return Optional.empty();
        }

        @Override
        public synchronized Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.ofNullable(current)
                    .map(claim -> new WorkloadClaimReading(claim, Duration.between(Instant.now(), claim.leaseUntil())));
        }
    }
}
