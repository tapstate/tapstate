package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromClause;
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
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ConsumerOffset;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CaptureOwnershipTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofHours(1);

    private static final SourceResource SOURCE = new SourceResource(
            "orders-source", null, "mysql", Map.of("host", "db.internal"), SourceMode.CDC,
            List.of(TableRef.literal("orders")), null, null);

    /** The same source declaring a second table, for the cases where a pipeline reads only one of them. */
    private static final SourceResource TWO_TABLE_SOURCE = new SourceResource(
            "orders-source", null, "mysql", Map.of("host", "db.internal"), SourceMode.CDC,
            List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null);

    /** The chain every pipeline here reads, whichever member drives it. */
    private static final String CHAIN = SourceCaptureResolution.of(SOURCE).chainId().value();

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
                opensTheRing(store);
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

    @Test
    void aJoinedPipelineSeesClaimLossWhileItsSharedTailIsStillClosing() throws Exception {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p", "q"));
        MemoryClaims claims = new MemoryClaims();
        ClusterMembershipGate gate = eligibleGate();
        CaptureOwnership ownership = new CaptureOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate,
                new ClusterWorkloadClaims(claims, gate), TTL);
        CountDownLatch tailCloseEntered = new CountDownLatch(1);
        CountDownLatch releaseTailClose = new CountDownLatch(1);
        CaptureAttacher attacher = (spec, passthrough, startTail) -> run(() -> {
            if (!startTail) {
                return;
            }
            tailCloseEntered.countDown();
            try {
                if (!releaseTailClose.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("tail close was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("tail close was interrupted", interrupted);
            }
        });
        StoreBackedPipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, attacher, new SrsCoordinator(store.meta()), new SnapshotBuffer(),
                ownership, Duration.ofMillis(20));
        try {
            coordinator.startCapture("p");
            coordinator.startCapture("q");
            synchronized (claims) {
                claims.release(claims.current);
            }

            assertThat(tailCloseEntered.await(5, TimeUnit.SECONDS))
                    .as("the renewal must detect claim loss and enter the slow tail close").isTrue();
            assertThat(coordinator.captureFailure("q"))
                    .as("the joined pipeline must see claim loss before tail close returns")
                    .hasValueSatisfying(failure -> assertThat(failure)
                            .isInstanceOfSatisfying(TapstateException.class,
                                    coded -> assertThat(coded.code()).isEqualTo(CaptureError.CLAIM_LOST)));
        } finally {
            releaseTailClose.countDown();
            coordinator.stopCapture("p", false);
            coordinator.stopCapture("q", false);
            coordinator.close();
        }
    }

    @Test
    void theReadFaceNamesTheSameCaptureTheRunningCoordinatorClaimed() {
        // The topology says who owns a pipeline's captures, and it works the identities out from the
        // stored contract rather than asking whoever is running them -- so that every member answers the
        // same. That only reports ownership if the id it derives is the id the claim is actually filed
        // under: derive it even slightly differently and the read face looks up a claim nobody ever
        // took, which is indistinguishable from a capture nobody owns.
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p"));
        MemoryClaims raw = new MemoryClaims();
        StoreBackedPipelineCaptureCoordinator owner = managed(
                store, (spec, passthrough, startTail) -> run(() -> { }), eligibleGate(), raw,
                new WorkloadOwner("node-a", "boot-a"));
        owner.startCapture("p");

        try {
            assertThat(new StoreBackedPipelineCaptures(store).captureIds("p"))
                    .as("the id the claim was filed under, worked out from the artifacts alone")
                    .containsExactly(raw.current.key().resourceId());
        } finally {
            owner.stopCapture("p", false);
        }
    }

    @Test
    void theReadFaceNamesTheCaptureOfOnlyTheTablesThePipelineReads() {
        // A capture's identity covers the streams it reads, and a start reads only the tables the pipeline
        // addresses, not every table its source declares. The read face has to narrow the same way: over a
        // source of two tables and a pipeline reading one, the whole source's id is a claim nobody took.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(TWO_TABLE_SOURCE);
        artifacts.save(pipelineServing("p", "orders-source.orders"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        MemoryClaims raw = new MemoryClaims();
        StoreBackedPipelineCaptureCoordinator owner = managed(
                store, (spec, passthrough, startTail) -> run(() -> { }), eligibleGate(), raw,
                new WorkloadOwner("node-a", "boot-a"));
        owner.startCapture("p");

        try {
            assertThat(new StoreBackedPipelineCaptures(store).captureIds("p"))
                    .as("the id the claim was filed under, for the one table the pipeline reads")
                    .containsExactly(raw.current.key().resourceId());
        } finally {
            owner.stopCapture("p", false);
        }
    }

    @Test
    void aPipelineAddressingATableItsSourceNeverDiscoveredNamesNoCaptureRatherThanFailingTheRead() {
        // Such a pipeline is refused at its start, so it holds no capture on that source. The read face
        // answers for every pipeline at once, and one that cannot resolve must not take that answer down.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(TWO_TABLE_SOURCE);
        artifacts.save(pipelineServing("p", "orders-source.invoices"));

        assertThat(new StoreBackedPipelineCaptures(new InMemoryStorePort(artifacts)).captureIds("p"))
                .as("no capture can exist for a reference its source cannot resolve")
                .isEmpty();
    }

    @Test
    void aPipelineWhoseArtifactsAreGoneNamesNoCaptureRatherThanInventingOne() {
        assertThat(new StoreBackedPipelineCaptures(new InMemoryStorePort(new InMemoryArtifactStore()))
                        .captureIds("p"))
                .as("there is no contract to derive an id from, and an invented one would file a "
                        + "reader's attention under a claim nobody holds")
                .isEmpty();
    }

    /**
     * A pipeline driven by a member that does not hold its capture still attaches, for its own load.
     *
     * <p>How a pipeline reads is its own settings' and its own record's to say, wherever it is driven. The
     * capture claim decides who tails the source and nothing else; a pipeline owed its initial load is owed
     * it on any member. A driver that left the pipeline unattached because the claim was held elsewhere
     * ran it with no load at all: every row the source held before it started missing from its target,
     * the run healthy, and nothing reported.
     */
    @Test
    void aPipelineDrivenWhereTheCaptureIsNotHeldAttachesForItsOwnLoadAndOpensNoTail() {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p", "q"));
        MemoryClaims claims = new MemoryClaims();
        List<String> starts = new ArrayList<>();
        Member a = new Member("node-a", store, claims, TTL, starts);
        Member b = new Member("node-b", store, claims, TTL, starts);

        a.captures.startCapture("p");
        b.captures.startCapture("q");

        assertThat(starts)
                .as("one tail, on the member holding the capture, and the pipeline driven by the other "
                        + "member attached there rather than left with nothing")
                .containsExactly("node-a p opened the tail", "node-b q attached");
    }

    /**
     * Clearing the state of the last pipeline one member runs leaves the chain to a pipeline on another.
     *
     * <p>A member's own release answers for the pipelines it runs and for no others. A pipeline another
     * member drives over the same chain keeps its load and its cursor on that chain's record, and a drop
     * made because this member had nobody left would take them from it: its next start would read its
     * whole source again, and nothing would say why.
     */
    @Test
    void clearingTheLastPipelineOnTheHoldingMemberLeavesTheChainToAPipelineOnAnother() {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p", "q"));
        MemoryClaims claims = new MemoryClaims();
        List<String> starts = new ArrayList<>();
        Member a = new Member("node-a", store, claims, TTL, starts);
        Member b = new Member("node-b", store, claims, TTL, starts);
        a.captures.startCapture("p");
        b.captures.startCapture("q");
        // What q's own run leaves on the record as it reads: its cursor, and its place on the chain with it.
        store.meta().upsertConsumerOffset(CHAIN, new ConsumerOffset("q", Map.of(), null));

        a.captures.stopCapture("p", true);

        assertThat(store.meta().read(CHAIN))
                .as("the chain is still there for the pipeline reading it on the other member")
                .isPresent();
        assertThat(consumersOn(store)).containsExactly("q");

        b.captures.stopCapture("q", true);

        assertThat(store.meta().read(CHAIN))
                .as("and it goes with the last pipeline on it, whichever member ran that one")
                .isEmpty();
    }

    /**
     * A member that takes a capture over runs its tail for the pipelines of its own that were reading
     * that capture already, not only for the one whose start took the claim.
     */
    @Test
    void aMemberThatTakesACaptureOverKeepsItsTailForThePipelinesThatHadJoinedIt() {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p", "q", "r"));
        MemoryClaims claims = new MemoryClaims();
        List<String> starts = new ArrayList<>();
        Member a = new Member("node-a", store, claims, TTL, starts);
        Member b = new Member("node-b", store, claims, TTL, starts);
        a.captures.startCapture("p");
        b.captures.startCapture("q");
        a.captures.stopCapture("p", false);

        b.captures.startCapture("r");
        assertThat(starts).last().isEqualTo("node-b r opened the tail");

        b.captures.stopCapture("r", false);
        assertThat(b.tailsClosed)
                .as("q still reads what this tail writes, so it outlasts the pipeline that took the claim")
                .hasValue(0);
        b.captures.stopCapture("q", false);
        assertThat(b.tailsClosed).as("and closes with the last pipeline reading it").hasValue(1);
    }

    /**
     * A holder that took a capture's claim and never opened its ring leaves the capture to the member
     * waiting on it, once its lease runs out.
     */
    @Test
    void aPipelineWaitingOnAHolderThatNeverOpenedItsRingTakesTheCaptureOnceTheLeaseRunsOut() {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("q"));
        MemoryClaims claims = new MemoryClaims();
        // A member that took the claim and died before opening anything: nothing renews it.
        claims.acquire(captureKey(store, "q"), Member.owner("node-a"), 7, Duration.ofMillis(300));
        List<String> starts = new ArrayList<>();
        Member b = new Member("node-b", store, claims, Duration.ofSeconds(10), starts);

        startAsConvergenceDoes(b.captures, "q");

        assertThat(starts).containsExactly("node-b q opened the tail");
        assertThat(claims.current.owner()).isEqualTo(Member.owner("node-b"));
    }

    /**
     * A pipeline started while the member holding its capture is still opening the ring attaches as soon
     * as the ring is there.
     */
    @Test
    void aPipelineStartedWhileTheHolderIsStillOpeningItsRingAttachesOnceItIsOpen() throws Exception {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("q"));
        MemoryClaims claims = new MemoryClaims();
        claims.acquire(captureKey(store, "q"), Member.owner("node-a"), 7, Duration.ofMinutes(5));
        List<String> starts = new ArrayList<>();
        Member b = new Member("node-b", store, claims, Duration.ofSeconds(10), starts);
        Thread holderOpening = new Thread(() -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            opensTheRing(store);
        });
        holderOpening.start();

        startAsConvergenceDoes(b.captures, "q");
        holderOpening.join();

        assertThat(starts).containsExactly("node-b q attached");
    }

    /**
     * A pipeline with no ring to read and no way to take its capture over is refused, with a code, once
     * the wait that would have let the claim move is over -- not left running over nothing.
     */
    @Test
    void aPipelineWithNoRingToReadAndNoWayToTakeTheCaptureOverIsRefusedWithACode() {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("q"));
        MemoryClaims claims = new MemoryClaims();
        claims.acquire(captureKey(store, "q"), Member.owner("node-a"), 7, Duration.ofMinutes(5));
        List<String> starts = new ArrayList<>();
        Member b = new Member("node-b", store, claims, Duration.ofMillis(600), starts);

        assertThatThrownBy(() -> startAsConvergenceDoes(b.captures, "q"))
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(CaptureError.NO_RING_TO_ATTACH));
        assertThat(starts).isEmpty();
    }

    /**
     * Starts {@code pipelineId} the way convergence does: a start given back is started again on the next
     * pass, until it is carried out or refused.
     */
    private static void startAsConvergenceDoes(
            StoreBackedPipelineCaptureCoordinator captures, String pipelineId) {
        long giveUp = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) {
            try {
                captures.startCapture(pipelineId);
                return;
            } catch (RingNotOpenYet notYet) {
                if (System.nanoTime() - giveUp >= 0) {
                    throw new AssertionError("the start was still being given back after 30 seconds", notYet);
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted between passes", interrupted);
                }
            }
        }
    }

    /**
     * A start that finds its capture held elsewhere and no ring open yet gives the pass back instead of
     * waiting for one. The wait was a sleep of up to a whole lease inside the start, on the one thread that
     * converges every pipeline here, holding the lock that this member's claim-loss handling, stops,
     * takeovers and failure checks all take: a holder that died between taking the claim and opening its
     * ring stalled all of them for that long.
     */
    @Test
    void aStartThatFindsTheRingNotOpenYetHoldsNeitherItsCallerNorTheCoordinator() throws Exception {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("q"));
        MemoryClaims claims = new MemoryClaims();
        // The holder has taken the claim and is still opening its ring; it keeps its lease throughout.
        claims.acquire(captureKey(store, "q"), Member.owner("node-a"), 7, Duration.ofMinutes(5));
        List<String> starts = Collections.synchronizedList(new ArrayList<>());
        Member b = new Member("node-b", store, claims, Duration.ofSeconds(5), starts);
        CountDownLatch startReturned = new CountDownLatch(1);
        AtomicReference<Throwable> startEndedWith = new AtomicReference<>();
        Thread convergence = new Thread(() -> {
            try {
                b.captures.startCapture("q");
            } catch (Throwable ended) {
                startEndedWith.set(ended);
            }
            startReturned.countDown();
        });
        convergence.start();
        Thread.sleep(100);

        long asked = System.nanoTime();
        b.captures.captureFailure("q");
        Duration waitedBehindTheStart = Duration.ofNanos(System.nanoTime() - asked);

        try {
            assertThat(waitedBehindTheStart)
                    .as("nothing else on this member's coordinator waits behind a start")
                    .isLessThan(Duration.ofMillis(500));
            assertThat(startReturned.await(1, TimeUnit.SECONDS))
                    .as("the start returns rather than waiting the lease out")
                    .isTrue();
            assertThat(startEndedWith.get())
                    .as("and is not refused: the ring may yet open, and the next pass looks again")
                    .isNotInstanceOf(TapstateException.class);
            assertThat(starts).isEmpty();
        } finally {
            convergence.join(Duration.ofSeconds(10).toMillis());
        }
    }

    /**
     * A start gives the pass back before opening anything, so nothing it opened has to be taken down again
     * and opened afresh on the next pass. A pipeline over two sources whose second capture is not ready yet
     * opened the first source's tail -- and read its load -- then closed it when the second one gave up.
     */
    @Test
    void aStartGivingThePassBackHasOpenedNothingAndHoldsNoClaim() {
        SourceResource items = new SourceResource(
                "items-source", null, "mysql", Map.of("host", "items.internal"), SourceMode.CDC,
                List.of(TableRef.literal("items")), null, null);
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(SOURCE);
        artifacts.save(items);
        artifacts.save(new PipelineResource(
                "p", null, List.of(SourceRef.spec("orders-source", true), SourceRef.spec("items-source", true)),
                null, null,
                new ServeBlock.Inline(
                        null, FromClause.list(FromRef.literal("orders-source"), FromRef.literal("items-source")),
                        List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        List<String> captureIds = new StoreBackedPipelineCaptures(store).captureIds("p");
        WorkloadClaimKey orders = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, captureIds.get(0));
        WorkloadClaimKey itemsCapture =
                new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, captureIds.get(1));
        InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        // Another member holds the items capture and has not opened its ring; the orders capture is free.
        claims.acquire(itemsCapture, Member.owner("node-a"), 7, Duration.ofMinutes(5));
        List<String> starts = new ArrayList<>();
        Member b = new Member("node-b", store, claims, Duration.ofMillis(300), starts);

        Throwable startEndedWith = null;
        try {
            b.captures.startCapture("p");
        } catch (RuntimeException ended) {
            startEndedWith = ended;
        }

        assertThat(startEndedWith).as("the start is given back, not carried out").isNotNull();
        assertThat(starts).as("and nothing was opened for it").isEmpty();
        assertThat(claims.read(orders).filter(WorkloadClaimReading::leased))
                .as("and the capture it could have taken is not left held")
                .isEmpty();
    }

    /** A read with no tail has no ring to wait for, so it attaches at once. */
    @Test
    void aSnapshotOnlyReadWaitsForNoRing() {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith(ReadMode.SNAPSHOT_ONLY, "q"));
        MemoryClaims claims = new MemoryClaims();
        claims.acquire(captureKey(store, "q"), Member.owner("node-a"), 7, Duration.ofMinutes(5));
        List<String> starts = new ArrayList<>();
        Member b = new Member("node-b", store, claims, Duration.ofSeconds(30), starts);

        long started = System.nanoTime();
        b.captures.startCapture("q");

        assertThat(starts).containsExactly("node-b q attached");
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
    }

    /**
     * When the member holding a capture stops the last of its own pipelines on it, a member whose
     * pipelines still read that capture takes the tail over, so they go on receiving changes.
     *
     * <p>Nothing else would: the holder lets the claim go with its last pipeline, and the pipelines on the
     * other member are running, healthy, and reading a ring nobody writes any more -- a pipeline whose
     * changes stopped because a different pipeline somewhere else was stopped.
     */
    @Test
    void aMemberWhosePipelinesStillReadACaptureTakesItsTailOverWhenTheHolderLetsItGo() throws Exception {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p", "q"));
        MemoryClaims claims = new MemoryClaims();
        List<String> starts = Collections.synchronizedList(new ArrayList<>());
        Duration look = Duration.ofMillis(100);
        Member a = new Member("node-a", store, claims, TTL, look, starts);
        Member b = new Member("node-b", store, claims, TTL, look, starts);
        a.captures.startCapture("p");
        b.captures.startCapture("q");

        a.captures.stopCapture("p", false);

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!starts.contains("node-b q opened the tail") && System.nanoTime() - deadline < 0) {
            Thread.sleep(20);
        }
        assertThat(List.copyOf(starts))
                .as("the member still reading the capture opened the tail its pipeline reads")
                .containsExactly("node-a p opened the tail", "node-b q attached", "node-b q opened the tail");
        assertThat(claims.current.owner()).isEqualTo(Member.owner("node-b"));

        b.captures.stopCapture("q", false);
        assertThat(b.tailsClosed).as("and that tail closes with the last pipeline reading it").hasValue(1);
    }

    /** A member holding nothing it has joined does not take anything over while the holder still tails. */
    @Test
    void aMemberTakesNothingOverWhileTheHolderStillTails() throws Exception {
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p", "q"));
        MemoryClaims claims = new MemoryClaims();
        List<String> starts = Collections.synchronizedList(new ArrayList<>());
        Duration look = Duration.ofMillis(100);
        Member a = new Member("node-a", store, claims, TTL, look, starts);
        Member b = new Member("node-b", store, claims, TTL, look, starts);
        a.captures.startCapture("p");
        b.captures.startCapture("q");

        Thread.sleep(look.multipliedBy(5).toMillis());

        assertThat(List.copyOf(starts)).containsExactly("node-a p opened the tail", "node-b q attached");
        assertThat(claims.current.owner()).isEqualTo(Member.owner("node-a"));
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
        return artifactsWith(ReadMode.CDC_ONLY, pipelineIds);
    }

    private static InMemoryArtifactStore artifactsWith(ReadMode readMode, String... pipelineIds) {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(SOURCE);
        for (String pipelineId : pipelineIds) {
            artifacts.save(new PipelineResource(
                    pipelineId, null, List.of(SourceRef.spec("orders-source", true)), null, null,
                    new ServeBlock.Inline(
                            null, FromRef.literal("orders-source"),
                            List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                    new Settings(null, null, null, null, readMode, "earliest"), null));
        }
        return artifacts;
    }

    /** A cdc-only pipeline over the orders source that serves only what {@code from} addresses. */
    private static PipelineResource pipelineServing(String pipelineId, String from) {
        return new PipelineResource(
                pipelineId, null, List.of(SourceRef.spec("orders-source", true)), null, null,
                new ServeBlock.Inline(
                        null, FromRef.literal(from),
                        List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
    }

    /** What the member starting a tail does to the store before anybody can read its ring. */
    private static void opensTheRing(InMemoryStorePort store) {
        if (store.meta().read(CHAIN).isEmpty()) {
            store.meta().create(CHAIN, null);
        }
        store.meta().openEpoch(CHAIN);
    }

    private static List<String> consumersOn(InMemoryStorePort store) {
        return store.meta().read(CHAIN).orElseThrow().consumerOffsets().stream()
                .map(ConsumerOffset::pipelineId)
                .toList();
    }

    private static WorkloadClaimKey captureKey(InMemoryStorePort store, String pipelineId) {
        return new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE,
                new StoreBackedPipelineCaptures(store).captureIds(pipelineId).getFirst());
    }

    /**
     * One member of a cluster over the shared store: a chain coordinator and a capture coordinator of its
     * own, and a run unit standing in for the real one by doing to the chain coordinator what the real one
     * does -- open the chain and its generation for a tail, join it for an attachment, attach the consumer
     * -- and nothing else.
     */
    private static final class Member {
        private final String node;
        private final List<String> starts;
        private final SrsCoordinator chains;
        private final StoreBackedPipelineCaptureCoordinator captures;
        private final AtomicInteger tailsClosed = new AtomicInteger();

        Member(String node, InMemoryStorePort store, WorkloadClaimStore claims, Duration ttl,
                List<String> starts) {
            this(node, store, claims, ttl, RENEW, starts);
        }

        Member(String node, InMemoryStorePort store, WorkloadClaimStore claims, Duration ttl, Duration renew,
                List<String> starts) {
            this.node = node;
            this.starts = starts;
            this.chains = new SrsCoordinator(store.meta());
            ClusterMembershipGate gate = eligibleGate();
            CaptureOwnership ownership = new CaptureOwnership(
                    "cluster-a", owner(node), gate, new ClusterWorkloadClaims(claims, gate), ttl);
            this.captures = new StoreBackedPipelineCaptureCoordinator(
                    store, this::start, chains, new SnapshotBuffer(), ownership, renew);
        }

        static WorkloadOwner owner(String node) {
            return new WorkloadOwner(node, "boot-" + node);
        }

        private CaptureRun start(CaptureRunSpec spec, Consumer<Envelope> passthrough, boolean startTail) {
            starts.add(node + " " + spec.pipelineId() + (startTail ? " opened the tail" : " attached"));
            Subscription subscription = startTail ? tailsClosed::incrementAndGet : () -> { };
            if (spec.readMode() == ReadMode.SNAPSHOT_ONLY) {
                return new CaptureRun(Optional.empty(), false, 0, Optional.empty(), Optional.of(subscription),
                        new CaptureHealth());
            }
            MiningChainId chain = MiningChainId.resolve(spec.config(), spec.srsKey());
            if (startTail) {
                chains.provisionSource(spec.sourceId(), chain, spec.config().streams(), spec.retention());
            } else {
                chains.joinSource(spec.sourceId(), chain, spec.config().streams());
            }
            chains.attachConsumer(chain, spec.pipelineId());
            return new CaptureRun(Optional.of(chain), !startTail, 0, Optional.empty(), Optional.of(subscription),
                    new CaptureHealth());
        }
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
