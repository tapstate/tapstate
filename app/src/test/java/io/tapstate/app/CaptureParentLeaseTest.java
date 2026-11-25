package io.tapstate.app;

import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class CaptureParentLeaseTest {
    final WorkloadOwner owner = new WorkloadOwner("node", "boot");
    final WorkloadClaim node = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION,"node"), owner,1,0,1,Instant.now().plusSeconds(30));
    final WorkloadClaim capture = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE,"capture"), owner,1,0,1,Instant.now().plusSeconds(30));
    final CaptureOwnership ownership = mock(CaptureOwnership.class);
    final AtomicReference<Optional<WorkloadClaimFence>> parent = new AtomicReference<>(Optional.of(WorkloadClaimFence.from(node)));
    final AtomicReference<WorkloadClaimFence> current = new AtomicReference<>(WorkloadClaimFence.from(capture));
    CaptureClaimLease open() { return CaptureClaimLease.followingParent(ownership,capture,Duration.ofHours(1),WorkloadClaimFence.from(node),parent::get,current::get); }

    @Test void actualNodeSessionProofExpiresWithoutBeingReplacedByCaptureRenewal() {
        var raw = new InMemoryWorkloadClaimStore();
        var actualNode = raw.acquire(node.key(), owner, 1, Duration.ofSeconds(30)).claim();
        var actualCapture = raw.acquire(capture.key(), owner, 1, Duration.ofSeconds(30)).claim();
        var clock = new AtomicLong();
        var nodeLost = new AtomicInteger();
        try (var session = new NodeSessionLease(raw, actualNode, 0, Duration.ofSeconds(30),
                Duration.ofHours(1), nodeLost::incrementAndGet, clock::get)) {
            var original = WorkloadClaimFence.from(actualNode);
            try(var lease = CaptureClaimLease.followingParent(ownership, actualCapture, Duration.ofHours(1),
                    original, session::currentFence, () -> raw.read(actualCapture.key()).filter(WorkloadClaimReading::leased)
                            .map(r -> WorkloadClaimFence.from(r.claim())).orElse(null))) {
                var stopped = new AtomicInteger();lease.onLost(stopped::incrementAndGet);
                lease.renew();assertThat(stopped).hasValue(0);
                clock.set(Duration.ofSeconds(31).toNanos());
                lease.renew();assertThat(stopped).hasValue(1);assertThat(session.currentFence()).isEmpty();
                assertThat(lease.retain(() -> fail("expired parent"))).isFalse();
                verify(ownership,never()).renew(any());lease.closeAfterPhysicalExit();verify(ownership).release(actualCapture);
            }
        }
    }
    @Test void followsExactParentWithoutIndependentRenewalButReallyReleases() {
        try(var lease=open()) {
            for(int i=0;i<5;i++)lease.renew();
            var count=new AtomicInteger(); assertThat(lease.retain(count::incrementAndGet)).isTrue(); assertThat(count).hasValue(1);
            verify(ownership,never()).renew(any());
        }
        verify(ownership).release(capture);
    }
    @Test void demandLossReleasesAndStopsExactlyOnce() {
        var stopped=new AtomicInteger(); var lease=open();lease.onLost(stopped::incrementAndGet);lease.whileNeeded(()->false);
        lease.renew();lease.renew();assertThat(stopped).hasValue(1);verify(ownership).release(capture);
        assertThat(lease.retain(()->fail("closed"))).isFalse();lease.close();
    }
    @Test void missingParentStopsAndLateLossBindingIsAnswered() {
        var lease=open();parent.set(Optional.empty());lease.renew();var stopped=new AtomicInteger();lease.onLost(stopped::incrementAndGet);
        assertThat(stopped).hasValue(1);assertThat(lease.retain(()->fail("closed"))).isFalse();verify(ownership,never()).renew(any());lease.closeAfterPhysicalExit();verify(ownership).release(capture);
    }
    @Test void changedParentGenerationOrBootCannotBeAdopted() {
        for(var replacement:List.of(new WorkloadClaimFence(node.key(),owner,2,0,1), new WorkloadClaimFence(node.key(),new WorkloadOwner("node","later"),1,0,1))) {
            parent.set(Optional.of(WorkloadClaimFence.from(node)));var lease=open();var stopped=new AtomicInteger();lease.onLost(stopped::incrementAndGet);
            parent.set(Optional.of(replacement));lease.renew();assertThat(stopped).hasValue(1);lease.closeAfterPhysicalExit();
        }
    }
    @Test void captureGuardRemainsMandatoryAndCheckedOnRetain() {
        var lease=open();current.set(new WorkloadClaimFence(capture.key(),owner,2,0,1));
        assertThatThrownBy(()->lease.retain(()->fail("unverified"))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        var stopped=new AtomicInteger();lease.onLost(stopped::incrementAndGet);lease.renew();assertThat(stopped).hasValue(1);lease.closeAfterPhysicalExit();
    }
    @Test void nullUnfencedAndFakeClaimsDoNotOpen() {
        assertThatThrownBy(()->CaptureClaimLease.followingParent(ownership,null,Duration.ofHours(1),WorkloadClaimFence.from(node),parent::get,current::get)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(()->CaptureClaimLease.followingParent(ownership,node,Duration.ofHours(1),WorkloadClaimFence.from(node),parent::get,current::get)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        current.set(null);assertThatThrownBy(this::open).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        parent.set(Optional.empty());assertThatThrownBy(this::open).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
}
