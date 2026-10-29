package io.tapstate.app;

import com.hazelcast.cluster.*;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ClusterNodeAlertSourceTest {
    static final Instant T=Instant.parse("2026-10-29T00:00:00Z");
    final HazelcastInstance member=mock(HazelcastInstance.class);
    final Cluster cluster=mock(Cluster.class);
    final ClusterMembershipGate membership=mock(ClusterMembershipGate.class);
    final PipelineAlertRuntime runtime=mock(PipelineAlertRuntime.class);
    final BuiltinResourceSignals signals=mock(BuiltinResourceSignals.class);
    final AlertNodeLifecycleStore store=mock(AlertNodeLifecycleStore.class);
    final LinkedHashMap<Key,Row> rows=new LinkedHashMap<>();
    final LinkedHashSet<Member> live=new LinkedHashSet<>();
    final Registration own=registration("node-a","boot-a",1);
    final Registration peer=registration("node-b","boot-b",1);
    ClusterNodeAlertSourceTest() {
        when(member.getCluster()).thenReturn(cluster);when(cluster.addMembershipListener(any())).thenReturn(UUID.randomUUID());
        when(cluster.getMembers()).thenAnswer(c->Set.copyOf(live));
        when(membership.committed()).thenReturn(new ClusterMembership("cluster",1,Set.of("node-a","node-b")));
        when(membership.businessEligible()).thenReturn(true);when(membership.visibleNodeIds()).thenReturn(Set.of("node-a","node-b"));
        when(store.register(any(),any(),any())).thenAnswer(c->{Registration registration=c.getArgument(0);var value=new Row(registration,1,T,Phase.EXPECTED,null,null);rows.put(registration.key(),value);return value;});
        when(store.list()).thenAnswer(c->List.copyOf(rows.values()));
        rows.put(peer.key(),new Row(peer,1,T,Phase.EXPECTED,null,null));live.add(member(own));live.add(member(peer));
    }
    static Registration registration(String node,String boot,long generation) {
        return new Registration(new Key(new AlarmKey.Scope("installation","cluster"),node,boot),new StorageSetupScope("cluster","installation","setup",1),
                new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,node),new WorkloadOwner(node,boot),generation,0,1),
                UUID.nameUUIDFromBytes((node+boot).getBytes(java.nio.charset.StandardCharsets.US_ASCII)).toString(),1);
    }
    static Member member(Registration original) {
        var peer=mock(Member.class);when(peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(original.key().nodeId());
        when(peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(original.key().bootId());when(peer.getUuid()).thenReturn(UUID.fromString(original.memberId()));return peer;
    }
    ClusterNodeAlertSource source() {
        return new ClusterNodeAlertSource(member,membership,runtime,signals,()->{},Clock.fixed(T.plusSeconds(2),ZoneOffset.UTC),()->store,()->own,registration->()->{});
    }
    @SuppressWarnings("unchecked") Function<AlarmKey.Resource,Optional<AlertStore.NodeEvidence>> proofs() {
        var captor=org.mockito.ArgumentCaptor.forClass(Function.class);verify(signals).nodeProofs(captor.capture());return captor.getValue();
    }
    @Test void anUnexpectedDetectorAbsenceIsBadWhileAnOrderlyLeaveHasNoRecoveryInput() {
        try(var source=source()) {
            source.tick();var proofs=proofs();clearInvocations(runtime);
            live.removeIf(value->value.getUuid().toString().equals(peer.memberId()));source.tick();
            verify(runtime).resourceInput(eq(peer.key().resource()),eq("cluster.node-unavailable"),eq(0L),anyString(),any(),argThat(input->input instanceof AlertInput.Conditions c && c.values().equals(List.of(AlertSignal.Condition.BAD))));
            assertThat(proofs.apply(peer.key().resource()).orElseThrow().expected()).isTrue();
            rows.put(peer.key(),new Row(peer,2,T,Phase.RETIRING,"67d93958-b5e1-4bb0-9273-f7bb29e48f01",T.plusSeconds(1)));
            clearInvocations(runtime);source.tick();
            assertThat(proofs.apply(peer.key().resource()).orElseThrow().expected()).isFalse();
            verify(runtime,never()).resourceInput(eq(peer.key().resource()),anyString(),anyLong(),anyString(),any(),any());
        }
    }
    @Test void aNewBootSupersedesTheOldMonitoringIdentityWithoutRecoveringIt() {
        try(var source=source()) {
            source.tick();var proofs=proofs();var previous=proofs.apply(peer.key().resource()).orElseThrow();
            var replacement=registration("node-b","boot-next",2);rows.put(replacement.key(),new Row(replacement,1,T,Phase.EXPECTED,null,null));
            live.removeIf(value->value.getUuid().toString().equals(peer.memberId()));live.add(member(replacement));clearInvocations(runtime);source.tick();
            var old=proofs.apply(peer.key().resource()).orElseThrow();assertThat(old.expected()).isFalse();assertThat(old.supersedingBootId()).isEqualTo("boot-next");
            assertThatThrownBy(previous.requireCurrent()::run).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            verify(runtime,never()).resourceInput(eq(peer.key().resource()),anyString(),anyLong(),anyString(),any(),any());
            verify(runtime).resourceInput(eq(replacement.key().resource()),anyString(),anyLong(),anyString(),any(),argThat(input->input instanceof AlertInput.Conditions c && c.values().equals(List.of(AlertSignal.Condition.GOOD))));
        }
    }
    @Test void aLostOriginalRegistrationRefusesProofAndMarksCollectionDegraded() {
        try(var source=source()) {
            source.tick();var proofs=proofs();rows.remove(own.key());clearInvocations(runtime);source.tick();
            verify(runtime).collectionUnavailable("node-lifecycle-unavailable");assertThat(proofs.apply(peer.key().resource())).isEmpty();
        }
    }
}
