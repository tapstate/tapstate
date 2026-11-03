package io.tapstate.app;

import com.hazelcast.cluster.*;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/** Controlled original detector snapshots traverse the actual CONFIG authority and episode transaction. */
@RequiresDocker
class AlertNodeRecoveryIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void onlyTheSameRegisteredMemberCanRecoverTheOriginalDetectorFault() {
        AlertEvaluationHostIT.withFixture(f->{
            var scope=f.storeHost().scope();var setup=f.storeHost().setup();
            var cache=f.configuration().bindings().cachePreparation().orElseThrow().authScope();
            var claims=f.cache().workloadClaims(f.business());
            var ownClaim=claims.read(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.NODE_SESSION,"node-a")).orElseThrow().claim();
            var peerClaim=claims.acquire(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.NODE_SESSION,"node-b"),new WorkloadOwner("node-b","boot-b"),1,Duration.ofSeconds(60)).claim();
            var own=new Registration(new Key(scope,"node-a","boot-a"),setup,cache,WorkloadClaimFence.from(ownClaim),UUID.randomUUID().toString(),1);
            var peer=new Registration(new Key(scope,"node-b","boot-b"),setup,cache,WorkloadClaimFence.from(peerClaim),UUID.randomUUID().toString(),1);
            var lifecycle=f.configuration().alertNodeLifecycles(scope,setup,()->{});lifecycle.register(peer,f.clock().instant(),()->{});
            var member=mock(HazelcastInstance.class);var cluster=mock(Cluster.class);var membership=mock(ClusterMembershipGate.class);
            var live=new LinkedHashSet<Member>();var ownMember=ClusterNodeAlertSourceTest.member(own);var peerMember=ClusterNodeAlertSourceTest.member(peer);live.add(ownMember);live.add(peerMember);
            when(member.getCluster()).thenReturn(cluster);when(cluster.addMembershipListener(any())).thenReturn(UUID.randomUUID());
            when(cluster.getMembers()).thenAnswer(call->Set.copyOf(live));
            when(membership.committed()).thenReturn(new ClusterMembership(scope.clusterId(),1,Set.of("node-a","node-b")));
            when(membership.visibleNodeIds()).thenAnswer(call->live.stream().map(ClusterMembershipGate::stableIdOf).collect(java.util.stream.Collectors.toSet()));
            var reference=new AtomicReference<PipelineAlertRuntime>();
            var signals=new BuiltinResourceSignals(reference::get,()->f.configuration().bindings().load().orElseThrow(),cache,()->{},f.clock());
            var host=new AlertStore.Host(scope,setup,AlertChannelPolicy.Deployment.ON_PREM,()->{},null,null,signals::resourceEvidence,Set.of());
            var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"node-recovery"),host.deployment(),f.clock().instant(),"operator");
            var rule=new AlertConfiguration.Rule(header,"Node","cluster.node-unavailable",1,new AlertConfiguration.Resources(Set.of("cluster-node"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
            store.saveDefinition(null,rule,AlertNotificationPlannerIT.access());
            var evaluator=new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),host.deployment(),f.clock(),"node-observer",()->{});
            try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(store,evaluator,BuiltinAlarmTypes.catalog(),null,null,Set.of("cluster-member-health","pipeline-lifecycle")),
                    f.configuration().intents(),(intent,fence)->Optional.empty(),()->{},f.clock());
                    var source=new ClusterNodeAlertSource(member,membership,runtime,signals,()->{},f.clock(),()->lifecycle,()->own,registration->()->{})) {
                reference.set(runtime);runtime.observerTick(source::tick);tick(runtime);
                var key=new AlarmKey(scope,peer.key().resource(),rule.typeId(),header.identity().incarnation());
                live.remove(peerMember);f.now().set(f.clock().instant().plusSeconds(1));tick(runtime);
                var failed=store.state(key,f.reads()).orElseThrow().value();assertThat(failed.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
                var foreign=mock(Member.class);when(foreign.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("node-b");
                when(foreign.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("boot-b");when(foreign.getUuid()).thenReturn(UUID.randomUUID());
                live.add(foreign);f.now().set(f.clock().instant().plusSeconds(1));tick(runtime);
                assertThat(store.state(key,f.reads()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
                live.remove(foreign);live.add(peerMember);f.now().set(f.clock().instant().plusSeconds(1));tick(runtime);
                var recovered=store.state(key,f.reads()).orElseThrow().value();assertThat(recovered.snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
                assertThat(recovered.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.RECOVERED);
                assertThat(recovered.episode().originalBinding()).isEqualTo(failed.episode().originalBinding());
            }
        });
    }
    private static void tick(PipelineAlertRuntime runtime){runtime.tick();await().atMost(Duration.ofSeconds(10)).until(()->runtime.health().queuedInputs()==0);}
}
