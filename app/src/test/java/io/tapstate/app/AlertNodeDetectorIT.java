package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/** Actual Hazelcast departures and actual CONFIG node authority, without changing the business quorum. */
@RequiresDocker
class AlertNodeDetectorIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void anUnexpectedDetectorRemovalFiresWhileAnOrderlyRemovalEndsMonitoringWithoutRecovery() {
        for(boolean orderly:List.of(false,true))AlertEvaluationHostIT.withFixture(f->{
            f.now().set(Instant.now());String cluster="node-alert-"+UUID.randomUUID();
            var first=Hazelcast.newHazelcastInstance(config(cluster,"node-a","boot-a",null));
            HazelcastInstance second=null;
            try {
                second=Hazelcast.newHazelcastInstance(config(cluster,"node-b","boot-b",first.getCluster().getLocalMember().getAddress().getHost()+":"+first.getCluster().getLocalMember().getAddress().getPort()));
                var peer=second;await().atMost(Duration.ofSeconds(15)).until(()->first.getCluster().getMembers().size()==2);
                var claims=f.cache().workloadClaims(f.business());
                var nodeA=claims.read(new WorkloadClaimKey(f.storeHost().scope().clusterId(),WorkloadClaimType.NODE_SESSION,"node-a")).orElseThrow().claim();
                var nodeB=claims.acquire(new WorkloadClaimKey(f.storeHost().scope().clusterId(),WorkloadClaimType.NODE_SESSION,"node-b"),new WorkloadOwner("node-b","boot-b"),1,Duration.ofSeconds(60)).claim();
                var setup=f.storeHost().setup();var cache=f.configuration().bindings().cachePreparation().orElseThrow().authScope();
                var lifecycle=f.configuration().alertNodeLifecycles(f.storeHost().scope(),setup,()->{});
                var own=new Registration(new Key(f.storeHost().scope(),"node-a","boot-a"),setup,cache,WorkloadClaimFence.from(nodeA),first.getCluster().getLocalMember().getUuid().toString(),1);
                var remote=new Registration(new Key(f.storeHost().scope(),"node-b","boot-b"),setup,cache,WorkloadClaimFence.from(nodeB),peer.getCluster().getLocalMember().getUuid().toString(),1);
                var peerRow=lifecycle.register(remote,f.clock().instant(),()->{});
                var gate=mock(ClusterMembershipGate.class);
                when(gate.committed()).thenReturn(new ClusterMembership(setup.clusterId(),1,Set.of("node-a","node-b")));
                when(gate.visibleNodeIds()).thenAnswer(call->first.getCluster().getMembers().stream().map(ClusterMembershipGate::stableIdOf).collect(java.util.stream.Collectors.toSet()));
                var reference=new AtomicReference<PipelineAlertRuntime>();
                var signals=new BuiltinResourceSignals(reference::get,()->f.configuration().bindings().load().orElseThrow(),cache,()->{},f.clock());
                var host=new AlertStore.Host(f.storeHost().scope(),setup,AlertChannelPolicy.Deployment.ON_PREM,()->{},null,null,signals::resourceEvidence,Set.of());
                var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());
                var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,host.scope(),"node-rule"),AlertChannelPolicy.Deployment.ON_PREM,f.clock().instant(),"operator");
                var rule=new AlertConfiguration.Rule(header,"Node availability","cluster.node-unavailable",1,new AlertConfiguration.Resources(Set.of("cluster-node"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
                store.saveDefinition(null,rule,AlertNotificationPlannerIT.access());
                var evaluator=new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),AlertChannelPolicy.Deployment.ON_PREM,f.clock(),"node-observer",()->{});
                try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(store,evaluator,BuiltinAlarmTypes.catalog(),null,null,Set.of("cluster-member-health","pipeline-lifecycle")),f.configuration().intents(),(scope,fence)->Optional.empty(),()->{},f.clock());
                        var source=new ClusterNodeAlertSource(first,gate,runtime,signals,()->{},f.clock(),()->lifecycle,()->own,registration->()->{})) {
                    reference.set(runtime);runtime.observerTick(source::tick);runtime.tick();drained(runtime);
                    var key=new AlarmKey(host.scope(),remote.key().resource(),"cluster.node-unavailable",rule.header().identity().incarnation());
                    assertThat(store.state(key,f.reads())).isPresent();
                    if(orderly) {
                        lifecycle.retire(remote,peerRow.revision(),UUID.randomUUID().toString(),f.clock().instant(),()->{});peer.shutdown();
                    }else peer.getLifecycleService().terminate();
                    await().atMost(Duration.ofSeconds(15)).until(()->first.getCluster().getMembers().size()==1);
                    f.now().set(Instant.now());runtime.tick();drained(runtime);runtime.tick();drained(runtime);
                    var head=store.state(key,f.reads()).orElseThrow().value();
                    if(orderly){assertThat(head.retiredAt()).isNotNull();assertThat(head.snapshot().status()).isNotEqualTo(AlertSnapshot.Status.FIRING);}
                    else {assertThat(head.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);assertThat(head.snapshot().episode().recoveredAt()).isNull();}
                }
            }finally{if(second!=null && second.getLifecycleService().isRunning())second.shutdown();first.shutdown();}
        });
    }
    private static Config config(String cluster,String node,String boot,String join) {
        var config=new Config().setClusterName(cluster);config.setProperty("hazelcast.logging.type","slf4j");config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.operation.thread.count","2");config.setProperty("hazelcast.operation.generic.thread.count","2");
        config.getJetConfig().setEnabled(false);config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE,node).setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE,boot);
        config.getNetworkConfig().setPort(0);config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        var tcp=config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);if(join!=null)tcp.addMember(join);return config;
    }
    private static void drained(PipelineAlertRuntime runtime){await().atMost(Duration.ofSeconds(10)).until(()->runtime.health().queuedInputs()==0);}
}
