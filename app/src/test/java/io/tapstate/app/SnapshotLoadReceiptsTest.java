package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SnapshotLoadReceiptsTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","pipeline",2);
    private static WorkloadClaimFence claim(long execution) {
        return new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),
                new WorkloadOwner("a","boot-a"),1,execution,1);
    }
    @Test void actualRemoteWritersMustAllBoundAndLateCallbacksCannotCompleteTheNextRun() throws Exception {
        var first=config("snapshot-receipts-"+UUID.randomUUID(),"a");
        first.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance a=Hazelcast.newHazelcastInstance(first),b=null;
        var current=new AtomicReference<>(new ExecutionFence(SCOPE,claim(1)));
        java.util.function.Consumer<ExecutionFence> proof=value -> {if(!value.equals(current.get()))throw SnapshotLoadAcks.refused();};
        var transientRefusals=new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.Consumer<ExecutionFence> ownerProof=value -> {
            proof.accept(value);if(transientRefusals.getAndUpdate(n -> Math.max(0,n-1))>0)throw SnapshotLoadAcks.refused();
        };
        try(var owner=new SnapshotLoadReceipts(a,() -> {},ownerProof,proof)) {
            var second=config(first.getClusterName(),"b");second.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                    .addMember("127.0.0.1:"+a.getCluster().getLocalMember().getAddress().getPort());
            b=Hazelcast.newHazelcastInstance(second);
            try(var remote=new SnapshotLoadReceipts(b,() -> {},proof,proof)) {
                var original=current.get();var factory=owner.forRun(original,Map.of("orders",7L,"empty",7L));
                factory.beginRun(b,Map.of("orders",List.of("left","right"),"empty",List.of("left","right")),claim(1));
                var left=factory.resolve(a).forWriter("left");var right=factory.resolve(b).forWriter("right");
                transientRefusals.set(3);
                left.bounded("orders",SourceOrder.snapshotRow(7),claim(1));await(() -> transientRefusals.get()==0);left.bounded("empty",SourceOrder.snapshotRow(7),claim(1));
                right.bounded("orders",SourceOrder.snapshotRow(7),claim(1));
                assertThat(owner.complete(original)).isFalse();
                right.bounded("empty",SourceOrder.snapshotRow(7),claim(1));
                await(() -> owner.complete(original));
                assertThatThrownBy(() -> right.bounded("orders",SourceOrder.snapshotRow(7),claim(2))).isInstanceOf(TapstateException.class);
                current.set(new ExecutionFence(SCOPE,claim(2)));
                var replacement=owner.forRun(current.get(),Map.of("orders",8L));replacement.beginRun(b,Map.of("orders",List.of("left")),claim(2));
                assertThatThrownBy(() -> right.bounded("empty",SourceOrder.snapshotRow(7),claim(1))).isInstanceOf(TapstateException.class);
                owner.forget(original);assertThat(owner.complete(current.get())).isFalse();
                replacement.resolve(b).forWriter("left").bounded("orders",SourceOrder.snapshotRow(8),claim(2));
                await(() -> owner.complete(current.get()));
                owner.forget(current.get());assertThat(owner.complete(current.get())).isFalse();
                var local=a.getCluster().getLocalMember();
                var retired=new SnapshotLoadAcks.Identity(current.get(),new SnapshotLoadAcks.Owner("a","boot-a",local.getUuid()),Map.of("orders",8L));
                assertThat(SnapshotLoadAcks.notify(b,retired,"orders","left",SourceOrder.snapshotRow(8))).isTrue();
                assertThat(owner.complete(current.get())).isFalse();
                HazelcastInstance remoteMember=b;
                assertThatThrownBy(() -> replacement.beginRun(remoteMember,Map.of("orders",List.of("left")),claim(2))).isInstanceOf(TapstateException.class);
            }
        } finally {if(b!=null)b.shutdown();a.shutdown();}
    }
    private static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        while(!condition.getAsBoolean() && System.nanoTime()<deadline)Thread.sleep(10);
        assertThat(condition.getAsBoolean()).isTrue();
    }
    private static Config config(String cluster,String id) {
        var config=new Config().setClusterName(cluster);config.setProperty("hazelcast.partition.count","17");
        config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE,id).setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE,"boot-"+id);
        config.setProperty("hazelcast.phone.home.enabled","false");config.setProperty("hazelcast.shutdownhook.enabled","false");config.setProperty("hazelcast.logging.type","none");
        config.setProperty("hazelcast.operation.thread.count","2");config.setProperty("hazelcast.operation.generic.thread.count","2");
        config.setProperty("hazelcast.io.input.thread.count","1");config.setProperty("hazelcast.io.output.thread.count","1");
        var network=config.getNetworkConfig();network.setPort(0).setPortAutoIncrement(false);network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join=network.getJoin();join.getMulticastConfig().setEnabled(false);join.getTcpIpConfig().setEnabled(false);join.getAutoDetectionConfig().setEnabled(false);return config;
    }
}
