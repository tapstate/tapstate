package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.srs.NativeSrsCaptureRunUnit;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeSrsRetentionTest {
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void originalTailIsRetainedAcrossFailedUnitCloseAndPostOpenGuardLoss(boolean loseGuard) {
        var scope=new CacheBusinessScope("cluster",1,"cache");var chains=mock(SrsChainStore.class);var models=mock(SrsModelStore.class);
        when(chains.guardedBy(any())).thenReturn(chains);when(models.guardedBy(any())).thenReturn(models);
        var consumers=mock(SrsConsumerStore.class);when(consumers.guardedBy(any())).thenReturn(consumers);
        var stores=new SrsStores(chains,models,mock(SrsLogStore.class),consumers);
        var opened=new SrsChainState("chain","capture",null,1,Set.of("orders"),Set.of("orders"),0,null,null,0,0);
        when(chains.load("chain")).thenReturn(Optional.of(opened));
        var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("node","boot"),1,1,1);
        var active=new java.util.concurrent.atomic.AtomicBoolean(true);var allowClose=new java.util.concurrent.atomic.AtomicBoolean();var closes=new java.util.concurrent.atomic.AtomicInteger();
        var pending=new IllegalStateException("original source is still ending");
        var source=new io.tapstate.spi.capture.Subscription(){public void close(){closes.incrementAndGet();if(!allowClose.get())throw pending;}};
        var port=mock(io.tapstate.spi.capture.CapturePort.class);
        when(port.cdc(any(),any(),any())).thenAnswer(call -> {if(loseGuard)active.set(false);return source;});
        var runtime=new NativeSrsCaptureRunUnit(scope,stores,mock(HazelcastInstance.class));
        Runnable guard=() -> {if(!active.get())throw new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);};
        try {
            if(loseGuard)assertThatThrownBy(() -> runtime.startTail(scope,opened,claim,new io.tapstate.spi.capture.CaptureConfig("fixture",Map.of(),List.of("orders")),io.tapstate.spi.capture.CaptureStart.present(),writer -> port,guard)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            else runtime.startTail(scope,opened,claim,new io.tapstate.spi.capture.CaptureConfig("fixture",Map.of(),List.of("orders")),io.tapstate.spi.capture.CaptureStart.present(),writer -> port,guard);
            assertThatThrownBy(runtime::close).isSameAs(pending);int before=closes.get();allowClose.set(true);runtime.close();assertThat(closes.get()).isGreaterThan(before);
        } finally {allowClose.set(true);runtime.close();}
    }

    @Test void aConsumerDetachedAfterTheTopologyReadDoesNotFailOtherReaders() {
        var scope=new CacheBusinessScope("cluster",1,"cache");var chains=mock(SrsChainStore.class);
        var models=mock(SrsModelStore.class);var log=mock(SrsLogStore.class);var consumers=mock(SrsConsumerStore.class);
        when(chains.guardedBy(any())).thenReturn(chains);when(models.guardedBy(any())).thenReturn(models);when(consumers.guardedBy(any())).thenReturn(consumers);
        var owner=new WorkloadOwner("node","boot");
        var capture=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),owner,1,1,1);
        var members=new ArrayList<SrsConsumerMembership>();
        for(String id:List.of("a","b")) {
            var context=new SrsConsumerContext(new SrsConsumerKey("chain",id,"source"),1);
            var claim=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,id),owner,1,1,1);
            members.add(new SrsConsumerMembership(context,1,false,Set.of("orders"),claim));
        }
        when(chains.load("chain")).thenReturn(Optional.of(new SrsChainState("chain","capture",null,1,Set.of("orders"),Set.of("orders"),0,null,null,2,2)));
        when(consumers.attached("chain")).thenReturn(new SrsConsumerTopology(scope,"chain",2,members));
        // The adapter returns empty only after a coherent detached relation; a corrupt attached cursor throws.
        when(consumers.progress(members.getFirst().context())).thenReturn(Optional.empty());
        try(var runtime=new NativeSrsCaptureRunUnit(scope,new SrsStores(chains,models,log,consumers),mock(HazelcastInstance.class))) {
            assertThatCode(() -> runtime.retain(scope,"chain",capture,() -> {})).doesNotThrowAnyException();
        }
        verify(log,never()).trim(anyString(),anyLong(),any());verify(models,never()).reclaim(anyString(),any());
    }
}
