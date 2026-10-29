package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.capture.SourceOperationObserver;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class BuiltinResourceSignalsTest {
    @Test void onlyClassifiedConnectionFailuresAreOperationalEvidence(){
        assertThat(BuiltinResourceSignals.connectionFailure(new java.sql.SQLException("private","08001"))).isTrue();
        assertThat(BuiltinResourceSignals.connectionFailure(new java.sql.SQLException("private","28000"))).isTrue();
        assertThat(BuiltinResourceSignals.connectionFailure(new java.sql.SQLException("private","42501"))).isTrue();
        assertThat(BuiltinResourceSignals.connectionFailure(new java.sql.SQLException("private","42000"))).isFalse();
        assertThat(BuiltinResourceSignals.connectionFailure(new java.sql.SQLException("private","22003"))).isFalse();
        assertThat(BuiltinResourceSignals.connectionFailure(new IllegalStateException("private"))).isFalse();
        assertThat(BuiltinResourceSignals.connectionFailure(new com.mongodb.MongoCommandException(org.bson.BsonDocument.parse("{\"ok\":0,\"code\":13,\"errmsg\":\"private\"}"),new com.mongodb.ServerAddress()))).isTrue();
    }
    @Test void sourceResultUsesTheCapturedLifetimeWithoutExecutingTheGuardOnTheCallback(){
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("other");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.fixed(Instant.EPOCH,ZoneOffset.UTC));
        var id=new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2);
        var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of(),null,null,null,null),"a".repeat(64),id);
        var callback=signals.source(source,()->{});
        callback.completed(SourceOperationObserver.Capability.STREAM_READ,new java.sql.SQLException("secret","08006"));
        verify(runtime).resourceInput(eq(new AlarmKey.Resource("source","erp",id.incarnation())),eq("source.connection-failed"),eq(2L),anyString(),any(),argThat(v->v instanceof AlertInput.Operational o&&!o.succeeded()&&o.capability()==OperationalEvidence.Capability.STREAM_READ));
        clearInvocations(runtime);
        callback.completed(SourceOperationObserver.Capability.STREAM_READ,new java.sql.SQLException("data","22003"));
        verifyNoInteractions(runtime);
        Runnable ended=()->{throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);};
        signals.source(source,ended).completed(SourceOperationObserver.Capability.STREAM_READ,null);
        var captured=org.mockito.ArgumentCaptor.forClass(Runnable.class);
        verify(runtime).resourceInput(any(),eq("source.connection-failed"),eq(2L),anyString(),captured.capture(),any());
        assertThatThrownBy(()->captured.getValue().run()).isInstanceOf(TapstateException.class);
    }
    @Test void conditionalWriteConflictsAreNotStorageFailuresAndTheWrapperAddsNoProbe(){
        var claims=mock(WorkloadClaimStore.class);var outcomes=new ArrayList<Throwable>();var writes=new ArrayList<Boolean>();
        var observed=new ObservedWorkloadClaims(claims,(write,failure)->{writes.add(write);outcomes.add(failure);});
        var key=new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"orders");
        when(claims.read(key)).thenReturn(Optional.empty());assertThat(observed.read(key)).isEmpty();
        when(claims.release(null)).thenReturn(false);assertThat(observed.release(null)).isFalse();
        assertThat(writes).containsExactly(false,true);assertThat(outcomes).containsOnlyNulls();
        verify(claims).read(key);verify(claims).release(null);verifyNoMoreInteractions(claims);
    }
}
