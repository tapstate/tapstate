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
    @Test void pipelineProducerSurvivesCloseUntilAnAlreadyDeclaredReportActuallySettles() throws Exception {
        var fixture=new PipelineAlertRuntimeTest();
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
        try(var runtime=fixture.runtime();var caller=java.util.concurrent.Executors.newSingleThreadExecutor()) {
            runtime.tick();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
            var pass=fixture.pass("orders");var dependency=runtime.pipelineGuard(new ExecutionFence(pass.scope(),pass.claim()),()->{});
            var delayed=mock(PipelineAlertRuntime.class);
            doAnswer(call->{
                entered.countDown();assertThat(release.await(3,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                runtime.resourceInput(call.getArgument(0),call.getArgument(1),call.getArgument(2),call.getArgument(3),
                        call.getArgument(4),call.getArgument(5),call.getArgument(6),call.getArgument(7));return null;
            }).when(delayed).resourceInput(any(),anyString(),anyLong(),anyString(),any(),any(),any(),any());
            var storage=mock(StorageBinding.class);when(storage.defaultSourceId()).thenReturn("other");
            var signals=new BuiltinResourceSignals(()->delayed,()->storage,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},fixture.fixture.clock);
            var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of(),null,null,null,null),"a".repeat(64),
                    new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2));
            var root=signals.source(source,"target",dependency,()->{});var phase=root.fork("open");
            var reporting=caller.submit(()->phase.completed(SourceOperationObserver.Capability.TARGET_WRITE,new java.net.ConnectException("controlled failure")));
            assertThat(entered.await(2,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            root.close();root.close();phase.close();assertThat(root.hasPendingReports()).isTrue();assertThat(dependency.retained()).isTrue();
            var observation=PipelineAlertRuntimeTest.observation("orders",PipelineAlertRuntimeTest.T,io.tapstate.core.lifecycle.PipelineState.STOPPED);
            var receipt=runtime.terminalPublished(pass,TerminalPublication.from(pass,7,observation),observation,null,true);
            assertThat(receipt.seal()).isFalse();assertThat(receipt.status()).isEqualTo(TerminalPublication.Status.PENDING);
            release.countDown();reporting.get(2,java.util.concurrent.TimeUnit.SECONDS);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->receipt.status()==TerminalPublication.Status.COMPLETE);
            assertThat(root.hasPendingReports()).isFalse();assertThat(dependency.retained()).isFalse();assertThat(receipt.seal()).isTrue();
            dependency.close();
        }finally{release.countDown();}
    }

    @Test void classifiedSourceFailuresRetainBoundedRedactedDiagnosticText() {
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("other");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.fixed(Instant.EPOCH,ZoneOffset.UTC));
        var identity=new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2);
        var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of("password","original-source-secret"),null,null,null,null),"a".repeat(64),identity);
        signals.source(source,()->{}).completed(SourceOperationObserver.Capability.STREAM_READ,
                new java.net.ConnectException("Connection reset password=original-source-secret"));
        var input=org.mockito.ArgumentCaptor.forClass(AlertInput.class);
        verify(runtime).resourceInput(any(),eq("source.connection-failed"),eq(2L),anyString(),any(),any(),input.capture(),any());
        var operation=(AlertInput.Operational)input.getValue();
        assertThat(operation.trace()).isNotNull();
        assertThat(operation.trace().text()).contains("Connection reset").doesNotContain("original-source-secret");
    }
    @Test void nestedOriginalSourceSecretsAreFrozenBeforeConnectorMutationAndForks() {
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("other");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.fixed(Instant.EPOCH,ZoneOffset.UTC));
        var nested=new HashMap<String,Object>();nested.put("password","old-private-value");
        var identity=new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2);
        var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of("options",nested),null,null,null,null),"a".repeat(64),identity);
        var root=signals.source(source,()->{});nested.put("password","new-value");var phase=root.fork("stream");
        phase.completed(SourceOperationObserver.Capability.STREAM_READ,new java.net.ConnectException("failed for old-private-value"));
        var input=org.mockito.ArgumentCaptor.forClass(AlertInput.class);
        verify(runtime).resourceInput(any(),eq("source.connection-failed"),eq(2L),anyString(),any(),any(),input.capture(),any());
        assertThat(((AlertInput.Operational)input.getValue()).trace().text()).doesNotContain("old-private-value");
    }
    @Test void driverFailuresKeepTheirMeaningAcrossAnIsolatedConnectorLoader() throws Exception {
        var urls=new java.net.URL[]{
                com.mongodb.MongoException.class.getProtectionDomain().getCodeSource().getLocation(),
                org.bson.BsonDocument.class.getProtectionDomain().getCodeSource().getLocation()};
        try(var loader=new java.net.URLClassLoader(urls,ClassLoader.getPlatformClassLoader())) {
            var foreign=Class.forName("com.mongodb.MongoException",true,loader);
            for(int code:new int[]{13,18,8000,999}) {
                var failure=(Throwable)foreign.getConstructor(int.class,String.class).newInstance(code,"controlled");
                assertThat(failure).isNotInstanceOf(com.mongodb.MongoException.class);
                assertThat(BuiltinResourceSignals.connectionFailure(failure)).isEqualTo(code!=999);
            }
            var timeout=(Throwable)Class.forName("com.mongodb.MongoTimeoutException",true,loader)
                    .getConstructor(String.class).newInstance("controlled");
            assertThat(BuiltinResourceSignals.connectionFailure(timeout)).isTrue();
        }
    }

    @Test void aClosedObserverFamilyTracksEveryCapturedPhaseReportUntilSettlement() {
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("other");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.systemUTC());
        var id=new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",1);
        var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of(),null,null,null,null),"a".repeat(64),id);
        var root=signals.source(source,"capture",()->{},()->{});var phase=root.fork("stream");
        phase.completed("orders",SourceOperationObserver.Capability.STREAM_READ,new java.net.ConnectException("failure"));
        phase.completed("customers",SourceOperationObserver.Capability.STREAM_READ,new java.net.ConnectException("failure"));
        var settled=org.mockito.ArgumentCaptor.forClass(Runnable.class);
        verify(runtime,times(2)).resourceInput(any(),anyString(),anyLong(),anyString(),any(),any(),any(),settled.capture());
        assertThat(root.hasPendingReports()).isTrue();root.close();phase.close();
        clearInvocations(runtime);phase.completed("orders",SourceOperationObserver.Capability.STREAM_READ,null);
        root.fork("late").completed(SourceOperationObserver.Capability.STREAM_READ,null);verifyNoInteractions(runtime);
        settled.getAllValues().getFirst().run();settled.getAllValues().getFirst().run();assertThat(root.hasPendingReports()).isTrue();
        settled.getAllValues().getLast().run();assertThat(root.hasPendingReports()).isFalse();
    }
    @Test void immediateRefusalAndSubmissionFailureCannotStrandMasterReportCounts() {
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("views");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.systemUTC());
        var id=new SourceBindingIdentity("views","e6e8d616-985e-49cc-a0f4-9802897abf95",1);
        var source=new SourceBindingSnapshot(new SourceResource("views",null,"test",Map.of(),null,null,null,null),"a".repeat(64),id);
        doAnswer(call->{((Runnable)call.getArgument(7)).run();return null;}).when(runtime).resourceInput(any(),anyString(),anyLong(),anyString(),any(),any(),any(),any());
        var observer=signals.source(source,"target",()->{},()->{});
        observer.completed(SourceOperationObserver.Capability.TARGET_WRITE,new java.net.ConnectException("failure"));
        verify(runtime,times(2)).resourceInput(any(),anyString(),anyLong(),anyString(),any(),any(),any(),any());
        assertThat(observer.hasPendingReports()).isFalse();
        var failure=new IllegalStateException("submission failed");
        doThrow(failure).when(runtime).resourceInput(any(),anyString(),anyLong(),anyString(),any(),any(),any(),any());
        assertThatThrownBy(()->observer.completed(SourceOperationObserver.Capability.TARGET_WRITE,new java.net.ConnectException("failure"))).isSameAs(failure);
        assertThat(observer.hasPendingReports()).isFalse();
        var queued=new java.util.concurrent.atomic.AtomicReference<Runnable>();var calls=new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(call->{
            if(calls.incrementAndGet()==1){queued.set(call.getArgument(7));return null;}
            throw failure;
        }).when(runtime).resourceInput(any(),anyString(),anyLong(),anyString(),any(),any(),any(),any());
        assertThatThrownBy(()->observer.completed(SourceOperationObserver.Capability.TARGET_WRITE,new java.net.ConnectException("failure"))).isSameAs(failure);
        observer.close();assertThat(observer.hasPendingReports()).isTrue();
        queued.get().run();queued.get().run();assertThat(observer.hasPendingReports()).isFalse();
    }
    @Test void retryablePipelineRenewalConflictIsNotACacheAvailabilitySample() {
        var runtime = mock(PipelineAlertRuntime.class);
        var signals = new BuiltinResourceSignals(() -> runtime, () -> null,
                new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1), () -> {},
                Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        var delegate = mock(WorkloadClaimStore.class);
        var claim = mock(WorkloadClaim.class);
        var ttl = Duration.ofSeconds(30);
        var conflict = new WorkloadClaimRenewalException(WorkloadClaimRenewalException.Reason.WRITE_CONFLICT,
                "Mongo Pipeline renewal WriteConflict code=112");
        when(delegate.renew(claim, ttl)).thenThrow(conflict).thenReturn(Optional.of(claim));
        var observed = ObservedWorkloadClaims.observe(delegate, signals::cacheOperation);

        assertThatThrownBy(() -> observed.renew(claim, ttl)).isSameAs(conflict);
        assertThat(observed.renew(claim, ttl)).contains(claim);

        var input = org.mockito.ArgumentCaptor.forClass(AlertInput.Operational.class);
        verify(runtime).cacheInput(any(), anyString(), any(), input.capture());
        assertThat(input.getValue().succeeded()).isTrue();
        assertThat(input.getValue().capability()).isEqualTo(OperationalEvidence.Capability.CACHE_WRITE);
    }

    @Test void cacheSuccessSamplingKeepsBoundariesClockRollbackAndEveryFailureEdge(){
        var runtime=mock(PipelineAlertRuntime.class);var now=new java.util.concurrent.atomic.AtomicReference<>(Instant.EPOCH);
        var clock=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return now.get();}};
        var signals=new BuiltinResourceSignals(()->runtime,()->null,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},clock);
        signals.cacheOperation(false,null);
        for(int i=0;i<200;i++)signals.cacheOperation(false,null);
        signals.cacheOperation(true,null);
        now.set(Instant.EPOCH.plusMillis(999));signals.cacheOperation(false,null);
        now.set(Instant.EPOCH.plusSeconds(1));signals.cacheOperation(false,null);
        var unavailable=new TapstateException(IoError.STORE_UNAVAILABLE,Map.of(),null);
        now.set(Instant.EPOCH.plusMillis(1100));signals.cacheOperation(false,unavailable);signals.cacheOperation(false,unavailable);
        now.set(Instant.EPOCH.plusMillis(1200));signals.cacheOperation(false,null);
        now.set(Instant.EPOCH.plusMillis(500));signals.cacheOperation(false,null);
        now.set(Instant.EPOCH.plusMillis(600));signals.cacheOperation(false,null);
        now.set(Instant.EPOCH.plusMillis(1500));signals.cacheOperation(false,null);
        now.set(Instant.EPOCH.plusMillis(1600));signals.cacheOperation(false,unavailable);
        now.set(Instant.EPOCH.plusMillis(1700));signals.cacheOperation(false,null);
        var input=org.mockito.ArgumentCaptor.forClass(AlertInput.Operational.class);
        verify(runtime,times(10)).cacheInput(eq(new AlarmKey.Resource("cluster-storage", "cache", "cache:1")),
                anyString(), any(), input.capture());
        var values=input.getAllValues();
        assertThat(values.stream().filter(v->!v.succeeded()).count()).isEqualTo(3);
        assertThat(values.get(1).capability()).isEqualTo(OperationalEvidence.Capability.CACHE_WRITE);
        assertThat(values.get(2).observedAt()).isEqualTo(Instant.EPOCH.plusSeconds(1));
        assertThat(values.get(6).observedAt()).isEqualTo(Instant.EPOCH.plusMillis(500));
        assertThat(values.getLast().succeeded()).isTrue();
    }
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
        verify(runtime).resourceInput(eq(new AlarmKey.Resource("source","erp",id.incarnation())),eq("source.connection-failed"),eq(2L),anyString(),any(),any(),argThat(v->v instanceof AlertInput.Operational o&&!o.succeeded()&&o.capability()==OperationalEvidence.Capability.STREAM_READ),any());
        clearInvocations(runtime);
        callback.completed(SourceOperationObserver.Capability.STREAM_READ,new java.sql.SQLException("data","22003"));
        verifyNoInteractions(runtime);
        Runnable ended=()->{throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);};
        signals.source(source,ended).completed(SourceOperationObserver.Capability.STREAM_READ,null);
        var captured=org.mockito.ArgumentCaptor.forClass(Runnable.class);
        verify(runtime).resourceInput(any(),eq("source.connection-failed"),eq(2L),anyString(),captured.capture(),any(),any(),any());
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
    @Test void actualAccessDomainsKeepTablesAndConnectionsApartAndFenceOldCallbacks() {
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("other");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.fixed(Instant.EPOCH,ZoneOffset.UTC));
        var id=new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2);
        var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of(),null,null,null,null),"a".repeat(64),id);
        var first=signals.source(source,"capture-a",()->{},()->{});
        first.completed("orders",SourceOperationObserver.Capability.STREAM_READ,new java.net.ConnectException("failure"));
        first.completed("customers",SourceOperationObserver.Capability.STREAM_READ,null);
        var input=org.mockito.ArgumentCaptor.forClass(AlertInput.class);var local=org.mockito.ArgumentCaptor.forClass(Runnable.class);
        verify(runtime,times(2)).resourceInput(any(),anyString(),eq(2L),anyString(),any(),local.capture(),input.capture(),any());
        var values=input.getAllValues().stream().map(v->(AlertInput.Operational)v).toList();
        assertThat(values.get(0).domainToken()).isNotEqualTo(values.get(1).domainToken());
        var replacement=signals.source(source,"capture-a",()->{},()->{});
        assertThatThrownBy(()->local.getAllValues().getFirst().run()).isInstanceOf(TapstateException.class);
        clearInvocations(runtime);first.completed("orders",SourceOperationObserver.Capability.STREAM_READ,null);verifyNoInteractions(runtime);
        replacement.completed("orders",SourceOperationObserver.Capability.STREAM_READ,null);
        var current=org.mockito.ArgumentCaptor.forClass(AlertInput.class);
        verify(runtime).resourceInput(any(),anyString(),eq(2L),anyString(),any(),any(),current.capture(),any());
        assertThat(((AlertInput.Operational)current.getValue()).domainToken()).isEqualTo(values.getFirst().domainToken());
        replacement.close();clearInvocations(runtime);replacement.completed("orders",SourceOperationObserver.Capability.STREAM_READ,null);verifyNoInteractions(runtime);
    }

    @Test void closingAConnectionCannotEraseEvidenceAlreadyCapturedUnderItsOriginalProof() {
        var runtime=mock(PipelineAlertRuntime.class);var binding=mock(StorageBinding.class);when(binding.defaultSourceId()).thenReturn("other");
        var signals=new BuiltinResourceSignals(()->runtime,()->binding,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),()->{},Clock.fixed(Instant.EPOCH,ZoneOffset.UTC));
        var id=new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2);
        var source=new SourceBindingSnapshot(new SourceResource("erp",null,"test",Map.of(),null,null,null,null),"a".repeat(64),id);
        var callback=signals.source(source,"assessment",()->{},()->{});
        callback.completed("orders",SourceOperationObserver.Capability.TARGET_READ,new java.net.ConnectException("failure"));
        var local=org.mockito.ArgumentCaptor.forClass(Runnable.class);var settled=org.mockito.ArgumentCaptor.forClass(Runnable.class);
        verify(runtime).resourceInput(any(),anyString(),eq(2L),anyString(),any(),local.capture(),any(),settled.capture());
        callback.close();assertThatCode(()->local.getValue().run()).doesNotThrowAnyException();
        clearInvocations(runtime);callback.completed("orders",SourceOperationObserver.Capability.TARGET_READ,null);verifyNoInteractions(runtime);
        settled.getValue().run();assertThatThrownBy(()->local.getValue().run()).isInstanceOf(TapstateException.class);
    }

}
