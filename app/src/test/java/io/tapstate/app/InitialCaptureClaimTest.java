package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class InitialCaptureClaimTest {
    @ParameterizedTest @CsvSource({"false,false","false,true","true,false","true,true"})
    void theObservedNativeFacadePromotesOnlyExplicitInitialCapture(boolean draining,boolean initial) {
        var store=mock(DeadlineWorkloadClaimStore.class);var gate=mock(ClusterMembershipGate.class);
        when(gate.businessEligible()).thenReturn(true);when(gate.committed()).thenReturn(new ClusterMembership("cluster",1,Set.of("node")));
        var key=new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture");var owner=new WorkloadOwner("node","boot");var ttl=Duration.ofSeconds(30);
        var claim=new WorkloadClaim(key,owner,1,0,1,Instant.now().plus(ttl));when(store.acquire(key,owner,1,ttl)).thenReturn(WorkloadClaimAttempt.acquired(claim));
        doAnswer(call->{call.<Runnable>getArgument(1).run();return call.<Supplier<?>>getArgument(2).get();}).when(store).withClaimAcquisition(any(),any(),any());
        doAnswer(call->{call.<Runnable>getArgument(2).run();return call.<Supplier<?>>getArgument(3).get();}).when(store).withClaimAcquisitionDeadline(any(),anyLong(),any(),any());
        doAnswer(call->{call.<Runnable>getArgument(1).run();return call.<Supplier<?>>getArgument(2).get();}).when(store).withInitialCaptureAcquisition(any(),any(),any());
        doAnswer(call->{call.<Runnable>getArgument(2).run();return call.<Supplier<?>>getArgument(3).get();}).when(store).withInitialCaptureAcquisition(any(),anyLong(),any(),any());
        var calls=new ClusterWorkloadClaims(ObservedWorkloadClaims.observe(store,(write,error)->{}),gate);if(draining)calls=calls.draining(new AdmissionDrain());
        var captures=new CaptureOwnership("cluster",owner,gate,calls,ttl);
        assertThat((initial?captures.acquireInitial(new CaptureId("capture")):captures.acquire(new CaptureId("capture"))).claim()).isEqualTo(claim);
        if(initial&&draining)verify(store).withInitialCaptureAcquisition(eq(key),anyLong(),any(),any());
        else if(initial)verify(store).withInitialCaptureAcquisition(eq(key),any(),any());
        else if(draining)verify(store).withClaimAcquisitionDeadline(eq(key),anyLong(),any(),any());
        else verify(store).withClaimAcquisition(eq(key),any(),any());
        if(!initial){verify(store,never()).withInitialCaptureAcquisition(any(),any(),any());verify(store,never()).withInitialCaptureAcquisition(any(),anyLong(),any(),any());}
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void observationForwardsTheOriginalKeyGuardCallbackAndDeadline(boolean boundedCall) {
        var store=mock(DeadlineWorkloadClaimStore.class);var wrapper=(DeadlineWorkloadClaimStore)ObservedWorkloadClaims.observe(store,(write,error)->{});
        var key=new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture");long deadline=System.nanoTime()+5000000000L;
        Runnable active=()->{};Supplier<Boolean> action=()->true;
        when(store.withInitialCaptureAcquisition(key,active,action)).thenReturn(true);when(store.withInitialCaptureAcquisition(key,deadline,active,action)).thenReturn(true);
        assertThat(boundedCall?wrapper.withInitialCaptureAcquisition(key,deadline,active,action):wrapper.withInitialCaptureAcquisition(key,active,action)).isTrue();
        if(boundedCall)verify(store).withInitialCaptureAcquisition(same(key),eq(deadline),same(active),same(action));
        else verify(store).withInitialCaptureAcquisition(same(key),same(active),same(action));
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void anUnsupportedHintUsesTheOriginalAcquisition(boolean boundedCall) {
        var store=mock(DeadlineWorkloadClaimStore.class,CALLS_REAL_METHODS);var key=new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture");
        long deadline=System.nanoTime()+5000000000L;Runnable active=()->{};Supplier<Boolean> action=()->true;
        when(store.withClaimAcquisition(key,active,action)).thenReturn(true);when(store.withClaimAcquisitionDeadline(key,deadline,active,action)).thenReturn(true);
        assertThat(boundedCall?store.withInitialCaptureAcquisition(key,deadline,active,action):store.withInitialCaptureAcquisition(key,active,action)).isTrue();
    }
}
