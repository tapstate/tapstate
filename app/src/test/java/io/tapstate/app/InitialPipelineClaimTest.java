package io.tapstate.app;
import io.tapstate.spi.store.*;
import io.tapstate.core.lifecycle.*;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class InitialPipelineClaimTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","orders",1);
    @ParameterizedTest @ValueSource(strings={"match","new-stop","new-run","scope","revision","legacy","no-start","scan-stop","identifier"})
    void onlyTheLoadedOriginalRunMatchesTheOriginalStartHint(String scenario) {
        var scope=scenario.equals("scope")?new PipelineIntentScope("cluster",1,"cache","orders",2):SCOPE;
        var snapshot=new PipelineIntentSnapshot(SCOPE,scenario.startsWith("new-")?5:4,
                new DesiredState("orders",scenario.equals("new-stop")?PipelineState.STOPPED:PipelineState.RUNNING,"hash"));
        var candidate=scenario.equals("legacy")?new PipelineIntentStore.ScanCandidate("orders",4):
                new PipelineIntentStore.ScanCandidate(scenario.equals("identifier")?"other":"orders",scenario.equals("no-start")?0:4,
                        scope,scenario.equals("revision")?3:4,scenario.equals("scan-stop")?PipelineState.STOPPED:PipelineState.RUNNING);
        assertThat(ConvergenceDriver.initialClaimMatches(candidate,snapshot)).isEqualTo(scenario.equals("match"));
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void actualObservedFacadeRoutesOnlyThisNewPipelineAcquisitionWithinItsOriginalDeadline(boolean nodeBacked) {
        var store=mock(DeadlineWorkloadClaimStore.class);var membership=mock(ClusterMembershipGate.class);
        when(membership.businessEligible()).thenReturn(true);
        var key=new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"orders");var owner=new WorkloadOwner("node","boot");
        var claim=new WorkloadClaim(key,owner,1,0,1,Instant.now().plusSeconds(30));
        var node=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),owner,1,0,1);
        when(store.acquirePipeline(SCOPE,owner,1,Duration.ofSeconds(30))).thenReturn(WorkloadClaimAttempt.acquired(claim));
        when(store.acquireNodeBackedPipeline(SCOPE,node,1)).thenReturn(WorkloadClaimAttempt.acquired(claim));
        long before=System.nanoTime();
        doAnswer(call->{assertThat((Object)call.getArgument(0)).isEqualTo(key);
            assertThat(call.<Long>getArgument(1)-before).isBetween(Duration.ofSeconds(9).toNanos(),Duration.ofSeconds(11).toNanos());
            call.<Runnable>getArgument(2).run();return call.<Supplier<?>>getArgument(3).get();})
                .when(store).withInitialPipelineAcquisition(eq(key),anyLong(),any(),any());
        var facade=new ClusterWorkloadClaims(ObservedWorkloadClaims.observe(store,(write,error)->{}),membership).draining(new AdmissionDrain());
        var result=nodeBacked?facade.acquireNodeBackedPipeline(SCOPE,node,1,true):facade.acquirePipeline(SCOPE,owner,1,Duration.ofSeconds(30),true);
        assertThat(result).contains(WorkloadClaimAttempt.acquired(claim));
        verify(store).withInitialPipelineAcquisition(eq(key),anyLong(),any(),any());
        verify(store,never()).withClaimAcquisitionDeadline(any(),anyLong(),any(),any());
    }
    @Test void anUnsupportedInitialSchedulingHintFallsBackToTheOriginalBoundedAcquisition() {
        var store=mock(DeadlineWorkloadClaimStore.class,CALLS_REAL_METHODS);
        var key=new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"orders");
        long deadline=System.nanoTime()+5000000000L;Runnable active=()->{};Supplier<Boolean> callback=()->true;
        doAnswer(call->call.<Supplier<?>>getArgument(3).get()).when(store).withClaimAcquisitionDeadline(key,deadline,active,callback);
        assertThat(store.withInitialPipelineAcquisition(key,deadline,active,callback)).isTrue();
        verify(store).withClaimAcquisitionDeadline(key,deadline,active,callback);
    }
}
