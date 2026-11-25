package io.tapstate.app;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class PipelineRenewalReservationRoutingTest {
    private final WorkloadClaimKey key = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "ordinary");
    private final Set<WorkloadClaimKey> recoveries = Set.of(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "recovery"));
    @Test void unknownStoreRetainsTheOriginalConservativeNoWorkBehavior() {
        var store=mock(DeadlineWorkloadClaimStore.class,CALLS_REAL_METHODS);var called=new AtomicBoolean();
        assertThatThrownBy(()->store.withRenewalDeadline(key,recoveries,System.nanoTime()+1000000000,()->{},()->{called.set(true);return true;}))
                .isInstanceOfSatisfying(WorkloadClaimRenewalException.class,e->assertThat(e.reason()).isEqualTo(WorkloadClaimRenewalException.Reason.NOT_STARTED));
        assertThat(called).isFalse();
    }
    @Test void productionObserverForwardsTheSameOriginalKeysDeadlineGuardAndCallback() {
        var delegate=mock(DeadlineWorkloadClaimStore.class);var observed=(DeadlineWorkloadClaimStore)ObservedWorkloadClaims.observe(delegate,(write,error)->{});
        long deadline=System.nanoTime()+5000000000L;Runnable guard=()->{};Supplier<Boolean> action=()->true;
        doAnswer(call->{assertThat((Object)call.getArgument(0)).isSameAs(key);assertThat((Object)call.getArgument(1)).isSameAs(recoveries);assertThat((Long)call.getArgument(2)).isEqualTo(deadline);assertThat((Object)call.getArgument(3)).isSameAs(guard);assertThat((Object)call.getArgument(4)).isSameAs(action);return action.get();})
                .when(delegate).withRenewalDeadline(eq(key),eq(recoveries),eq(deadline),same(guard),same(action));
        assertThat(observed.withRenewalDeadline(key,recoveries,deadline,guard,action)).isTrue();
    }
    @Test void nonDeadlineStoreCannotLoseItsConservativeFallback() {
        var store=mock(WorkloadClaimStore.class);var membership=mock(ClusterMembershipGate.class);
        when(membership.businessEligible()).thenReturn(true);when(membership.committed()).thenReturn(new ClusterMembership("cluster",7,Set.of("node")));
        var claim=new WorkloadClaim(key,new WorkloadOwner("node","boot"),1,1,7,Instant.now().plusSeconds(30));
        when(store.renew(claim,Duration.ofSeconds(30))).thenReturn(Optional.of(claim));
        var claims=new ClusterWorkloadClaims(store,membership);
        assertThatThrownBy(()->claims.renewScopedActuation(claim,Duration.ofSeconds(30),5000000000L,recoveries))
                .isInstanceOfSatisfying(WorkloadClaimRenewalException.class,e->assertThat(e.reason()).isEqualTo(WorkloadClaimRenewalException.Reason.NOT_STARTED));
        verify(store,never()).renew(any(),any());
        assertThat(claims.renewScopedActuation(claim,Duration.ofSeconds(30),5000000000L)).contains(claim);
    }
}
