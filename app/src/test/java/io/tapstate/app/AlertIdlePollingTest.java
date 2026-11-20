package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class AlertIdlePollingTest {
    private static final Runnable ACTIVE=()->{};
    private static final AlarmKey.Scope SCOPE=new AlarmKey.Scope("installation","cluster");
    private static AlertStore.Planning plan(long revision,boolean isolated,List<AlertStore.Stored<AlertSchedule.Record>> schedules){
        var host=new AlertStore.Host(SCOPE,new StorageSetupScope("cluster","installation","setup",1),AlertChannelPolicy.Deployment.ON_PREM,ACTIVE);
        return new AlertStore.Planning(new AlertStore.PlanVersion(revision,99),host,List.of(),List.of(),schedules,Map.of(),Map.of(),Map.of(),Set.of(),isolated);
    }
    @Test void onlyTheLiveAuthoringRevisionCanSustainANoDestinationProof(){
        var store=mock(AlertStore.class);when(store.planning()).thenReturn(plan(7,false,List.of()));
        when(store.planningIfConfigurationChanged(7)).thenReturn(Optional.empty(),Optional.of(plan(8,false,List.of())));
        when(store.planningIfConfigurationChanged(8)).thenReturn(Optional.empty());
        var planner=new AlertNotificationPlanner(store,Clock.systemUTC(),"worker",null,ACTIVE);
        planner.tick();planner.tick();planner.tick();planner.tick();
        verify(store,times(1)).planning();verify(store,times(2)).planningIfConfigurationChanged(7);verify(store).planningIfConfigurationChanged(8);
    }
    @Test void isolatedPlanningCannotBecomeAnIdleProof(){
        var store=mock(AlertStore.class);when(store.planning()).thenReturn(plan(1,true,List.of()));
        var planner=new AlertNotificationPlanner(store,Clock.systemUTC(),"worker",null,ACTIVE);
        assertThatThrownBy(planner::tick).isInstanceOf(TapstateException.class);assertThatThrownBy(planner::tick).isInstanceOf(TapstateException.class);
        verify(store,times(2)).planning();verify(store,never()).planningIfConfigurationChanged(anyLong());
    }
    @Test void retainedSchedulesAndFailedRevisionChecksRequireAFullReadAgain(){
        var store=mock(AlertStore.class);var schedule=mock(AlertSchedule.Record.class);when(schedule.retiredAt()).thenReturn(Instant.EPOCH);
        when(store.planning()).thenReturn(plan(1,false,List.of(new AlertStore.Stored<>(1,schedule))),plan(2,false,List.of()),plan(3,false,List.of()));
        var planner=new AlertNotificationPlanner(store,Clock.systemUTC(),"worker",null,ACTIVE);planner.tick();planner.tick();
        when(store.planningIfConfigurationChanged(2)).thenThrow(new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","test"),null));
        assertThatThrownBy(planner::tick).isInstanceOf(TapstateException.class);planner.tick();verify(store,times(3)).planning();
    }
    @Test void anEmptyDeliveryProofResetsPaginationAndClearsAnEarlierFailure(){
        var store=mock(AlertStore.class);var channels=mock(AlertDeliveryRuntime.Channels.class);
        var cursor=new AlertStore.DueCursor(Instant.EPOCH,"business","later");
        when(store.deliveryIntentsEmpty()).thenReturn(false,true,false);
        when(store.dueIntents(any(),isNull(),eq(32))).thenReturn(new AlertStore.DuePage(List.of(),cursor,true));
        try(var runtime=new AlertDeliveryRuntime(store,Clock.systemUTC(),"worker",ACTIVE,channels,a->null)){
            runtime.tick();assertThat(runtime.failure()).isNotNull();runtime.tick();assertThat(runtime.failure()).isNull();runtime.tick();
            verify(store,times(2)).dueIntents(any(),isNull(),eq(32));verify(channels,never()).open(any(),any(),any());
        }
    }
    @Test void aFailedEmptyProbeCannotPresentDeliveryAsHealthy(){
        var store=mock(AlertStore.class);when(store.deliveryIntentsEmpty()).thenThrow(new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","test"),null));
        try(var runtime=new AlertDeliveryRuntime(store,Clock.systemUTC(),"worker",ACTIVE,mock(AlertDeliveryRuntime.Channels.class),a->null)){
            runtime.tick();assertThat(runtime.failure()).isNotNull();verify(store,never()).dueIntents(any(),any(),anyInt());
        }
    }
}
