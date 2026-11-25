package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.ValidationTaskStore;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeValidationDiscoveryTest {
    private static NativeValidationScheduler scheduler(MongoConfigurationDomain configuration){
        return new NativeValidationScheduler(configuration,mock(ValidationMemberResources.Member.class),mock(ValidationMemberResources.class),
                ()->{throw new AssertionError("no scheduled task was returned");},new MemoryValidationSchedules(),Clock.systemUTC(),new ValidationScheduleCalculator(),()->{});
    }
    private static void discover(NativeValidationScheduler scheduler){ReflectionTestUtils.invokeMethod(scheduler,"discover");}
    @Test void stableRevisionStopsFullScansAndAMidScanChangeRequiresAnotherCompletePass(){
        var configuration=mock(MongoConfigurationDomain.class);var tasks=mock(ValidationTaskStore.class);var revision=new AtomicLong(1);
        when(configuration.configurationRevision()).thenAnswer(c->revision.get());when(configuration.pollingValidationTasks(any())).thenReturn(tasks);
        when(tasks.pipelines(null,1)).thenReturn(List.of("flow"));when(tasks.pipelines("flow",1)).thenReturn(List.of());
        when(tasks.taskIds("flow",null,1)).thenReturn(List.of());
        try(var scheduler=scheduler(configuration)){
            discover(scheduler);revision.set(2);discover(scheduler);
            assertThat(ReflectionTestUtils.getField(scheduler,"completedRevision")).isNull();
            discover(scheduler);discover(scheduler);
            assertThat(ReflectionTestUtils.getField(scheduler,"completedRevision")).isEqualTo(2L);
            clearInvocations(configuration);discover(scheduler);discover(scheduler);
            verify(configuration,times(2)).configurationRevision();verify(configuration,never()).pollingValidationTasks(any());
            revision.set(3);discover(scheduler);verify(configuration).pollingValidationTasks(any());
        }
    }
    @Test void aDamagedFirstPipelineDoesNotStarveLaterPipelinesOrCertifyACompleteScan(){
        var configuration=mock(MongoConfigurationDomain.class);var tasks=mock(ValidationTaskStore.class);var damaged=new AtomicBoolean(true);
        when(configuration.configurationRevision()).thenReturn(1L);when(configuration.pollingValidationTasks(any())).thenReturn(tasks);
        when(tasks.pipelines(null,1)).thenReturn(List.of("broken"));when(tasks.pipelines("broken",1)).thenReturn(List.of("healthy"));
        when(tasks.pipelines("healthy",1)).thenReturn(List.of());
        when(tasks.taskIds("broken",null,1)).thenAnswer(c->{if(damaged.get())throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","fixture"),null);return List.of();});
        when(tasks.taskIds("healthy",null,1)).thenReturn(List.of());
        try(var scheduler=scheduler(configuration)){
            discover(scheduler);discover(scheduler);discover(scheduler);
            verify(tasks).taskIds("healthy",null,1);assertThat(ReflectionTestUtils.getField(scheduler,"completedRevision")).isNull();
            damaged.set(false);discover(scheduler);discover(scheduler);discover(scheduler);
            assertThat(ReflectionTestUtils.getField(scheduler,"completedRevision")).isEqualTo(1L);
        }
    }
}
