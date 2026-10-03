package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.PipelineEventStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaimStore;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The runtime convergence loop is wired into startup and gated on the same store switch as the store it
 * reads and writes: enabled, the framework-free converger and its scheduled driver both come up; disabled
 * (a run with no store), neither does.
 */
class RuntimeConvergenceStartupTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            // This bare context does not install Boot's Duration converter or the runtime's adjacent beans.
            .withInitializer(context -> context.getBeanFactory()
                    .setConversionService(ApplicationConversionService.getSharedInstance()))
            .withBean(StorePort.class, RuntimeConvergenceStartupTest::store)
            .withBean(LifecycleActuator.class, NoOpActuator::new)
            // The publisher reads its snapshot source through this port. Production brings the coordinator up
            // under the same store switch as this configuration, so it is always there; stubbed here because
            // this context is the convergence loop alone, not the data plane behind it.
            .withBean(PipelineCaptureCoordinator.class, NoOpCaptureCoordinator::new)
            .withBean(Engine.class, () -> new Engine(mock(HazelcastInstance.class)))
            .withBean(Clock.class, Clock::systemUTC)
            .withBean(ObservationScopeRegistry.class, ObservationScopeRegistry::new)
            .withBean(LifecyclePendingRegistry.class, LifecyclePendingRegistry::new)
            .withBean(ClusterProperties.class, ClusterProperties::new)
            .withBean(ClusterIdentityStore.class, () -> {
                ClusterIdentityStore identities = mock(ClusterIdentityStore.class);
                when(identities.createIfAbsent(any())).thenAnswer(invocation -> invocation.getArgument(0));
                return identities;
            })
            // The two the driver asks the cluster for: whether this member may act on business work at
            // all, and which pipelines are this member's to drive. Production wires both from the member
            // configuration, which this context does not bring up -- it is the convergence loop alone.
            .withBean(ClusterMembershipGate.class, () -> new ClusterMembershipGate(new ClusterProperties()))
            .withBean(PipelineActuationOwnership.class, PipelineActuationOwnership::single)
            .withBean(RebuildAdmission.class, RebuildAdmission::never)
            .withUserConfiguration(RuntimeConvergenceConfiguration.class, AdjacentStoreConfiguration.class);

    @Configuration(proxyBeanMethods = false)
    static class AdjacentStoreConfiguration {
        @Bean WorkloadClaimStore workloadClaimStore(StorePort storePort) {
            return storePort.workloadClaims();
        }
    }

    private static StorePort store() {
        InMemoryStorePort store = spy(new InMemoryStorePort());
        doReturn(new InMemoryWorkloadClaimStore()).when(store).workloadClaims();
        doReturn(mock(PipelineEventStore.class)).when(store).events();
        return store;
    }

    @Test
    void enabledBringsUpTheConvergerAndItsDriver() {
        runner.withPropertyValues("tapstate.store.mongo.enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(PipelineConverger.class);
                    assertThat(context).hasSingleBean(ConvergenceDriver.class);
                });
    }

    @Test
    void disabledBringsUpNeither() {
        runner.withPropertyValues("tapstate.store.mongo.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(PipelineConverger.class);
                    assertThat(context).doesNotHaveBean(ConvergenceDriver.class);
                });
    }
}
