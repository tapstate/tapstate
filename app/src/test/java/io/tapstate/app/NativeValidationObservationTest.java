package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NativeValidationObservationTest {
    @Test
    void nativeBusinessContextProvidesItsOwnMemberBoundCaptureObserver() {
        var member = mock(HazelcastInstance.class);
        var cluster = mock(Cluster.class);
        var local = mock(Member.class);
        when(member.getCluster()).thenReturn(cluster);
        when(cluster.getLocalMember()).thenReturn(local);
        when(local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("node");
        when(local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("boot");
        var properties = new ClusterProperties(); properties.setId("cluster");
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getBeanFactory().registerSingleton("nativeBusinessContext", mock(NativeBusinessContext.class));
            context.getBeanFactory().registerSingleton("hazelcastMember", member);
            context.getBeanFactory().registerSingleton("clusterProperties", properties);
            context.addBeanFactoryPostProcessor(factory -> {
                for (String name : factory.getBeanDefinitionNames()) factory.getBeanDefinition(name).setLazyInit(true);
            });
            context.register(NativeBusinessContext.Wiring.class);
            context.refresh();
            assertThat(context.getBeansOfType(ValidationCaptureObservations.class)).hasSize(1);
            assertThat(context.getBean(ValidationCaptureObservations.class).available()).isTrue();
        }
    }
}
