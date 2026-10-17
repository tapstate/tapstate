package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The production lease factory must not resolve a configuration-side key session at any lease stage. */
class NodeSessionKeyringIndependenceTest {
    @Test
    void aConfigurationKeySessionThatCannotBeLoadedDoesNotPreventCacheRenewalAndExactRelease() throws Exception {
        var claim = new WorkloadClaim(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"),
                new WorkloadOwner("node-a", "boot-a"), 1, 0, 0, Instant.now().plusSeconds(30));
        var member = mock(HazelcastInstance.class); var context = new ConcurrentHashMap<String, Object>();
        context.put(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY, claim);
        context.put(HazelcastConfiguration.NODE_SESSION_ASKED_AT_CONTEXT_KEY, System.nanoTime());
        when(member.getUserContext()).thenReturn(context);
        var store = mock(WorkloadClaimStore.class); var renewals = new CountDownLatch(2);
        when(store.renew(any(WorkloadClaim.class), any(Duration.class))).thenAnswer(call -> { renewals.countDown(); return Optional.of(claim); });
        when(store.release(claim)).thenReturn(true);
        var properties = new ClusterProperties(); properties.setNodeSessionTtl(Duration.ofSeconds(5));
        properties.setNodeSessionRenewInterval(Duration.ofMillis(10));
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("configuration", new HazelcastConfiguration()); beans.registerSingleton("member", member);
        beans.registerSingleton("properties", properties); beans.registerSingleton("claims", store);
        var attempted = new AtomicInteger(); var poison = new RootBeanDefinition(SourceConfigKeyringSession.class);
        poison.setLazyInit(true); poison.setInstanceSupplier(() -> {
            attempted.incrementAndGet(); throw new IllegalStateException("configuration key session is unavailable");
        });
        beans.registerBeanDefinition("unavailableKeySession", poison);
        var lease = new RootBeanDefinition(NodeSessionLease.class); lease.setFactoryBeanName("configuration");
        lease.setFactoryMethodName("nodeSessionLease"); lease.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
        lease.setDestroyMethodName("close"); beans.registerBeanDefinition("lease", lease);
        try {
            assertThatCode(() -> assertThat(beans.getBean("lease", NodeSessionLease.class)).isNotNull()).doesNotThrowAnyException();
            assertThat(renewals.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(attempted).hasValue(0); verify(member, never()).shutdown();
        } finally { beans.destroySingletons(); }
        verify(store).release(claim); assertThat(attempted).hasValue(0);
    }
}
