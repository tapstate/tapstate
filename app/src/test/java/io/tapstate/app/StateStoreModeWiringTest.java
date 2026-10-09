package io.tapstate.app;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.StateStoreSetupService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.ConnectionTester;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.StandardEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** The production state-store factories must consume the same validated four-value runtime mode. */
class StateStoreModeWiringTest {

    private static final String ABSENT = "absent";
    private static final String[] CLOUD_VALUES = {
            "tapstate.cloud.base-url=https://cloud.example.test",
            "tapstate.cloud.token=state-mode-token-sentinel",
            "tapstate.cloud.atlas-uri=mongodb://atlas.example.test/state_mode",
            "tapstate.cloud.cluster-id=state-mode-cluster"
    };

    @ParameterizedTest
    @ValueSource(strings = {ABSENT, "cloud", "on-prem", "onprem", "invalid-legacy-profile"})
    void completeCloudSettingsEnableSetupRegardlessOfAnIndependentLegacyProfile(String legacyProfile)
            throws Exception {
        Fixture fixture = new Fixture();
        runner(fixture, legacyProfile).withPropertyValues(CLOUD_VALUES).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isTrue();
            assertThatThrownBy(() -> context.getBean(StateStoreSetupService.class)
                    .connect("state-mode-user", Map.of()))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                        assertThat(error.args()).containsExactlyEntriesOf(
                                Map.of("reason", "MongoDB Atlas connector settings are required"));
                    });
            fixture.assertNoExternalWork();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {ABSENT, "cloud", "on-prem", "onprem", "invalid-legacy-profile"})
    void absentCloudSettingsKeepSetupOnPremRegardlessOfAnIndependentLegacyProfile(String legacyProfile)
            throws Exception {
        Fixture fixture = new Fixture();
        runner(fixture, legacyProfile).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isFalse();
            assertThatThrownBy(() -> context.getBean(StateStoreSetupService.class)
                    .connect("state-mode-user", Map.of()))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                        assertThat(error.args()).containsExactlyEntriesOf(
                                Map.of("reason", "Atlas state store setup is only available in Cloud"));
                    });
            fixture.assertNoExternalWork();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {ABSENT, "cloud", "on-prem", "onprem", "invalid-legacy-profile"})
    void completeCloudSettingsRequireAMarkedStoreForAView(String legacyProfile) throws Exception {
        Fixture fixture = new Fixture();
        runner(fixture, legacyProfile).withPropertyValues(CLOUD_VALUES).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isTrue();
            assertMissingStore(context.getBean(DagSource.class), "atlas-store");
            fixture.assertNoExternalWork();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {ABSENT, "cloud", "on-prem", "onprem", "invalid-legacy-profile"})
    void absentCloudSettingsKeepTheOnPremViewStoreFallback(String legacyProfile) throws Exception {
        Fixture fixture = new Fixture();
        runner(fixture, legacyProfile).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isFalse();
            assertMissingStore(context.getBean(DagSource.class), "views");
            fixture.assertNoExternalWork();
        });
    }

    private static void assertMissingStore(DagSource dagSource, String storeId) {
        assertThatThrownBy(() -> dagSource.dagFor("orders_view"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ActuationError.VIEW_STORE_NOT_CONFIGURED);
                    assertThat(error.args()).containsExactlyEntriesOf(Map.of("store", storeId));
                });
    }

    private static ApplicationContextRunner runner(Fixture fixture, String legacyProfile) {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withUserConfiguration(FourValueBinding.class)
                .withInitializer(context -> {
                    context.getEnvironment().getPropertySources()
                            .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
                    context.getEnvironment().getPropertySources()
                            .remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
                    BeanDefinitionRegistry registry = (BeanDefinitionRegistry) context.getBeanFactory();
                    context.getBeanFactory().registerSingleton("cloudFactory", new CloudRuntimeConfiguration());
                    context.getBeanFactory().registerSingleton("controlFactory", new ControlPlaneConfiguration());
                    context.getBeanFactory().registerSingleton("actuationFactory", new DataPlaneActuationConfiguration());
                    registerFactory(registry, "cloudRuntimeSettings", CloudRuntimeSettings.class,
                            "cloudFactory", "cloudRuntimeSettings");
                    registerFactory(registry, "stateStoreSetupService", StateStoreSetupService.class,
                            "controlFactory", "stateStoreSetupService");
                    registerFactory(registry, "dagSource", DagSource.class, "actuationFactory", "dagSource");
                })
                .withBean(ApplyService.class, () -> fixture.apply)
                .withBean(ArtifactQueryService.class, () -> fixture.artifacts)
                .withBean(SourceRepresentation.class, () -> fixture.representation)
                .withBean(ConnectionTestService.class, () -> fixture.connections)
                .withBean(StorePort.class, () -> fixture.store)
                .withBean(NestSettings.class, NestSettings::defaults)
                .withBean(ConnectionTester.class, () -> fixture.probe)
                .withBean(HazelcastInstance.class, () -> fixture.member);
        return ABSENT.equals(legacyProfile) ? runner
                : runner.withPropertyValues("tapstate.deployment.profile=" + legacyProfile);
    }

    private static void registerFactory(BeanDefinitionRegistry registry, String beanName, Class<?> beanType,
            String factoryName, String methodName) {
        RootBeanDefinition bean = new RootBeanDefinition(beanType);
        bean.setFactoryBeanName(factoryName);
        bean.setFactoryMethodName(methodName);
        bean.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
        registry.registerBeanDefinition(beanName, bean);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({CloudProperties.class, MongoProperties.class})
    static class FourValueBinding {
    }

    private static final class Fixture {
        private final ApplyService apply = mock(ApplyService.class);
        private final ArtifactQueryService artifacts = mock(ArtifactQueryService.class);
        private final SourceRepresentation representation = mock(SourceRepresentation.class);
        private final ConnectionTestService connections = mock(ConnectionTestService.class);
        private final ConnectionTester probe = mock(ConnectionTester.class);
        private final HazelcastInstance member = mock(HazelcastInstance.class);
        private final InMemoryStorePort store = new InMemoryStorePort();

        private Fixture() throws Exception {
            Cluster cluster = mock(Cluster.class);
            Member local = mock(Member.class);
            when(member.getCluster()).thenReturn(cluster);
            when(cluster.getLocalMember()).thenReturn(local);
            when(local.getAddress()).thenReturn(new Address("127.0.0.1", 5701));
            store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "example.test"),
                    SourceMode.CDC, List.of(TableRef.literal("orders")), null, null));
            store.schemas().save(new DiscoveredSourceModel("orders_src", "mysql", 0L,
                    new SourceModel(List.of(new SourceTable("orders",
                            List.of(new SourceField("id", "bigint", TapstateType.INT64)),
                            List.of("id"), List.of())))));
            store.artifacts().save(new PipelineResource("orders_view", null,
                    List.of(SourceRef.spec("orders_src", true)), null,
                    new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "id", null),
                    null, null, null));
            OpenRingGenerations.forSources(store, "orders_src");
        }

        private void assertNoExternalWork() {
            verifyNoInteractions(apply, artifacts, representation, connections, probe);
        }
    }
}
