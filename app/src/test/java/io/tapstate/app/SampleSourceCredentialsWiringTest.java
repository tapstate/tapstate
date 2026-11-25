package io.tapstate.app;

import io.tapstate.control.core.SampleSourceCredentialsProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Resolves the production factory method with Boot's mapper without starting runtime infrastructure. */
class SampleSourceCredentialsWiringTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
            .withInitializer(context -> {
                context.getEnvironment().getPropertySources()
                        .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
                // Register the actual factory instance, isolating this bean from store and engine startup.
                context.getBeanFactory().registerSingleton("sampleSourceFactory", new ControlPlaneConfiguration());
                RootBeanDefinition provider = new RootBeanDefinition(SampleSourceCredentialsProvider.class);
                provider.setFactoryBeanName("sampleSourceFactory");
                provider.setFactoryMethodName("sampleSourceCredentialsProvider");
                provider.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
                ((BeanDefinitionRegistry) context.getBeanFactory())
                        .registerBeanDefinition("sampleSourceCredentialsProvider", provider);
            });

    @Test
    void cloudFactoryUsesTheBootJsonMapperWithoutALegacyMapperBean() {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("http://127.0.0.1:1");
        properties.setToken("sample-wiring-token-sentinel");
        properties.setAtlasUri("mongodb://127.0.0.1:27017/sample_wiring");
        properties.setClusterId("sample-wiring-cluster");
        runner.withBean(CloudRuntimeSettings.class, () -> CloudRuntimeSettings.resolve(properties))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(JsonMapper.class);
                    assertThat(context).doesNotHaveBean(com.fasterxml.jackson.databind.ObjectMapper.class);
                    assertThat(context.getBean(SampleSourceCredentialsProvider.class))
                            .isInstanceOf(CloudSampleSourceCredentialsProvider.class);
                });
    }

    @Test
    void onPremFactoryRetainsConfiguredCredentialsWithoutALegacyMapperBean() {
        runner.withBean(CloudRuntimeSettings.class, () -> CloudRuntimeSettings.resolve(new CloudProperties()))
                .withPropertyValues("tapstate.sample.host=sample-wiring.example.test",
                        "tapstate.sample.password=sample-wiring-password-sentinel")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(JsonMapper.class);
                    assertThat(context).doesNotHaveBean(com.fasterxml.jackson.databind.ObjectMapper.class);
                    SampleSourceCredentialsProvider provider = context.getBean(SampleSourceCredentialsProvider.class);
                    assertThat(provider).isInstanceOf(ConfiguredSampleSourceCredentialsProvider.class);
                    assertThat(provider.available()).isTrue();
                    assertThat(provider.fetch()).isEqualTo(new SampleSourceCredentialsProvider.Credentials(
                            "sample-wiring.example.test", "sample-wiring-password-sentinel"));
                });
    }
}
