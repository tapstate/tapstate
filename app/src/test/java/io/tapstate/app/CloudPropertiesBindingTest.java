package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CloudPropertiesBindingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(CloudRuntimeConfiguration.class);

    @Test
    void springPropertyKeysBindTheThreeCloudValues() {
        runner.withPropertyValues(
                        "tapstate.cloud.base-url=https://cloud.example",
                        "tapstate.cloud.token=token-from-properties",
                        "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata")
                .run(context -> assertBound(context.getBean(CloudProperties.class),
                        "https://cloud.example", "token-from-properties", "mongodb://atlas.example/metadata"));
    }

    @Test
    void environmentVariableNamesUseSpringRelaxedBinding() {
        runner.withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("cloud-test-env", Map.of(
                                "TAPSTATE_CLOUD_BASE_URL", "https://env.cloud.example",
                                "TAPSTATE_CLOUD_TOKEN", "token-from-env",
                                "TAPSTATE_CLOUD_ATLAS_URI", "mongodb+srv://env.atlas.example/metadata"))))
                .run(context -> assertBound(context.getBean(CloudProperties.class),
                        "https://env.cloud.example", "token-from-env",
                        "mongodb+srv://env.atlas.example/metadata"));
    }

    @Test
    void jvmSystemPropertiesUseTheCanonicalKeys() {
        runner.withSystemProperties(
                        "tapstate.cloud.base-url=https://jvm.cloud.example",
                        "tapstate.cloud.token=token-from-jvm",
                        "tapstate.cloud.atlas-uri=mongodb://jvm.atlas.example/metadata")
                .run(context -> assertBound(context.getBean(CloudProperties.class),
                        "https://jvm.cloud.example", "token-from-jvm",
                        "mongodb://jvm.atlas.example/metadata"));
    }

    private static void assertBound(
            CloudProperties properties, String baseUrl, String token, String atlasUri) {
        assertThat(properties.getBaseUrl()).isEqualTo(baseUrl);
        assertThat(properties.getToken()).isEqualTo(token);
        assertThat(properties.getAtlasUri()).isEqualTo(atlasUri);
    }
}
