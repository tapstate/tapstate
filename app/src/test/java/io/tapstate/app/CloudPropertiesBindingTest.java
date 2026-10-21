package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CloudPropertiesBindingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(context -> context.getEnvironment().getPropertySources()
                    .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME))
            .withPropertyValues("SDK_STATUS_SENDER_ENABLED=false")
            .withUserConfiguration(CloudRuntimeConfiguration.class);

    @Test
    void springPropertyKeysBindTheFourCloudValues() {
        runner.withPropertyValues(
                        "tapstate.cloud.base-url=https://cloud.example",
                        "tapstate.cloud.token=token-from-properties",
                        "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata",
                        "tapstate.cloud.cluster-id=properties-cluster")
                .run(context -> assertBound(context.getBean(CloudProperties.class),
                        "https://cloud.example", "token-from-properties", "mongodb://atlas.example/metadata",
                        "properties-cluster"));
    }

    @Test
    void environmentVariableNamesUseSpringRelaxedBinding() {
        runner.withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("cloud-test-env", Map.of(
                                "TAPSTATE_CLOUD_BASE_URL", "https://env.cloud.example",
                                "TAPSTATE_CLOUD_TOKEN", "token-from-env",
                                "TAPSTATE_CLOUD_ATLAS_URI", "mongodb+srv://env.atlas.example/metadata",
                                "TAPSTATE_CLOUD_CLUSTER_ID", "environment-cluster"))))
                .run(context -> assertBound(context.getBean(CloudProperties.class),
                        "https://env.cloud.example", "token-from-env",
                        "mongodb+srv://env.atlas.example/metadata", "environment-cluster"));
    }

    @Test
    void jvmSystemPropertiesUseTheCanonicalKeys() {
        runner.withSystemProperties(
                        "tapstate.cloud.base-url=https://jvm.cloud.example",
                        "tapstate.cloud.token=token-from-jvm",
                        "tapstate.cloud.atlas-uri=mongodb://jvm.atlas.example/metadata",
                        "tapstate.cloud.cluster-id=jvm-cluster")
                .run(context -> assertBound(context.getBean(CloudProperties.class),
                        "https://jvm.cloud.example", "token-from-jvm",
                        "mongodb://jvm.atlas.example/metadata", "jvm-cluster"));
    }

    @Test
    void aCompleteCloudConfigurationCannotDisableItsMandatoryMetadataStore() {
        runner.withPropertyValues(
                        "tapstate.cloud.base-url=https://cloud.example",
                        "tapstate.cloud.token=token-from-properties",
                        "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata",
                        "tapstate.cloud.cluster-id=properties-cluster",
                        "tapstate.store.mongo.enabled=false")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(TapstateException.class);
                    Throwable cause = context.getStartupFailure();
                    while (cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    assertThat(((TapstateException) cause).code()).isEqualTo(BootError.CLOUD_STORE_REQUIRED);
                });
    }

    @Test
    void onPremMayStillDisableTheStoreForASubstrateOnlyRun() {
        runner.withPropertyValues("tapstate.store.mongo.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isFalse();
                });
    }

    private static void assertBound(
            CloudProperties properties, String baseUrl, String token, String atlasUri, String clusterId) {
        assertThat(properties.getBaseUrl()).isEqualTo(baseUrl);
        assertThat(properties.getToken()).isEqualTo(token);
        assertThat(properties.getAtlasUri()).isEqualTo(atlasUri);
        assertThat(properties.getClusterId()).isEqualTo(clusterId);
    }

    @Test
    void theSdkClusterIdEnvironmentNameSuppliesOnlyTheMissingCanonicalId() {
        runner.withPropertyValues(
                        "tapstate.cloud.base-url=https://cloud.example",
                        "tapstate.cloud.token=token-from-properties",
                        "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata")
                .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("sdk-cluster-env", Map.of("CLUSTER_ID", "sdk-cluster"))))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(CloudRuntimeSettings.class).clusterId()).isEqualTo("sdk-cluster");
                });
    }

    @Test
    void theCanonicalClusterIdWinsOverTheSdkEnvironmentFallback() {
        runner.withPropertyValues(
                        "tapstate.cloud.base-url=https://cloud.example",
                        "tapstate.cloud.token=token-from-properties",
                        "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata",
                        "tapstate.cloud.cluster-id=canonical-cluster")
                .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("sdk-cluster-env", Map.of("CLUSTER_ID", "sdk-cluster"))))
                .run(context -> assertThat(context.getBean(CloudRuntimeSettings.class).clusterId())
                        .isEqualTo("canonical-cluster"));
    }

    @Test
    void aBlankCanonicalClusterIdCannotBeRescuedByAnEnvironmentFallback() {
        runner.withPropertyValues(
                        "tapstate.cloud.base-url=https://cloud.example",
                        "tapstate.cloud.token=token-from-properties",
                        "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata",
                        "tapstate.cloud.cluster-id=")
                .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("sdk-cluster-env", Map.of("CLUSTER_ID", "sdk-cluster"))))
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void theSdkClusterIdAloneCannotSilentlySelectOnPrem() {
        runner.withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("sdk-cluster-env", Map.of("CLUSTER_ID", "sdk-cluster"))))
                .run(context -> assertThat(context).hasFailed());
    }
}
