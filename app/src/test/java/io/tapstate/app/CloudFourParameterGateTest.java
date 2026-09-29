package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CloudFourParameterGateTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(context -> context.getEnvironment().getPropertySources()
                    .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME))
            .withPropertyValues("SDK_STATUS_SENDER_ENABLED=false")
            .withUserConfiguration(CloudRuntimeConfiguration.class);

    @ParameterizedTest(name = "partial configuration mask {0} is rejected")
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
    void everyNonemptyProperSubsetFailsBeforeAnyRuntimeIsAssembled(int mask) {
        runner.withPropertyValues(values(mask)).run(context -> {
            assertThat(context).hasFailed();
            Throwable failure = context.getStartupFailure();
            while (failure.getCause() != null) {
                failure = failure.getCause();
            }
            assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(BootError.CLOUD_CONFIG_INCOMPLETE);
                assertThat(error.args()).isEmpty();
                assertThat(error.getCause()).isNull();
                assertThat(error.toString()).doesNotContain("fixture-status-token", "atlas.example");
            });
        });
    }

    @Test
    void noCloudConfigurationPreservesOnPrem() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isFalse();
        });
    }

    @Test
    void allFourParametersSelectCloud() {
        runner.withPropertyValues(values(15)).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isTrue();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"base-url", "token", "atlas-uri", "cluster-id"})
    void aBlankRequiredValueIsNotACompleteCloudConfiguration(String field) {
        runner.withPropertyValues(values(15)).withPropertyValues("tapstate.cloud." + field + "= ")
                .run(context -> assertThat(context).hasFailed());
    }

    private static String[] values(int mask) {
        List<String> values = new ArrayList<>();
        String[] configured = {
                "tapstate.cloud.base-url=https://cloud.example",
                "tapstate.cloud.token=fixture-status-token",
                "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata",
                "tapstate.cloud.cluster-id=allocated-cluster"
        };
        for (int index = 0; index < configured.length; index++) {
            if ((mask & (1 << index)) != 0) {
                values.add(configured[index]);
            }
        }
        return values.toArray(String[]::new);
    }
}
