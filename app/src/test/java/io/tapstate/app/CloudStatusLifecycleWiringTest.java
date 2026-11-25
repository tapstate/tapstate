package io.tapstate.app;

import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.control.core.CloudStatusReporter;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Assembly gating is tested without an SDK substitute in production or real outbound requests. */
class CloudStatusLifecycleWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(context -> context.getEnvironment().getPropertySources()
                    .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME))
            .withUserConfiguration(CloudRuntimeConfiguration.class);

    @Test
    void onPremStaysOffAndCloudRequiresARealReporterByDefault() {
        runner.run(context -> assertThat(context.getBean(CloudStatusLifecycle.class).enabled()).isFalse());
        cloud().run(context -> assertFailure(context.getStartupFailure(), BootError.CLOUD_STATUS_SDK_REQUIRED));
    }

    @Test
    void anOnPremAttemptToEnableTheCloudOnlySenderFailsWithACode() {
        runner.withPropertyValues("SDK_STATUS_SENDER_ENABLED=true")
                .run(context -> assertFailure(context.getStartupFailure(), BootError.CLOUD_STATUS_MODE_REQUIRED));
    }

    @Test
    void enablingCloudReportingWithoutTheRealReporterFailsInsteadOfInventingHeartbeatSuccess() {
        cloud().withPropertyValues("SDK_STATUS_SENDER_ENABLED=true")
                .run(context -> assertFailure(context.getStartupFailure(), BootError.CLOUD_STATUS_SDK_REQUIRED));
    }

    @Test
    void anInvalidSwitchDoesNotEchoItsValue() {
        runner.withPropertyValues("SDK_STATUS_SENDER_ENABLED=invalid-secret-sentinel")
                .run(context -> {
                    assertFailure(context.getStartupFailure(), BootError.CLOUD_STATUS_CONFIG_INVALID);
                    assertThat(context.getStartupFailure()).hasMessageNotContaining("invalid-secret-sentinel");
                });
    }

    @Test
    void aCloudReporterIsEnabledByModeAndDoesNotSendBeforeApplicationReady() {
        AtomicInteger sent = new AtomicInteger();
        CloudStatusReporter controlled = new CloudStatusReporter("validated-fixture-cluster",
                () -> new CloudRuntimeStatus("fixture-version", 0, 0, null),
                (cluster, nonce, status) -> sent.incrementAndGet(), () -> "fixture-nonce");
        cloud().withBean(CloudStatusReporter.class, () -> controlled)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(CloudStatusLifecycle.class).enabled()).isTrue();
                    assertThat(sent.get()).isZero();
                });
        cloud().withBean(CloudStatusReporter.class, () -> controlled)
                .withPropertyValues("SDK_STATUS_SENDER_ENABLED=false")
                .run(context -> assertThat(context.getBean(CloudStatusLifecycle.class).enabled()).isFalse());
        assertThat(sent.get()).isZero();
    }

    private ApplicationContextRunner cloud() {
        return runner.withPropertyValues(
                "tapstate.cloud.base-url=https://cloud.example",
                "tapstate.cloud.token=fixture-token",
                "tapstate.cloud.atlas-uri=mongodb://atlas.example/metadata",
                "tapstate.cloud.cluster-id=lifecycle-cluster");
    }

    private static void assertFailure(Throwable failure, BootError code) {
        assertThat(failure).isNotNull();
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        assertThat(root).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(code));
    }
}
