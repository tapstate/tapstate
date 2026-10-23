package io.tapstate.app;

import io.tapstate.spi.store.CapabilityDeriver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Files;
import java.nio.file.Path;

/** Explicit, pinned public artifacts prepared outside the test JVM; never a credential-bearing input. */
final class CloudConnectorTestInputs {
    private CloudConnectorTestInputs() {
    }

    /** Observe the original empty-config load failure before readiness wraps it; never retry or replace it. */
    static void reportCapabilityFailures(ConfigurableApplicationContext context) {
        context.addBeanFactoryPostProcessor(factory -> factory.addBeanPostProcessor(new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (!(bean instanceof CapabilityDeriver original)) {
                    return bean;
                }
                return (CapabilityDeriver) connectorId -> {
                    try {
                        return original.derive(connectorId);
                    } catch (RuntimeException | LinkageError failure) {
                        System.err.println("Cloud test capability derivation failed for " + connectorId);
                        failure.printStackTrace(System.err);
                        throw failure;
                    }
                };
            }
        }));
    }

    static Path seedDirectory() {
        String value = System.getProperty("tapstate.test.cloud-release-dir",
                System.getenv("TAPSTATE_TEST_CLOUD_RELEASE_DIR"));
        if (value == null || value.isBlank()) {
            throw new AssertionError("Prepare the published Cloud connector inputs with "
                    + "deploy/cloud/prepare-test-inputs.py and set TAPSTATE_TEST_CLOUD_RELEASE_DIR to its output; "
                    + "these Cloud startup witnesses require the actual locked release, not synthetic JARs");
        }
        Path release = Path.of(value).toAbsolutePath().normalize();
        if (!Files.isDirectory(release.resolve("connectors"))
                || !Files.isRegularFile(release.resolve("release/connectors.lock.json"))) {
            throw new AssertionError("The prepared Cloud test release is missing its connectors or lock");
        }
        return release.resolve("connectors");
    }
}
