package io.tapstate.app;

import java.nio.file.Files;
import java.nio.file.Path;

/** Explicit, pinned public artifacts prepared outside the test JVM; never a credential-bearing input. */
final class CloudConnectorTestInputs {
    private CloudConnectorTestInputs() {
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
