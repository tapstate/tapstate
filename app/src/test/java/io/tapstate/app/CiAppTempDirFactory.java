package io.tapstate.app;

import org.junit.jupiter.api.extension.AnnotatedElementContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDirFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/** Keeps case-owned App fixtures on the runner disk instead of a quota-limited RAM-backed /tmp. */
public final class CiAppTempDirFactory implements TempDirFactory {
    @Override
    public Path createTempDirectory(AnnotatedElementContext elementContext, ExtensionContext extensionContext)
            throws Exception {
        if (!"true".equals(System.getenv("GITHUB_ACTIONS"))) {
            return TempDirFactory.Standard.INSTANCE.createTempDirectory(elementContext, extensionContext);
        }
        String configured = System.getenv("RUNNER_TEMP");
        if (configured == null || configured.isBlank() || !Path.of(configured).isAbsolute()) {
            throw new IllegalStateException("GitHub App test fixtures require an absolute RUNNER_TEMP");
        }
        Path parent = Path.of(configured).resolve("junit-app");
        Files.createDirectories(parent);
        return Files.createTempDirectory(parent, "junit-");
    }
}
