package io.tapstate.app;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Proves the managed image's Boot JAR carries the SDK that its Cloud bindings compile against. */
class CloudSdkInBootJarIT {

    @Test
    void theBootJarContainsTheCloudOwnedSdkAndNotTheControlPlaneServer() throws Exception {
        String configured = System.getProperty("tapstate.app.boot-jar");
        assumeTrue(configured != null && Files.isRegularFile(Path.of(configured)),
                "no packaged boot jar - not a packaging build, skipping");
        try (JarFile jar = new JarFile(configured)) {
            assertThat(jar.stream().map(entry -> entry.getName()))
                    .anyMatch(name -> name.startsWith("BOOT-INF/lib/cloud-control-plane-sdk-")
                            && name.endsWith(".jar"))
                    .noneMatch(name -> name.contains("cloud-control-plane-server"));
        }
    }
}
