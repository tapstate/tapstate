package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Test;

class DistributionTarballContainsLauncherIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.app.boot-jar";

    @Test
    void theDistributionShipsAnExecutableLauncher() throws Exception {
        String bootJarProperty = System.getProperty(BOOT_JAR_PROPERTY);
        assertThat(bootJarProperty).as(BOOT_JAR_PROPERTY).isNotBlank().endsWith("-boot.jar");

        Path bootJar = Path.of(bootJarProperty);
        String distributionName = bootJar.getFileName().toString().replace("-boot.jar", "-dist.tar.gz");
        Path distribution = bootJar.resolveSibling(distributionName);
        assertThat(distribution).as("distribution built beside %s", bootJar).isRegularFile();

        TarArchiveEntry launcher = null;
        List<String> entries = new ArrayList<>();
        try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(
                new BufferedInputStream(Files.newInputStream(distribution))))) {
            TarArchiveEntry entry;
            while ((entry = archive.getNextTarEntry()) != null) {
                entries.add(entry.getName());
                if (entry.getName().endsWith("/bin/tapstate-server")) {
                    launcher = entry;
                }
            }
        }

        assertThat(launcher)
                .withFailMessage("%s has no bin/tapstate-server; entries were %s", distribution, entries)
                .isNotNull();
        assertThat(launcher.getMode() & 0111)
                .as("execute bits on %s in %s", launcher.getName(), distribution)
                .isNotZero();
    }
}
