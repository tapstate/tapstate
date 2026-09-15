package io.tapstate.tools.catalog.derive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Resolves a connector id to its built dist jar — the jar catalog-derive classloads to probe
 * capabilities. The jar is named {@code <id>-connector-<version>.jar}, so the id with its connector suffix matches its own
 * jar without matching a sibling whose name extends it (mysql-connector vs mysql-pxc-connector). A
 * missing or ambiguous match is a build setup error, so it fails loud rather than guessing.
 */
class JarResolverTest {

    @TempDir
    Path dist;

    @Test
    void resolvesTheJarForTheModuleWithoutMatchingASiblingThatExtendsItsName() throws IOException {
        touch("mysql-connector-v1.0-SNAPSHOT.jar");
        touch("mysql-pxc-connector-v1.0-SNAPSHOT.jar");

        assertThat(new JarResolver(dist).resolve("mysql"))
                .isEqualTo(dist.resolve("mysql-connector-v1.0-SNAPSHOT.jar"));
    }

    @Test
    void failsLoudWhenNoJarMatchesTheModule() {
        assertThatThrownBy(() -> new JarResolver(dist).resolve("mysql"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mysql-connector");
    }

    @Test
    void failsLoudWhenMoreThanOneJarMatchesRatherThanGuessing() throws IOException {
        touch("foo-connector-v1.0-SNAPSHOT.jar");
        touch("foo-connector-v2.0-SNAPSHOT.jar");

        assertThatThrownBy(() -> new JarResolver(dist).resolve("foo"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("foo-connector");
    }

    @Test
    void findReturnsEmptyForAModuleWithNoJarSoItCanBeSkippedNotFatal() throws IOException {
        touch("mysql-connector-v1.0-SNAPSHOT.jar");

        // hazelcast-connector and kafka-avro-connector exist in source but are not in the OSS dist
        // build; a missing jar is an expected, recoverable skip, not the build error resolve() raises.
        assertThat(new JarResolver(dist).find("hazelcast")).isEmpty();
        assertThat(new JarResolver(dist).find("mysql"))
                .contains(dist.resolve("mysql-connector-v1.0-SNAPSHOT.jar"));
    }

    @Test
    void findStillFailsLoudOnAmbiguityRatherThanGuessing() throws IOException {
        touch("foo-connector-v1.0-SNAPSHOT.jar");
        touch("foo-connector-v2.0-SNAPSHOT.jar");

        assertThatThrownBy(() -> new JarResolver(dist).find("foo"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("foo-connector");
    }

    private void touch(String jar) throws IOException {
        Files.createFile(dist.resolve(jar));
    }
}
