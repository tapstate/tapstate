package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BootJarWebFixtureTest {

    @TempDir Path work;

    @ParameterizedTest
    @ValueSource(strings = {"cloud", "onprem"})
    void profilesKeepTheLauncherAndStoredNestedDependenciesButReplaceAllWebCopies(String profile)
            throws Exception {
        Path original = original();
        byte[] before = Files.readAllBytes(original);
        Path fixture = BootJarWebFixture.create(original, work.resolve(profile + ".jar"), profile);
        try (JarFile jar = new JarFile(fixture.toFile()); JarFile input = new JarFile(original.toFile())) {
            assertThat(jar.getManifest()).isEqualTo(input.getManifest());
            JarEntry nested = jar.getJarEntry("BOOT-INF/lib/dependency.jar");
            assertThat(nested.getMethod()).isEqualTo(ZipEntry.STORED);
            assertThat(jar.getInputStream(nested).readAllBytes())
                    .isEqualTo(input.getInputStream(input.getJarEntry(nested.getName())).readAllBytes());
            assertThat(jar.getJarEntry("META-INF/tapstate-web.properties")).isNull();
            assertThat(jar.getJarEntry("BOOT-INF/classes/static/assets/old.js")).isNull();
            assertThat(jar.getJarEntry("BOOT-INF/classes/META-INF/tapstate-web.files.sha256")).isNull();
            assertThat(jar.getInputStream(jar.getJarEntry("BOOT-INF/classes/io/tapstate/app/Bootstrap.class"))
                    .readAllBytes()).isEqualTo("original-bytecode".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(WebDistribution.read(fixture.toUri().toURL()).profile().value()).isEqualTo(profile);
        assertThat(Files.readAllBytes(original)).isEqualTo(before);
    }

    @Test
    void aMissingProfileStillHasWebPayloadAndIsNotMistakenForAServerOnlyBuild() throws Exception {
        Path fixture = BootJarWebFixture.create(original(), work.resolve("missing.jar"), null);
        assertThatThrownBy(() -> WebDistribution.read(fixture.toUri().toURL()))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                        coded -> assertThat(coded.code()).isEqualTo(BootError.WEB_PROFILE_INVALID));
    }

    @Test
    void fixtureCreationNeverOverwritesTheRealPackagedInput() throws Exception {
        Path original = original();
        byte[] before = Files.readAllBytes(original);
        assertThatThrownBy(() -> BootJarWebFixture.create(original, original, "onprem"))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        assertThat(Files.readAllBytes(original)).isEqualTo(before);
    }

    private Path original() throws Exception {
        Path archive = work.resolve("original.jar");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS,
                "org.springframework.boot.loader.launch.JarLauncher");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(archive), manifest)) {
            for (var entry : Map.of(
                    "BOOT-INF/classes/io/tapstate/app/Bootstrap.class", "original-bytecode",
                    "BOOT-INF/classes/io/tapstate/app/WebDistribution.class", "owned-origin",
                    "META-INF/tapstate-web.properties", "web.profile=cloud\n",
                    "BOOT-INF/classes/META-INF/tapstate-web.properties", "web.profile=cloud\n",
                    "BOOT-INF/classes/META-INF/tapstate-web.files.sha256", "old-manifest\n",
                    "BOOT-INF/classes/static/index.html", "old-index",
                    "BOOT-INF/classes/static/assets/old.js", "old-assets").entrySet()) {
                output.putNextEntry(new JarEntry(entry.getKey()));
                output.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                output.closeEntry();
            }
            byte[] bytes = "unchanged-nested-library".getBytes(StandardCharsets.UTF_8);
            CRC32 crc = new CRC32();
            crc.update(bytes);
            JarEntry nested = new JarEntry("BOOT-INF/lib/dependency.jar");
            nested.setMethod(ZipEntry.STORED);
            nested.setSize(bytes.length);
            nested.setCompressedSize(bytes.length);
            nested.setCrc(crc.getValue());
            output.putNextEntry(nested);
            output.write(bytes);
            output.closeEntry();
        }
        return archive;
    }
}
