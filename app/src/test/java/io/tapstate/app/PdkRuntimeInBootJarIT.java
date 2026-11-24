package io.tapstate.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Guards that the host half of the PDK is actually packaged into the shipped boot jar, not merely
 * present on a test classpath.
 *
 * <p>Connectors are built against these libraries and do not carry them: they declare them
 * {@code provided}, meaning whoever hosts the connector has them, and the connector loader delegates
 * all of {@code io.tapdata} and an unbundled Log4j API to the host. A server whose boot jar omits one is a server where affected
 * connectors half-work — they load, they connect, and they fail on the first call that reaches a class
 * from the missing library. These libraries are not referenced by app source, so they look unused to
 * anyone reading the pom, and neither absence fails a build.
 *
 * <p>What the packaged artifact is inspected for, rather than the classpath: a test fork carries
 * runtime- and test-scope entries alike, so a witness that drives a real connector stays green whether
 * the dependency ships or not. Downgrading an entry to test scope, or dropping it as unused,
 * removes it from {@code BOOT-INF/lib} and reds here. Runs on every build that packages the boot jar
 * (no database, no external jar); skips only when no boot jar was produced.
 */
class PdkRuntimeInBootJarIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.app.boot-jar";

    @Test
    void manifestDescribesThePackagedHostJars() throws Exception {
        try (var jar = new JarFile(System.getProperty(BOOT_JAR_PROPERTY))) {
            var resource = jar.getJarEntry("META-INF/tapstate/pdk-version.json");
            assertThat(resource).isNotNull();
            var manifest = new ObjectMapper().readTree(jar.getInputStream(resource));
            var modules = new HashSet<String>();
            for (var dependency : manifest.required("dependencies")) {
                String id = dependency.required("artifactId").asText();
                String base = dependency.required("version").asText();
                String resolved = dependency.required("resolvedVersion").asText();
                assertThat(modules.add(id)).as("unique host artifact %s", id).isTrue();
                assertThat(dependency.required("groupId").asText()).isEqualTo("io.tapdata");
                var matching = jar.stream().filter(entry -> entry.getName().equals("BOOT-INF/lib/" + id + "-" + base + ".jar")
                        || entry.getName().equals("BOOT-INF/lib/" + id + "-" + resolved + ".jar")).toList();
                assertThat(matching).as(id).hasSize(1);
                try (var bytes = jar.getInputStream(matching.getFirst())) {
                    var hash = MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes());
                    assertThat(dependency.required("sha256").asText()).isEqualTo(HexFormat.of().formatHex(hash));
                }
                if (base.endsWith("-SNAPSHOT")) {
                    assertThat(resolved).contains(dependency.required("snapshotTimestamp").asText())
                            .doesNotEndWith("-SNAPSHOT");
                }
                if (id.equals("tapdata-pdk-api")) {
                    assertThat(manifest.required("pdkApiVersion").asText()).isEqualTo(base);
                }
            }
            assertThat(modules).contains("tapdata-pdk-api", "tapdata-api", "tapdata-pdk-runner", "tapdata-common",
                    "pdk-error-code", "error-code-scanner", "error-code-core");
        }
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
        // A real connector's base class bootstraps TapRuntime through the host loader at construction.
        "tapdata-pdk-runner, a real connector bootstraps its runtime through the host",
        // Connectors link against these utilities without bundling them; the change-stream path of the
        // MongoDB connector is one that does, and it dies part-way through a follow when they are absent.
        "tapdata-common, connectors declare this provided and link against it through the host",
        "log4j-api, thin journal decoders use the host logging facade at runtime",
    })
    void theBootJarBundlesTheHostSideOfThePdk(String artifactId, String why) throws Exception {
        String bootJar = System.getProperty(BOOT_JAR_PROPERTY);
        assumeTrue(bootJar != null && Files.isRegularFile(Path.of(bootJar)),
                "no packaged boot jar - not a packaging build, skipping");

        String prefix = "BOOT-INF/lib/" + artifactId + "-";
        boolean bundled = false;
        try (JarFile jar = new JarFile(bootJar)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.startsWith(prefix) && name.endsWith(".jar")) {
                    bundled = true;
                    break;
                }
            }
        }
        assertThat(bundled).as("%s bundled in %s (%s)", artifactId, bootJar, why).isTrue();
    }
}
