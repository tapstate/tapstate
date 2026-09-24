package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;

class ObservedMongoConnectorJarTest {
    @TempDir Path directory;

    @Test
    void preserves_the_entire_delegate_artifact_and_spec_and_packages_only_compiled_observer_classes() throws Exception {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("PDK-API-Version", "2.0.8");
        byte[] spec = "{\"properties\":{\"id\":\"mongodb\"}}".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream out = new JarOutputStream(bytes, manifest)) {
            out.putNextEntry(new JarEntry("spec.json"));
            out.write(spec);
            out.closeEntry();
            out.putNextEntry(new JarEntry("driver-resource.bin"));
            out.write(new byte[] {0, 1, 127, -1});
            out.closeEntry();
        }
        byte[] original = bytes.toByteArray();
        Path witness = directory.resolve("writes.tsv");
        byte[] packaged = ObservedMongoConnectorJar.build(original, witness);
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (JarInputStream in = new JarInputStream(new ByteArrayInputStream(packaged))) {
            assertThat(in.getManifest().getMainAttributes().getValue("PDK-API-Version")).isEqualTo("2.0.8");
            Path delegate = witness.resolveSibling("writes.tsv.mongodb.jar").toAbsolutePath();
            assertThat(in.getManifest().getMainAttributes().getValue("Class-Path")).isEqualTo(delegate.toUri().toString());
            assertThat(Files.readAllBytes(delegate)).containsExactly(original);
            for (JarEntry entry; (entry = in.getNextJarEntry()) != null;) {
                assertThat(entries.put(entry.getName(), in.readAllBytes())).isNull();
            }
        }
        assertThat(entries.keySet()).containsExactlyInAnyOrder(
                "io/tapstate/e2e/mongowitness/ObservedMongoConnector.class",
                "io/tapstate/e2e/mongowitness/WriteTableWitness.class",
                "observed-mongo-spec.json", "mongo-write-witness-path.txt");
        assertThat(entries.get("observed-mongo-spec.json")).containsExactly(spec);
        assertThat(new String(entries.get("mongo-write-witness-path.txt"), StandardCharsets.UTF_8))
                .isEqualTo(witness.toAbsolutePath().toString());
    }
}
