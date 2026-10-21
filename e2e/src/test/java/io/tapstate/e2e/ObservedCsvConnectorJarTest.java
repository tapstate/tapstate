package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObservedCsvConnectorJarTest {
    @TempDir Path directory;

    @Test void theOriginalSdkAndSpecRemainByteIdentical() throws Exception {
        Path sdk = E2eConnectorJar.buildInto(Files.createDirectories(directory.resolve("sdk")));
        byte[] original = Files.readAllBytes(sdk);
        Map<String, byte[]> originalEntries = entries(original);
        Path journal = directory.resolve("source.tsv");
        byte[] wrapped = ObservedCsvConnectorJar.build(original, journal);
        try (JarInputStream input = new JarInputStream(new ByteArrayInputStream(wrapped))) {
            Path delegate = journal.resolveSibling("source.tsv.e2e_file.jar").toAbsolutePath();
            assertThat(input.getManifest().getMainAttributes().getValue(Attributes.Name.CLASS_PATH))
                    .isEqualTo(delegate.toUri().toString());
            assertThat(Files.readAllBytes(delegate)).containsExactly(original);
        }
        Map<String, byte[]> wrappedEntries = entries(wrapped);
        assertThat(wrappedEntries.keySet()).containsExactlyInAnyOrder(
                "io/tapstate/e2e/csvwitness/ObservedCsvConnector.class",
                "observed-csv-spec.json", "csv-entry-witness-path.txt");
        assertThat(wrappedEntries.get("observed-csv-spec.json")).containsExactly(originalEntries.get("e2e-file-spec.json"));
        assertThat(wrappedEntries).doesNotContainKey("io/tapstate/e2e/connector/CsvConnector.class");
    }

    private static Map<String, byte[]> entries(byte[] jar) throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (JarInputStream input = new JarInputStream(new ByteArrayInputStream(jar))) {
            for (var entry = input.getNextJarEntry(); entry != null; entry = input.getNextJarEntry())
                assertThat(entries.put(entry.getName(), input.readAllBytes())).isNull();
        }
        return entries;
    }
}
