package io.tapstate.adapters.pdk;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Replays the artifact audit behind an explicit API-level registration. */
class PdkApiBinaryContractTest {
    @Test
    void suppliedPdkArtifactsHaveIdenticalClassBytes() throws IOException {
        String baseline = System.getProperty("tapstate.pdk.compatibility.baseline");
        String candidate = System.getProperty("tapstate.pdk.compatibility.candidate");
        assumeTrue(baseline != null || candidate != null, "no PDK artifacts supplied for a binary contract audit");
        assertThat(baseline).as("baseline artifact path").isNotBlank();
        assertThat(candidate).as("candidate artifact path").isNotBlank();
        Map<String, byte[]> expected = classes(Path.of(baseline));
        Map<String, byte[]> actual = classes(Path.of(candidate));
        assertThat(expected).containsKey("io/tapdata/pdk/apis/TapConnector.class");
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        for (String name : expected.keySet()) {
            assertThat(actual.get(name)).as("class bytes of %s", name).isEqualTo(expected.get(name));
        }
        System.out.printf("PDK artifact audit: %d classes have identical bytes%n", expected.size());
    }

    private static Map<String, byte[]> classes(Path path) throws IOException {
        Map<String, byte[]> classes = new TreeMap<>();
        try (JarFile jar = new JarFile(path.toFile())) {
            var entries = jar.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                if (entry.getName().endsWith(".class")) {
                    try (var bytes = jar.getInputStream(entry)) {
                        classes.put(entry.getName(), bytes.readAllBytes());
                    }
                }
            }
        }
        return classes;
    }
}
