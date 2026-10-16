package io.tapstate.e2e;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/** Uses the existing observer packaging shape and retains the complete original SDK bytes separately. */
final class ObservedCsvConnectorJar {
    private static final String PACKAGE = "io/tapstate/e2e/csvwitness/";
    private ObservedCsvConnectorJar() { }

    static byte[] build(byte[] original, Path entries) throws Exception {
        Manifest manifest;
        byte[] spec = null;
        try (JarInputStream input = new JarInputStream(new ByteArrayInputStream(original))) {
            manifest = input.getManifest();
            for (JarEntry entry; (entry = input.getNextJarEntry()) != null;)
                if (entry.getName().equals("e2e-file-spec.json")) spec = input.readAllBytes();
        }
        if (manifest == null || spec == null) throw new AssertionError("the actual fixture SDK manifest and spec are required");
        Path delegate = entries.resolveSibling(entries.getFileName() + ".e2e_file.jar").toAbsolutePath();
        Files.write(delegate, original);
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, delegate.toUri().toString());
        var resource = ObservedCsvConnectorJar.class.getClassLoader().getResource(PACKAGE);
        if (resource == null) throw new AssertionError("the source entry observer has not been compiled");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream output = new JarOutputStream(bytes, manifest);
                var classes = Files.list(Path.of(resource.toURI()))) {
            for (Path file : classes.filter(Files::isRegularFile).sorted().toList())
                put(output, PACKAGE + file.getFileName(), Files.readAllBytes(file));
            put(output, "observed-csv-spec.json", spec);
            put(output, "csv-entry-witness-path.txt", entries.toAbsolutePath().toString().getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }
    private static void put(JarOutputStream output, String name, byte[] bytes) throws Exception {
        output.putNextEntry(new JarEntry(name));
        output.write(bytes);
        output.closeEntry();
    }
}
