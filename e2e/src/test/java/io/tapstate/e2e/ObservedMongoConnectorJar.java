package io.tapstate.e2e;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/** Packages compiled observer classes with the original connector bytes, never rewriting them. */
final class ObservedMongoConnectorJar {
    private static final String PACKAGE = "io/tapstate/e2e/mongowitness/";

    private ObservedMongoConnectorJar() {}

    static byte[] build(byte[] original, Path witness) throws IOException, URISyntaxException {
        Manifest manifest;
        byte[] spec = null;
        try (JarInputStream in = new JarInputStream(new ByteArrayInputStream(original))) {
            manifest = in.getManifest();
            for (JarEntry entry; (entry = in.getNextJarEntry()) != null;) {
                if (entry.getName().equals("spec.json")) spec = in.readAllBytes();
            }
        }
        if (manifest == null || spec == null) throw new AssertionError("real Mongo jar must carry its manifest and spec");
        // The product owns this loader and closes its manifest class path too, even for a capability
        // probe that never initializes or stops the connector. The test directory owns the bytes.
        Path delegate = witness.resolveSibling(witness.getFileName() + ".mongodb.jar").toAbsolutePath();
        Files.write(delegate, original);
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, delegate.toUri().toString());
        var resource = ObservedMongoConnectorJar.class.getClassLoader().getResource(PACKAGE);
        if (resource == null) throw new AssertionError("observer package was not compiled");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream out = new JarOutputStream(bytes, manifest);
             var classes = Files.list(Path.of(resource.toURI()))) {
            for (Path file : classes.filter(Files::isRegularFile).sorted().toList()) {
                put(out, PACKAGE + file.getFileName(), Files.readAllBytes(file));
            }
            put(out, "observed-mongo-spec.json", spec);
            put(out, "mongo-write-witness-path.txt", witness.toAbsolutePath().toString().getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }

    private static void put(JarOutputStream out, String name, byte[] bytes) throws IOException {
        out.putNextEntry(new JarEntry(name));
        out.write(bytes);
        out.closeEntry();
    }
}
