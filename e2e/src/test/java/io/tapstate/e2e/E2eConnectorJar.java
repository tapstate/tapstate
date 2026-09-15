package io.tapstate.e2e;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

/**
 * Packages the harness's own connector into the artifact shape the product registers: an annotated
 * entry class, the specification its annotation names, and a manifest declaring the PDK API version.
 *
 * <p>The class is compiled by the build like any other, so the compiler checks it and a reader can read
 * it; this only puts the compiled form into a jar. What it packages is the connector's whole package
 * directory rather than a list of class names - a class compiles into more files than its own when it
 * nests anything, and a jar missing one of them fails at construction time as a load failure, which
 * reads like a product refusal rather than a packaging mistake. Taking the directory cannot miss a
 * file, so that failure cannot happen. The package holds the connector and nothing else, which
 * {@link E2eConnectorJarTest} pins.
 */
final class E2eConnectorJar {

    /** The connector id: the identity the product files the artifact under, declared by the spec. */
    static final String CONNECTOR_ID = "e2e_file";

    /**
     * The id to package this connector under when a specification needs the product to read it rows.
     *
     * <p>Row reads are served only to connectors the product knows speak the request shape it asks in,
     * and that set is a closed one naming real connectors - a test connector does not belong in a
     * shipped list, and would appear in the refusal text users read. So the substitution happens on
     * this side: for those specifications this connector stands in for the one browsable connector
     * there is, and is packaged under its name.
     *
     * <p>What that costs, said plainly: those specifications name a connector they are not driving.
     * They are still testing the product's own logic - the bound on a page, the "there is more" flag,
     * the confinement of a read, the keying of a pooled instance - none of which is MongoDB's. The
     * behaviour that really is MongoDB's is witnessed against the real connector on the real-connector
     * lane, which is the reason this substitution does not leave a hole.
     */
    static final String BROWSABLE_CONNECTOR_ID = "mongodb";

    /** The entry class's package, packaged whole. */
    private static final String PACKAGE_PATH = "io/tapstate/e2e/connector/";

    /** The resource the entry class's {@code @TapConnectorClass} names, resolved as a jar entry. */
    private static final String SPEC_ENTRY = "e2e-file-spec.json";

    /**
     * {@code properties.id} is the identity registration files the artifact under, and {@code dataTypes}
     * is how the connector's own word for a column type becomes a type the product knows.
     *
     * <p>Declaring the one type this connector has is not decoration. A discovered column whose native
     * type maps to nothing resolves as unknown, and an expression reading an unknown column is refused
     * at apply - so without this, every specification whose pipeline reads a row field is refused
     * before it runs. The connector already says every column is text; this is where it says it in the
     * vocabulary the mapping reads.
     */
    private static final String SPEC_TEMPLATE =
            "{\"properties\":{\"id\":\"%s\"},"
                    + "\"configOptions\":{\"connection\":{\"type\":\"object\","
                    + "\"properties\":{\"password\":{\"type\":\"string\","
                    + "\"title\":\"Password\",\"x-component\":\"Password\"}}}},"
                    + "\"dataTypes\":{\"string\":{\"to\":\"TapString\",\"byte\":65535}}}";

    /**
     * The API version the product's level table registers. An unregistered version does not refuse the
     * artifact - it escapes every guard on the way to a bare crash - so this is not a value to pick
     * loosely.
     */
    private static final String PDK_API_VERSION = "2.0.8";

    private E2eConnectorJar() {
    }

    /** Writes the connector jar into {@code directory} and returns it, named so the id finds it. */
    static Path buildInto(Path directory) {
        return buildInto(directory, CONNECTOR_ID);
    }

    /**
     * The same connector packaged under a different declared id.
     *
     * <p>Identity is the specification's, not the class's, so one connector can be packaged as any id -
     * which is what lets a test hand the product an artifact that is well formed in every way except
     * that its id is one this release does not support.
     */
    static Path buildInto(Path directory, String connectorId) {
        Path jar = directory.resolve(connectorId + ".jar");
        Path classes = packageDirectory();
        try (OutputStream out = Files.newOutputStream(jar);
                JarOutputStream jos = new JarOutputStream(out, manifest())) {
            for (Path file : classFiles(classes)) {
                jos.putNextEntry(new JarEntry(PACKAGE_PATH + file.getFileName()));
                Files.copy(file, jos);
                jos.closeEntry();
            }
            jos.putNextEntry(new JarEntry(SPEC_ENTRY));
            jos.write(SPEC_TEMPLATE.formatted(connectorId).getBytes(StandardCharsets.UTF_8));
            jos.closeEntry();
        } catch (IOException e) {
            throw new UncheckedIOException("building the " + connectorId + " connector jar at " + jar, e);
        }
        return jar;
    }

    /** Every compiled file of the connector's package, in a fixed order so the jar is reproducible. */
    private static List<Path> classFiles(Path classes) {
        try (Stream<Path> tree = Files.list(classes)) {
            List<Path> files = new ArrayList<>(tree.filter(Files::isRegularFile).sorted().toList());
            if (files.isEmpty()) {
                throw new IllegalStateException("the connector package at " + classes + " compiled to nothing");
            }
            return files;
        } catch (IOException e) {
            throw new UncheckedIOException("listing the connector package at " + classes, e);
        }
    }

    /**
     * The connector's compiled package, found through the class loader rather than by naming a build
     * directory - the same lookup wherever the tests run from.
     */
    private static Path packageDirectory() {
        URL url = E2eConnectorJar.class.getClassLoader().getResource(PACKAGE_PATH);
        if (url == null) {
            throw new IllegalStateException("the connector package " + PACKAGE_PATH + " is not on this classpath");
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("the connector package resolved to an unusable location: " + url, e);
        }
    }

    private static Manifest manifest() {
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        attributes.put(new Attributes.Name("PDK-API-Version"), PDK_API_VERSION);
        return manifest;
    }
}
