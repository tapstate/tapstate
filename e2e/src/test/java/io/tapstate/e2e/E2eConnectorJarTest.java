package io.tapstate.e2e;

import io.tapstate.adapters.pdk.PdkApiLevels;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The connector artifact the harness hands the product, checked as an artifact - because every way it
 * can be wrong fails far from here and reads like something else.
 */
class E2eConnectorJarTest {

    /**
     * What the connector's classes are allowed to name. The isolating loader delegates only the frozen
     * PDK contract to the host and resolves everything else against the jar, so a class named here that
     * is not in the jar and not on this list does not exist as far as the connector is concerned.
     */
    private static final List<String> REACHABLE = List.of(
            "java/", "javax/", "jdk/", "io/tapdata/", "io/tapstate/e2e/connector/");

    @TempDir
    private Path directory;

    @Test
    void theJarCarriesTheEntryClassAndTheSpecificationItsAnnotationNames() throws IOException {
        Path jar = E2eConnectorJar.buildInto(directory);

        try (JarFile file = new JarFile(jar.toFile())) {
            assertThat(file.getJarEntry("io/tapstate/e2e/connector/CsvConnector.class")).isNotNull();
            String spec = new String(file.getInputStream(file.getJarEntry("e2e-file-spec.json")).readAllBytes(),
                    StandardCharsets.UTF_8);
            assertThat(JsonReader.parse(spec)).isInstanceOf(java.util.Map.class);
            assertThat(spec).contains("\"id\":\"e2e_file\"", "\"sslKey\"", "\"sslKeyPassword\"");
        }
    }

    /** The id names the jar, because that is how a specification's registered connector is found. */
    @Test
    void theJarIsNamedSoTheConnectorIdFindsIt() {
        assertThat(E2eConnectorJar.buildInto(directory)).hasFileName("e2e_file.jar");
    }

    /**
     * The version stamped on the manifest is pinned to the product's own level table rather than agreed
     * with it by hand. A version the table does not register is not refused by the product: it escapes
     * every guard as a bare crash at start, a long way from the manifest that caused it. So the check
     * is made here, where the stamp is, and made against the table itself - a second copy of "2.0.8" in
     * this test would only ever agree with the first one.
     */
    @Test
    void theStampedApiVersionIsOneTheProductsLevelTableRegisters() throws IOException {
        Path jar = E2eConnectorJar.buildInto(directory);

        try (JarFile file = new JarFile(jar.toFile())) {
            String stamped = file.getManifest().getMainAttributes().getValue("PDK-API-Version");

            assertThat(stamped).isNotBlank();
            assertThatCode(() -> PdkApiLevels.level(stamped)).doesNotThrowAnyException();
        }
    }

    /**
     * The rule the connector cannot break and the compiler cannot catch: it lives in this module, so a
     * reference to any of the harness's own classes compiles perfectly and then fails to link inside the
     * product, where it surfaces as a coded load failure that reads like the product refusing the
     * artifact. This is the only place that mistake is cheap to find.
     */
    @Test
    void theConnectorNamesNothingItsLoaderCannotReach() throws IOException {
        Path jar = E2eConnectorJar.buildInto(directory);

        Set<String> unreachable = new LinkedHashSet<>();
        try (JarFile file = new JarFile(jar.toFile())) {
            for (ZipEntry entry : file.stream().filter(e -> e.getName().endsWith(".class")).toList()) {
                for (String named : classesNamedBy(file.getInputStream(entry).readAllBytes())) {
                    if (REACHABLE.stream().noneMatch(named::startsWith)) {
                        unreachable.add(named);
                    }
                }
            }
        }

        assertThat(unreachable)
                .describedAs("the connector loads in isolation and can reach only %s", REACHABLE)
                .isEmpty();
    }

    /**
     * The packager takes the connector's whole package rather than a list of names, which is only safe
     * while the package holds the connector and nothing else. A second class dropped in here would be
     * packaged into every connector jar silently.
     */
    @Test
    void theConnectorPackageHoldsTheConnectorAndNothingElse() throws IOException {
        Path jar = E2eConnectorJar.buildInto(directory);

        try (JarFile file = new JarFile(jar.toFile())) {
            assertThat(file.stream().map(ZipEntry::getName).filter(name -> name.endsWith(".class")))
                    .isNotEmpty()
                    .allMatch(name -> name.startsWith("io/tapstate/e2e/connector/CsvConnector"));
        }
    }

    /**
     * Every type a class file names, read out of its constant pool. Reading the pool rather than
     * searching the bytes for a prefix is what makes this answer the linker's question: these are the
     * entries the loader resolves, so a name absent here is a name that cannot fail to link.
     */
    private static List<String> classesNamedBy(byte[] classFile) {
        ByteBuffer buffer = ByteBuffer.wrap(classFile);
        buffer.position(8); // magic, minor, major
        int count = Short.toUnsignedInt(buffer.getShort());
        String[] utf8 = new String[count];
        List<Integer> classNameIndexes = new ArrayList<>();
        for (int slot = 1; slot < count; slot++) {
            int tag = Byte.toUnsignedInt(buffer.get());
            switch (tag) {
                case 1 -> { // Utf8
                    byte[] text = new byte[Short.toUnsignedInt(buffer.getShort())];
                    buffer.get(text);
                    utf8[slot] = new String(text, StandardCharsets.UTF_8);
                }
                case 7 -> classNameIndexes.add(Short.toUnsignedInt(buffer.getShort())); // Class
                case 8, 16, 19, 20 -> buffer.position(buffer.position() + 2);
                case 15 -> buffer.position(buffer.position() + 3); // MethodHandle
                case 3, 4, 9, 10, 11, 12, 17, 18 -> buffer.position(buffer.position() + 4);
                case 5, 6 -> { // Long / Double take two slots
                    buffer.position(buffer.position() + 8);
                    slot++;
                }
                default -> throw new IllegalStateException("unknown constant pool tag " + tag);
            }
        }
        List<String> named = new ArrayList<>();
        for (int index : classNameIndexes) {
            named.add(elementType(utf8[index]));
        }
        return named;
    }

    /** An array's element type; a plain class name is its own. Arrays name their element, not "[". */
    private static String elementType(String name) {
        String stripped = name;
        while (stripped.startsWith("[")) {
            stripped = stripped.substring(1);
        }
        if (stripped.startsWith("L") && stripped.endsWith(";")) {
            stripped = stripped.substring(1, stripped.length() - 1);
        }
        return stripped;
    }
}
