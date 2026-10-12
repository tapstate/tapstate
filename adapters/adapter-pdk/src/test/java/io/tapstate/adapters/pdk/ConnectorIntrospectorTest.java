package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCapabilities;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Self-scan: {@link ConnectorIntrospector} reads a connector artifact and reports its entry class, the
 * spec its {@code @TapConnectorClass} annotation names (path and content), and its declared PDK API
 * version — enough to build a {@link ConnectorRef} and hand the spec to catalog normalization, without
 * being told the entry class up front. Synthetic jars compiled at test time stand in for real
 * connector dist jars.
 */
class ConnectorIntrospectorTest {

    @Test
    void introspectsAConnectorJarIntoItsEntryClassSpecAndApiVersion(@TempDir Path dir) {
        Path jar = Synthetic.annotatedConnector(dir);

        IntrospectedConnector connector = new ConnectorIntrospector().introspect(List.of(jar));

        assertThat(connector.className()).isEqualTo("synthetic.OrdersConnector");
        assertThat(connector.specPath()).isEqualTo("orders-spec.json");
        assertThat(connector.spec()).isEqualTo("{\"id\":\"orders\"}");
        assertThat(connector.pdkApiVersion()).isEqualTo("1.3.5");
    }

    @Test
    void refusesAnArtifactWithNoConnectorClass(@TempDir Path dir) {
        Path jar = Synthetic.jarWithoutConnectorClass(dir);

        assertThatThrownBy(() -> new ConnectorIntrospector().introspect(List.of(jar)))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(ConnectorError.NO_CONNECTOR_CLASS));
    }

    @Test
    void refusesAnArtifactWithMoreThanOneUnrelatedConnectorClass(@TempDir Path dir) {
        Path jar = Synthetic.twoDistinctConnectors(dir);

        assertThatThrownBy(() -> new ConnectorIntrospector().introspect(List.of(jar)))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(ConnectorError.AMBIGUOUS_CONNECTOR_CLASS));
    }

    @Test
    void refusesAConnectorWhoseAnnotationNamesAMissingSpec(@TempDir Path dir) {
        Path jar = Synthetic.annotatedConnectorMissingSpec(dir);

        assertThatThrownBy(() -> new ConnectorIntrospector().introspect(List.of(jar)))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(ConnectorError.SPEC_NOT_FOUND));
    }

    @Test
    void refusesAnUnreadableSpecWithACodedArtifactError(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.annotatedConnector(dir);
        ConnectorIntrospector introspector = new ConnectorIntrospector();
        assertThat(introspector.introspect(List.of(jar)).spec()).isEqualTo("{\"id\":\"orders\"}");
        corruptSpecStream(jar);

        assertThatThrownBy(() -> introspector.introspect(List.of(jar)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ConnectorError.ARTIFACT_UNREADABLE);
                    assertThat(error.args()).containsEntry("artifact", jar.getFileName().toString());
                    assertThat(error).hasCauseInstanceOf(ZipException.class);
                    assertThat(error.getCause().getStackTrace())
                            .anySatisfy(frame -> assertThat(frame.getMethodName()).isEqualTo("readSpec"));
                });
    }

    private static void corruptSpecStream(Path jar) throws Exception {
        byte[] archive = Files.readAllBytes(jar);
        ByteBuffer headers = ByteBuffer.wrap(archive).order(ByteOrder.LITTLE_ENDIAN);
        for (int offset = 0; offset + 30 <= archive.length; offset++) {
            if (headers.getInt(offset) != 0x04034b50) {
                continue;
            }
            int nameLength = Short.toUnsignedInt(headers.getShort(offset + 26));
            int extraLength = Short.toUnsignedInt(headers.getShort(offset + 28));
            int content = offset + 30 + nameLength + extraLength;
            if (content < archive.length && new String(archive, offset + 30, nameLength,
                    StandardCharsets.UTF_8).equals("orders-spec.json")) {
                // A reserved DEFLATE block type makes only the spec unreadable; classes still scan and load.
                archive[content] = 0x07;
                Files.write(jar, archive);
                return;
            }
        }
        throw new AssertionError("the fixture carries no local spec entry");
    }

    @Test
    void refusesAConnectorWhoseAnnotationBytesAreCorrupt(@TempDir Path dir) {
        // The class scans and links, but its annotation attribute bytes are corrupt, so reading the
        // annotation reflectively fails. That is a defective artifact to refuse with a code — never an
        // error that escapes and takes the caller down.
        Path jar = Synthetic.corruptAnnotationConnector(dir);

        assertThatThrownBy(() -> new ConnectorIntrospector().introspect(List.of(jar)))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.LOAD_FAILED));
    }

    @Test
    void keepsTheMostDerivedWhenAConnectorSubclassesAnAnnotatedBase(@TempDir Path dir) {
        Path jar = Synthetic.baseAndVariantConnector(dir);

        IntrospectedConnector connector = new ConnectorIntrospector().introspect(List.of(jar));

        assertThat(connector.className()).isEqualTo("synthetic.VariantConnector");
        assertThat(connector.specPath()).isEqualTo("variant-spec.json");
    }

    @Test
    void theIntrospectedFactsDriveCapabilityDerivation(@TempDir Path dir) {
        // The point of self-scan: turn an artifact into a ConnectorRef the rest of the bridge can drive
        // without being told the entry class. Introspect, build the ref, derive its capabilities.
        Path jar = Synthetic.annotatedEmittingConnector(dir);
        IntrospectedConnector introspected = new ConnectorIntrospector().introspect(List.of(jar));

        ConnectorRef ref = new ConnectorRef(
                List.of(jar), introspected.className(), introspected.pdkApiVersion(), null);
        ConnectorCapabilities caps = new PdkCapabilityDeriver(id -> ref).derive("reader");

        assertThat(caps.capabilityIds()).containsExactly("batch_read_function");
    }
}
