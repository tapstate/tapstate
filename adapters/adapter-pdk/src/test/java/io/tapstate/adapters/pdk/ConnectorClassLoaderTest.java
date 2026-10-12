package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.TapConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ConnectorClassLoaderTest {

    /** A connector-shaped class whose {@code tag()} returns the given marker, so two builds differ. */
    private static Path widgetJar(Path dir, String marker) {
        return SyntheticJar.compileToJar(dir, "synthetic.Widget",
                "package synthetic; public class Widget { public String tag() { return \"" + marker + "\"; } }");
    }

    @Test
    void loadsAClassFromTheConnectorJar(@TempDir Path dir) throws Exception {
        Path jar = widgetJar(dir, "A");
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(jar))) {
            Class<?> widget = loader.load("synthetic.Widget");
            assertThat(widget.getName()).isEqualTo("synthetic.Widget");
            // The class comes from the connector loader, not leaked in from the host.
            assertThat(widget.getClassLoader()).isNotSameAs(getClass().getClassLoader());
        }
    }

    @Test
    void twoConnectorsWithTheSameClassNameStayIsolated(@TempDir Path dir) throws Exception {
        Path jarA = widgetJar(dir.resolve("a"), "A");
        Path jarB = widgetJar(dir.resolve("b"), "B");
        try (ConnectorClassLoader a = ConnectorClassLoader.open(List.of(jarA));
             ConnectorClassLoader b = ConnectorClassLoader.open(List.of(jarB))) {
            Class<?> wa = a.load("synthetic.Widget");
            Class<?> wb = b.load("synthetic.Widget");
            // Same name, different Class objects: neither loader pollutes the other.
            assertThat(wa).isNotSameAs(wb);
            String ta = (String) wa.getMethod("tag").invoke(wa.getDeclaredConstructor().newInstance());
            String tb = (String) wb.getMethod("tag").invoke(wb.getDeclaredConstructor().newInstance());
            assertThat(ta).isEqualTo("A");
            assertThat(tb).isEqualTo("B");
        }
    }

    @Test
    void connectorsCannotSeeHostApplicationClasses(@TempDir Path dir) throws Exception {
        Path jar = widgetJar(dir, "A");
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(jar))) {
            // A tapstate class that IS on the host classpath must stay invisible to the connector.
            assertThatThrownBy(() -> loader.load("io.tapstate.adapters.pdk.ConnectorClassLoader"))
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    void theSharedPdkContractResolvesToTheHostClass(@TempDir Path dir) throws Exception {
        Path jar = widgetJar(dir, "A");
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(jar))) {
            // The PDK contract is shared from the host: one TapConnector type, not a per-connector copy.
            Class<?> shared = loader.load("io.tapdata.pdk.apis.TapConnector");
            assertThat(shared).isSameAs(TapConnector.class);
        }
    }

    @Test
    void sharesTheRuntimeCodegenLibraryFromTheHost(@TempDir Path dir) throws Exception {
        // The PDK runtime maps connection config through cglib: it generates a BeanMap subclass and
        // defines it into the connector's own loader (the config bean lives there). That generated class
        // extends net.sf.cglib.beans.BeanMap, so the connector loader must resolve cglib from the host,
        // or a real connector's first config load dies linking the generated class. cglib rides the PDK
        // runtime, present only in the real-connector lane, so this reads through to the host class there
        // and stays out of the runtime-free default build.
        Class<?> hostBeanMap;
        try {
            hostBeanMap = Class.forName("net.sf.cglib.beans.BeanMap");
        } catch (ClassNotFoundException noRuntimeOnDefaultBuild) {
            assumeTrue(false, "cglib is not on the default host classpath; the real-connector lane covers this");
            return;
        }
        Path jar = widgetJar(dir, "A");
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(jar))) {
            Class<?> shared = loader.load("net.sf.cglib.beans.BeanMap");
            assertThat(shared).isSameAs(hostBeanMap);
        }
    }

    /**
     * Every caller over one artifact gets the one loader, and so the one copy of each class: a connector that
     * binds a JNI library to the loader it was loaded through finds that loader again on its next open.
     * {@code open} still gives each caller a loader of its own.
     */
    @Test
    void everyCallerOverOneArtifactSharesOneLoader(@TempDir Path dir) throws Exception {
        Path jar = widgetJar(dir, "A");
        ConnectorClassLoader first = ConnectorClassLoader.shared(List.of(jar));
        ConnectorClassLoader second = ConnectorClassLoader.shared(List.of(jar));
        assertThat(second).isSameAs(first);
        assertThat(second.load("synthetic.Widget")).isSameAs(first.load("synthetic.Widget"));
        try (ConnectorClassLoader own = ConnectorClassLoader.open(List.of(jar))) {
            assertThat(own.load("synthetic.Widget")).isNotSameAs(first.load("synthetic.Widget"));
        }
    }

    @Test
    void sharedLoadersOverDifferentArtifactsStayIsolated(@TempDir Path dir) throws Exception {
        Path jarA = widgetJar(dir.resolve("a"), "A");
        Path jarB = widgetJar(dir.resolve("b"), "B");
        Class<?> wa = ConnectorClassLoader.shared(List.of(jarA)).load("synthetic.Widget");
        Class<?> wb = ConnectorClassLoader.shared(List.of(jarB)).load("synthetic.Widget");
        assertThat(wa).isNotSameAs(wb);
        assertThat(wa.getMethod("tag").invoke(wa.getDeclaredConstructor().newInstance())).isEqualTo("A");
        assertThat(wb.getMethod("tag").invoke(wb.getDeclaredConstructor().newInstance())).isEqualTo("B");
    }

    /** One caller finishing with a shared loader must not take it from the others: its close does nothing. */
    @Test
    void closingASharedLoaderLeavesItToTheOtherCallers(@TempDir Path dir) throws Exception {
        Path jar = messagesJar(dir);
        ConnectorClassLoader shared = ConnectorClassLoader.shared(List.of(jar));
        shared.close();
        Class<?> widget = ConnectorClassLoader.shared(List.of(jar)).load("synthetic.Widget");
        assertThat(widget.getClassLoader()).isSameAs(shared.load("synthetic.Widget").getClassLoader());
        try (InputStream messages = widget.getClassLoader().getResourceAsStream(MESSAGES_ENTRY)) {
            assertThat(messages.readAllBytes()).isEqualTo(MESSAGES);
        }
    }

    /** A jar replaced in place is a different artifact, and gets its own classes rather than the old ones. */
    @Test
    void aJarReplacedInPlaceGetsALoaderOfItsOwn(@TempDir Path dir) throws Exception {
        Path jar = dir.resolve("connector.jar");
        Files.copy(widgetJar(dir.resolve("a"), "A"), jar);
        Class<?> before = ConnectorClassLoader.shared(List.of(jar)).load("synthetic.Widget");

        Files.copy(widgetJar(dir.resolve("b"), "a longer marker"), jar, StandardCopyOption.REPLACE_EXISTING);
        Class<?> after = ConnectorClassLoader.shared(List.of(jar)).load("synthetic.Widget");

        assertThat(after).isNotSameAs(before);
        assertThat(after.getMethod("tag").invoke(after.getDeclaredConstructor().newInstance()))
                .isEqualTo("a longer marker");
    }

    @Test
    void closeReleasesTheLoader(@TempDir Path dir) throws Exception {
        Path jar = widgetJar(dir, "A");
        ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(jar));
        loader.close();
        // After close the jar is released; a not-yet-loaded class can no longer be resolved.
        assertThatThrownBy(() -> loader.load("synthetic.Widget"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    /** A resource long enough that reading it spans several reads of the jar. */
    private static final byte[] MESSAGES = "message.key=a message text long enough to span several reads\n"
            .repeat(1024).getBytes(StandardCharsets.ISO_8859_1);

    private static final String MESSAGES_ENTRY = "synthetic/messages.properties";

    /** A jar holding a class, to open a loader through, and {@link #MESSAGES}. */
    private static Path messagesJar(Path dir) {
        return SyntheticJar.jarWithEntries(dir, Map.of(
                "synthetic/Widget.class", SyntheticJar.classBytes(dir, "synthetic.Widget",
                        "package synthetic; public class Widget { }"),
                MESSAGES_ENTRY, MESSAGES), Map.of());
    }

    /**
     * Two connectors over one jar - the two sources of one pipeline, say: closing one while the other is part
     * way through a resource leaves that read whole. Both reading through one file the first had recorded as
     * its own, the close ended the other read with "Stream closed", and a driver that loads its messages in a
     * static initializer failed it for good.
     */
    @Test
    void closingOneConnectorLeavesAResourceAnotherIsReadingWhole(@TempDir Path dir) throws Exception {
        Path jar = messagesJar(dir);
        ConnectorClassLoader first = ConnectorClassLoader.open(List.of(jar));
        try (ConnectorClassLoader second = ConnectorClassLoader.open(List.of(jar))) {
            ClassLoader firstLoader = first.load("synthetic.Widget").getClassLoader();
            ClassLoader secondLoader = second.load("synthetic.Widget").getClassLoader();
            try (InputStream earlier = firstLoader.getResourceAsStream(MESSAGES_ENTRY)) {
                assertThat(earlier.readAllBytes()).hasSize(MESSAGES.length);
            }
            try (InputStream reading = secondLoader.getResourceAsStream(MESSAGES_ENTRY)) {
                first.close();
                assertThat(reading.readAllBytes()).isEqualTo(MESSAGES);
            }
        }
    }

    /**
     * The same meeting, with the second read made the way a resource bundle makes it: through the resource's
     * URL, over the jar file the whole process shares - the MySQL driver's messages load this way. Closing a
     * connector that had read a resource of the jar itself closed that shared file under the read.
     */
    @Test
    void closingOneConnectorLeavesAReadThroughTheSharedJarFileWhole(@TempDir Path dir) throws Exception {
        Path jar = messagesJar(dir);
        ConnectorClassLoader first = ConnectorClassLoader.open(List.of(jar));
        try (ConnectorClassLoader second = ConnectorClassLoader.open(List.of(jar))) {
            ClassLoader firstLoader = first.load("synthetic.Widget").getClassLoader();
            ClassLoader secondLoader = second.load("synthetic.Widget").getClassLoader();
            try (InputStream earlier = firstLoader.getResourceAsStream(MESSAGES_ENTRY)) {
                assertThat(earlier.readAllBytes()).hasSize(MESSAGES.length);
            }
            URLConnection shared = secondLoader.getResource(MESSAGES_ENTRY).openConnection();
            assertThat(shared.getUseCaches()).as("the read goes through the shared file").isTrue();
            try (InputStream reading = shared.getInputStream()) {
                first.close();
                assertThat(reading.readAllBytes()).isEqualTo(MESSAGES);
            }
        }
    }

    /**
     * A resource that cannot be read answers as absent, the way {@link java.net.URLClassLoader} answers: one
     * the jar does not hold, and one it holds but that can no longer be opened - here a jar whose file went
     * away after the loader opened it, so the entry is still found and the jar cannot be opened again to read
     * it. A caller asking for a resource is not the one to be told about a jar that stopped being readable.
     */
    @Test
    void aResourceThatCannotBeReadIsAbsent(@TempDir Path dir) throws Exception {
        Path jar = messagesJar(dir);
        try (ConnectorClassLoader connector = ConnectorClassLoader.open(List.of(jar))) {
            ClassLoader loader = connector.load("synthetic.Widget").getClassLoader();
            assertThat(loader.getResourceAsStream("synthetic/absent.properties")).isNull();

            Files.delete(jar);
            assertThat(loader.getResource(MESSAGES_ENTRY))
                    .as("the entry, found through the jar the loader already has open")
                    .isNotNull();
            assertThat(loader.getResourceAsStream(MESSAGES_ENTRY)).isNull();
        }
    }

    @Test
    void loadConnectorClassRejectsANonConnectorClass(@TempDir Path dir) throws Exception {
        Path jar = widgetJar(dir, "A");
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(jar))) {
            // synthetic.Widget does not implement TapConnector: loading it as a connector is refused.
            assertThatThrownBy(() -> loader.loadConnectorClass("synthetic.Widget"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("synthetic.Widget");
        }
    }
}
