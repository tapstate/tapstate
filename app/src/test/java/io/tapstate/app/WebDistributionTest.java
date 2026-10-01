package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebDistributionTest {

    private static final String CLASS = "io/tapstate/app/WebDistribution.class";
    private static final String METADATA = "META-INF/tapstate-web.properties";
    private static final String BOOT_CLASSES = "BOOT-INF/classes/";
    private static final String CONSOLE = "https://console.example/cluster?return=%2F";

    @TempDir
    Path directory;

    @Test
    void aGenuinelyServerOnlyOriginPreservesBothStartupModes() throws Exception {
        WebDistribution distribution = WebDistribution.read(exploded("server-only", null, Map.of()));

        assertThat(distribution.profile()).isEqualTo(WebDistribution.Profile.SERVER_ONLY);
        distribution.validate(CloudRuntimeSettings.resolve(new CloudProperties()));
        distribution.validate(cloud());
    }

    @Test
    void anExplodedOriginReadsOnlyItsOwnProfile() throws Exception {
        WebDistribution distribution = WebDistribution.read(exploded("cloud", cloudMetadata(CONSOLE), index()));

        assertThat(distribution.profile()).isEqualTo(WebDistribution.Profile.CLOUD);
        assertThat(distribution.consoleUrl()).isEqualTo(CONSOLE);
        distribution.validate(cloud());
        assertThatThrownBy(() -> distribution.validate(CloudRuntimeSettings.resolve(new CloudProperties())))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(BootError.WEB_PROFILE_MODE_MISMATCH);
                    assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("profile", "cloud", "mode", "onprem"));
                    assertThat(failure.getCause()).isNull();
                });
    }

    @Test
    void onPremMetadataAcceptsOnlyTheExistingOnPremMode() throws Exception {
        WebDistribution distribution = WebDistribution.read(exploded("onprem", "web.profile=onprem\n", index()));

        assertThat(distribution.profile()).isEqualTo(WebDistribution.Profile.ON_PREM);
        assertThat(distribution.consoleUrl()).isNull();
        distribution.validate(CloudRuntimeSettings.resolve(new CloudProperties()));
        assertThatThrownBy(() -> distribution.validate(cloud()))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_PROFILE_MODE_MISMATCH));
    }

    @Test
    void bootArchiveRootMetadataIsReadWithoutUsingTheClassLoaderSearchOrder() throws Exception {
        Path archive = archive("boot-root.jar", BOOT_CLASSES, Map.of(METADATA, cloudMetadata(CONSOLE)), index());

        WebDistribution distribution = WebDistribution.read(jarUrl(archive, BOOT_CLASSES));

        assertThat(distribution.profile()).isEqualTo(WebDistribution.Profile.CLOUD);
        assertThat(distribution.consoleUrl()).isEqualTo(CONSOLE);
    }

    @Test
    void bootArchiveClassesMetadataAndIdenticalRootCopiesAreAccepted() throws Exception {
        Path classesOnly = archive("boot-classes.jar", BOOT_CLASSES,
                Map.of(BOOT_CLASSES + METADATA, cloudMetadata(CONSOLE)), index());
        Path twoCopies = archive("boot-two-copies.jar", BOOT_CLASSES, Map.of(
                METADATA, cloudMetadata(CONSOLE), BOOT_CLASSES + METADATA, cloudMetadata(CONSOLE)), index());

        assertThat(WebDistribution.read(jarUrl(classesOnly, BOOT_CLASSES)).profile())
                .isEqualTo(WebDistribution.Profile.CLOUD);
        assertThat(WebDistribution.read(jarUrl(twoCopies, BOOT_CLASSES)).profile())
                .isEqualTo(WebDistribution.Profile.CLOUD);
    }

    @Test
    void theBootNestedClassUrlStillSelectsTheExactOuterArchive() throws Exception {
        Path archive = archive("boot nested.jar", BOOT_CLASSES, Map.of(METADATA, cloudMetadata(CONSOLE)), index());
        URL nested = new URL(null,
                "jar:nested:" + archive.toUri().getRawPath() + "/!" + BOOT_CLASSES + "!/",
                new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL url) {
                        throw new AssertionError("resource lookup must open the owned archive, not the fixture URL");
                    }
                });

        assertThat(WebDistribution.read(nested).profile()).isEqualTo(WebDistribution.Profile.CLOUD);
    }

    @Test
    void anOrdinaryJarKeepsItsOwnRootMetadataAndStaticPayload() throws Exception {
        Path archive = archive("plain.jar", "", Map.of(METADATA, "web.profile=onprem\n"), index());

        assertThat(WebDistribution.read(jarUrl(archive, "")).profile())
                .isEqualTo(WebDistribution.Profile.ON_PREM);
    }

    @Test
    void codeSourceDirectoryAndThinJarIdentifyTheOriginWithoutResourceSearch() throws Exception {
        exploded("code-source-directory", "web.profile=onprem\n", index());
        Path archive = archive("thin code source.jar", "", Map.of(METADATA, "web.profile=onprem\n"), index());

        assertThat(WebDistribution.read(directory.resolve("code-source-directory").toUri().toURL()).profile())
                .isEqualTo(WebDistribution.Profile.ON_PREM);
        assertThat(WebDistribution.read(archive.toUri().toURL()).profile())
                .isEqualTo(WebDistribution.Profile.ON_PREM);
        assertThat(WebDistribution.read(new URL("jar:" + archive.toUri() + "!/")).profile())
                .isEqualTo(WebDistribution.Profile.ON_PREM);
    }

    @Test
    void codeSourceBootArchiveRecognizesRootAndClassesOrigins() throws Exception {
        Path archive = archive("boot code source.jar", BOOT_CLASSES,
                Map.of(METADATA, cloudMetadata(CONSOLE)), index());

        assertThat(WebDistribution.read(archive.toUri().toURL()).profile())
                .isEqualTo(WebDistribution.Profile.CLOUD);
        assertThat(WebDistribution.read(new URL("jar:" + archive.toUri() + "!/" + BOOT_CLASSES)).profile())
                .isEqualTo(WebDistribution.Profile.CLOUD);
        for (String prefix : new String[]{"jar:nested:", "nested:"}) {
            for (String suffix : new String[]{BOOT_CLASSES, BOOT_CLASSES + "!/"}) {
                URL source = locationUrl(prefix + archive.toUri().getRawPath() + "/!" + suffix);
                assertThat(WebDistribution.read(source).profile()).isEqualTo(WebDistribution.Profile.CLOUD);
            }
        }
    }

    @Test
    void aDependencyResourceOverrideCannotSubstituteForTheProtectionDomainOrigin() throws Exception {
        URL ownedSource = exploded("owned-code-source", "web.profile=onprem\n", index());
        exploded("foreign-class-resource", cloudMetadata(CONSOLE), index());
        URL foreignResource = directory.resolve("foreign-class-resource").resolve(CLASS).toUri().toURL();
        Class<?> isolated = isolatedDistribution(ownedSource, foreignResource);
        assertThat(isolated.getProtectionDomain().getCodeSource().getLocation()).isEqualTo(ownedSource);
        assertThat(isolated.getResource("WebDistribution.class")).isEqualTo(foreignResource);

        var bundled = isolated.getDeclaredMethod("bundled");
        bundled.setAccessible(true);
        Object distribution = bundled.invoke(null);
        var profile = isolated.getDeclaredMethod("profile");
        profile.setAccessible(true);

        assertThat(((Enum<?>) profile.invoke(distribution)).name()).isEqualTo("ON_PREM");
    }

    @Test
    void dependencyCodeSourcesAndAmbiguousArchivesHaveNoFallback() throws Exception {
        Path archive = archive("boot-code-source.jar", BOOT_CLASSES,
                Map.of(METADATA, cloudMetadata(CONSOLE)), index());
        URL dependency = locationUrl("jar:nested:" + archive.toUri().getRawPath()
                + "/!BOOT-INF/lib/dependency.jar!/");
        Path ambiguous = archive("ambiguous-code-source.jar", BOOT_CLASSES,
                Map.of(METADATA, cloudMetadata(CONSOLE), CLASS, "second class fixture"), index());

        assertProfileInvalid(() -> WebDistribution.read(dependency),
                "application resource origin is unsupported");
        assertProfileInvalid(() -> WebDistribution.read(ambiguous.toUri().toURL()),
                "application class origin is absent or ambiguous");
        assertProfileInvalid(() -> WebDistribution.read(null),
                "application resource origin is unavailable");
    }

    @Test
    void conflictingRootAndClassesMetadataCannotChooseAConvenientMode() throws Exception {
        Path archive = archive("conflicting.jar", BOOT_CLASSES, Map.of(
                METADATA, cloudMetadata(CONSOLE), BOOT_CLASSES + METADATA, "web.profile=onprem\n"), index());

        assertProfileInvalid(() -> WebDistribution.read(jarUrl(archive, BOOT_CLASSES)),
                "Web metadata copies conflict");
    }

    @Test
    void dependencyAndParentMetadataCannotSupplyAnUnstampedWebOrigin() throws Exception {
        URL own = exploded("own", null, index());
        Files.createDirectories(directory.resolve("META-INF"));
        Files.writeString(directory.resolve(METADATA), cloudMetadata(CONSOLE));
        Path dependency = archive("dependency.jar", "", Map.of(METADATA, cloudMetadata(CONSOLE)), index());
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader foreign = new URLClassLoader(new URL[]{dependency.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(foreign);
            assertProfileInvalid(() -> WebDistribution.read(own), "Web metadata is missing");
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void foreignMetadataCannotTurnAServerOnlyOriginIntoACloudDistribution() throws Exception {
        URL own = exploded("own-server", null, Map.of());
        Path dependency = archive("foreign-cloud.jar", "", Map.of(METADATA, cloudMetadata(CONSOLE)), index());
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader foreign = new URLClassLoader(new URL[]{dependency.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(foreign);
            assertThat(WebDistribution.read(own).profile()).isEqualTo(WebDistribution.Profile.SERVER_ONLY);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void orphanAssetsCountAsWebPayloadEvenWithoutAnIndex() throws Exception {
        URL own = exploded("orphan-assets", null, Map.of("assets/app.js", "export default {}"));

        assertProfileInvalid(() -> WebDistribution.read(own), "Web metadata is missing");
    }

    @Test
    void aProfileWithoutACompleteIndexCannotMasqueradeAsAServerOnlyBuild() throws Exception {
        URL noPayload = exploded("no-payload", "web.profile=onprem\n", Map.of());
        URL emptyIndex = exploded("empty-index", "web.profile=onprem\n", Map.of("index.html", ""));
        URL assetsOnly = exploded("assets-only", "web.profile=onprem\n", Map.of("assets/app.js", "code"));

        for (URL origin : new URL[]{noPayload, emptyIndex, assetsOnly}) {
            assertProfileInvalid(() -> WebDistribution.read(origin), "Web index payload is missing or empty");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "revision=known\n", "web.profile=default\n", "web.profile=CLOUD\n",
            "web.profile=cloud \n"})
    void presentMissingOrUnknownProfilesNeverBecomeServerOnly(String metadata) throws Exception {
        URL own = exploded("unknown-profile", metadata, index());

        assertProfileInvalid(() -> WebDistribution.read(own), "Web profile is missing or unsupported");
    }

    @Test
    void malformedPropertiesAndDuplicateKeysFailWithoutParserInputsOrCauses() throws Exception {
        URL duplicate = exploded("duplicate", "web.profile=onprem\nweb.profile=cloud\n", index());
        URL malformed = exploded("malformed", "web.profile=onprem\nprivate=\\uSECRET\n", index());

        assertProfileInvalid(() -> WebDistribution.read(duplicate),
                "Web metadata is malformed or contains repeated keys");
        assertProfileInvalid(() -> WebDistribution.read(malformed),
                "Web metadata is malformed or contains repeated keys");
    }

    @Test
    void anOversizedManifestIsNotLoadedAsAnOptionalMissingResource() throws Exception {
        URL own = exploded("oversized", "web.profile=onprem\n#" + "x".repeat(64 * 1024), index());

        assertProfileInvalid(() -> WebDistribution.read(own), "Web metadata exceeds its size limit");
    }

    @Test
    void anUnreadableArchiveDoesNotFallBackToServerOnly() throws Exception {
        Path missing = directory.resolve("absent-sentinel-password.jar");

        assertProfileInvalid(() -> WebDistribution.read(jarUrl(missing, BOOT_CLASSES)),
                "application resources are unreadable");
    }

    @Test
    void directoryAndSymlinkMetadataCannotLeaveTheOwnedOrigin() throws Exception {
        URL directoryManifest = exploded("directory-manifest", null, index());
        Path root = directory.resolve("directory-manifest");
        Files.createDirectories(root.resolve(METADATA));
        URL symlinkManifest = exploded("symlink-manifest", null, index());
        Path external = directory.resolve("external.properties");
        Files.writeString(external, cloudMetadata(CONSOLE));
        Files.createDirectories(directory.resolve("symlink-manifest/META-INF"));
        Files.createSymbolicLink(directory.resolve("symlink-manifest").resolve(METADATA), external);

        assertProfileInvalid(() -> WebDistribution.read(directoryManifest),
                "Web metadata is not an owned regular resource");
        assertProfileInvalid(() -> WebDistribution.read(symlinkManifest),
                "Web metadata is not an owned regular resource");
    }

    @Test
    void symbolicLinkAssetsCannotReferenceAParentPayload() throws Exception {
        URL own = exploded("symlink-assets", "web.profile=onprem\n", index());
        Path external = directory.resolve("external.js");
        Files.writeString(external, "code");
        Files.createSymbolicLink(directory.resolve("symlink-assets/static/foreign.js"), external);

        assertProfileInvalid(() -> WebDistribution.read(own), "Web payload is not an owned regular resource");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"http://console.example/", "/relative", "https:///missing-host",
            "https://user:sentinel-password@console.example/", "https://console.example/#fragment",
            "https://console.example/with space", "https://console.example/\nline",
            "https://console.example/\tline", "https://console.example/\u007fline",
            "https://console.example/\u00e9", "https://console.example:0/", "https://console.example:65536/",
            "https://console.example:/", "https://console.example/bad%escape"})
    void cloudMetadataNeedsAnExplicitSafeHttpsUrl(String consoleUrl) throws Exception {
        URL own = exploded("invalid-url", cloudMetadata(consoleUrl), index());

        assertThatThrownBy(() -> WebDistribution.read(own))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(BootError.WEB_CONSOLE_URL_INVALID);
                    assertThat(failure.args()).isEmpty();
                    assertThat(failure.getCause()).isNull();
                    assertThat(MessageCatalog.bundled().render(failure.code(), failure.args()).message())
                            .doesNotContain("sentinel-password", "console.example", "fragment");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "https://console.example/"})
    void onPremMetadataMustOmitTheConsoleUrlEvenWhenBlank(String consoleUrl) throws Exception {
        URL own = exploded("onprem-url", "web.profile=onprem\ncloud.console.url=" + consoleUrl + "\n", index());

        assertThatThrownBy(() -> WebDistribution.read(own))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_CONSOLE_URL_INVALID));
    }

    private URL exploded(String name, String metadata, Map<String, String> assets) throws IOException {
        Path root = directory.resolve(name);
        Path applicationClass = root.resolve(CLASS);
        Files.createDirectories(applicationClass.getParent());
        Files.write(applicationClass, new byte[]{0});
        if (metadata != null) {
            Files.createDirectories(root.resolve("META-INF"));
            Files.writeString(root.resolve(METADATA), metadata, StandardCharsets.ISO_8859_1);
        }
        for (var asset : assets.entrySet()) {
            Path path = root.resolve("static").resolve(asset.getKey());
            Files.createDirectories(path.getParent());
            Files.writeString(path, asset.getValue());
        }
        return root.toUri().toURL();
    }

    private Path archive(String name, String classes, Map<String, String> metadata,
            Map<String, String> assets) throws IOException {
        Map<String, String> entries = new LinkedHashMap<>(metadata);
        entries.put(classes + CLASS, "class fixture");
        assets.forEach((path, value) -> entries.put(classes + "static/" + path, value));
        Path archive = directory.resolve(name);
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(archive))) {
            for (var entry : entries.entrySet()) {
                output.putNextEntry(new JarEntry(entry.getKey()));
                output.write(entry.getValue().getBytes(StandardCharsets.ISO_8859_1));
                output.closeEntry();
            }
        }
        return archive;
    }

    private static URL jarUrl(Path archive, String classes) throws IOException {
        return new URL("jar:" + archive.toUri() + "!/" + classes);
    }

    private static URL locationUrl(String value) throws IOException {
        return new URL(null, value, new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL url) {
                throw new AssertionError("code-source lookup must not call a classpath resource handler");
            }
        });
    }

    private static Class<?> isolatedDistribution(URL ownedSource, URL foreignResource) throws Exception {
        Set<String> knownClasses = Set.of(
                "io.tapstate.app.WebDistribution",
                "io.tapstate.app.WebDistribution$Profile",
                "io.tapstate.app.WebDistribution$UniqueProperties",
                "io.tapstate.app.BootError");
        ProtectionDomain domain = new ProtectionDomain(new CodeSource(ownedSource, (Certificate[]) null), null);
        ClassLoader loader = new ClassLoader(WebDistribution.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (!knownClasses.contains(name)) return super.loadClass(name, resolve);
                synchronized (getClassLoadingLock(name)) {
                    Class<?> found = findLoadedClass(name);
                    if (found == null) {
                        String resource = name.replace('.', '/') + ".class";
                        try (InputStream input = getParent().getResourceAsStream(resource)) {
                            if (input == null) throw new ClassNotFoundException(name);
                            byte[] bytes = input.readAllBytes();
                            found = defineClass(name, bytes, 0, bytes.length, domain);
                        } catch (IOException failure) {
                            throw new ClassNotFoundException(name, failure);
                        }
                    }
                    if (resolve) resolveClass(found);
                    return found;
                }
            }

            @Override
            public URL getResource(String name) {
                return CLASS.equals(name) ? foreignResource : super.getResource(name);
            }
        };
        return Class.forName("io.tapstate.app.WebDistribution", true, loader);
    }

    private static String cloudMetadata(String consoleUrl) {
        return "web.profile=cloud\n" + (consoleUrl == null ? "" : "cloud.console.url="
                + consoleUrl.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t") + "\n");
    }

    private static Map<String, String> index() {
        return Map.of("index.html", "<!doctype html><title>Local fixture</title>");
    }

    private static CloudRuntimeSettings cloud() {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example");
        properties.setToken("sentinel-token");
        properties.setAtlasUri("mongodb://user:sentinel-password@atlas.example/metadata");
        properties.setClusterId("fixture-cluster");
        return CloudRuntimeSettings.resolve(properties);
    }

    private static void assertProfileInvalid(CheckedAction action, String reason) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(BootError.WEB_PROFILE_INVALID);
            assertThat(failure.args()).containsExactlyEntriesOf(Map.of("reason", reason));
            assertThat(failure.getCause()).isNull();
            assertThat(MessageCatalog.bundled().render(failure.code(), failure.args()).message())
                    .doesNotContain("sentinel-password", "SECRET");
        });
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws Exception;
    }
}
