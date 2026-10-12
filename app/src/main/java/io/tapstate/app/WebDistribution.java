package io.tapstate.app;

import io.tapstate.core.common.TapstateException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Reads immutable Web deployment metadata only from the archive or directory carrying this class. */
record WebDistribution(Profile profile, String consoleUrl) {

    private static final String CLASS_RESOURCE = "io/tapstate/app/WebDistribution.class";
    private static final String METADATA_RESOURCE = "META-INF/tapstate-web.properties";
    private static final String BOOT_CLASSES = "BOOT-INF/classes/";
    private static final int MAX_METADATA_BYTES = 64 * 1024;

    enum Profile {
        SERVER_ONLY("server-only"),
        CLOUD("cloud"),
        ON_PREM("onprem");

        private final String value;

        Profile(String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }

    static WebDistribution bundled() {
        var source = WebDistribution.class.getProtectionDomain().getCodeSource();
        return read(source == null ? null : source.getLocation());
    }

    static WebDistribution read(URL applicationCodeSource) {
        if (applicationCodeSource == null) {
            throw invalid("application resource origin is unavailable");
        }
        try {
            String external = applicationCodeSource.toExternalForm();
            if ("file".equals(applicationCodeSource.getProtocol())) {
                Path source = Path.of(applicationCodeSource.toURI()).toRealPath();
                if (Files.isDirectory(source)) return readDirectory(source);
                return readArchive(source, archiveClasses(source));
            }
            if (external.startsWith("jar:file:")) {
                int separator = external.indexOf("!/");
                if (separator < 0) throw invalid("application resource origin is unsupported");
                String classes = external.substring(separator + 2);
                if (!(classes.isEmpty() || BOOT_CLASSES.equals(classes))) {
                    throw invalid("application resource origin is unsupported");
                }
                return readArchive(Path.of(new URI(external.substring(4, separator))), classes);
            }
            String prefix = external.startsWith("jar:nested:") ? "jar:nested:"
                    : external.startsWith("nested:") ? "nested:" : null;
            if (prefix != null) {
                int separator = external.indexOf("/!", prefix.length());
                if (separator < 0) throw invalid("application resource origin is unsupported");
                String entry = external.substring(separator + 2);
                if (!(BOOT_CLASSES.equals(entry) || (BOOT_CLASSES + "!/").equals(entry))) {
                    throw invalid("application resource origin is unsupported");
                }
                String path = external.substring(prefix.length(), separator);
                return readArchive(Path.of(new URI("file:" + path)), BOOT_CLASSES);
            }
            throw invalid("application resource origin is unsupported");
        } catch (IOException | UncheckedIOException | URISyntaxException failure) {
            throw invalid("application resources are unreadable");
        }
    }

    private static String archiveClasses(Path archive) throws IOException {
        try (JarFile jar = new JarFile(archive.toFile())) {
            boolean plain = jar.getJarEntry(CLASS_RESOURCE) != null;
            boolean boot = jar.getJarEntry(BOOT_CLASSES + CLASS_RESOURCE) != null;
            if (plain == boot) throw invalid("application class origin is absent or ambiguous");
            return plain ? "" : BOOT_CLASSES;
        }
    }

    private static WebDistribution readDirectory(Path root) throws IOException {
        List<Properties> metadata = new ArrayList<>();
        Path manifest = root.resolve(METADATA_RESOURCE);
        BasicFileAttributes manifestAttributes = attributes(manifest);
        if (manifestAttributes != null) {
            if (!manifestAttributes.isRegularFile() || !manifest.toRealPath().startsWith(root)) {
                throw invalid("Web metadata is not an owned regular resource");
            }
            try (InputStream input = Files.newInputStream(manifest)) {
                metadata.add(parse(input));
            }
        }

        boolean payload = false;
        boolean index = false;
        Path staticRoot = root.resolve("static");
        BasicFileAttributes staticAttributes = attributes(staticRoot);
        if (staticAttributes != null) {
            if (!staticAttributes.isDirectory() || !staticRoot.toRealPath().startsWith(root)) {
                throw invalid("Web payload is not an owned regular resource");
            }
            try (var paths = Files.walk(staticRoot)) {
                for (Path path : paths.toList()) {
                    BasicFileAttributes found = Files.readAttributes(
                            path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (found.isSymbolicLink()) {
                        throw invalid("Web payload is not an owned regular resource");
                    }
                    if (found.isRegularFile()) {
                        payload = true;
                        if (path.equals(staticRoot.resolve("index.html")) && found.size() > 0) index = true;
                    }
                }
            }
        }
        return resolve(metadata, payload, index);
    }

    private static BasicFileAttributes attributes(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException absent) {
            return null;
        }
    }

    private static WebDistribution readArchive(Path archive, String classes) throws IOException {
        List<Properties> metadata = new ArrayList<>();
        boolean payload = false;
        boolean index = false;
        boolean rootMetadata = false;
        boolean classesMetadata = false;
        try (JarFile jar = new JarFile(archive.toFile())) {
            if (jar.getJarEntry(classes + CLASS_RESOURCE) == null) {
                throw invalid("application class is absent from its resource origin");
            }
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (METADATA_RESOURCE.equals(name) || (BOOT_CLASSES + METADATA_RESOURCE).equals(name)) {
                    boolean root = METADATA_RESOURCE.equals(name);
                    if (entry.isDirectory() || (root ? rootMetadata : classesMetadata)) {
                        throw invalid("Web metadata is repeated or is not a regular resource");
                    }
                    if (root) rootMetadata = true;
                    else classesMetadata = true;
                    try (InputStream input = jar.getInputStream(entry)) {
                        metadata.add(parse(input));
                    }
                }
                if (!entry.isDirectory() && name.startsWith(classes + "static/")) {
                    payload = true;
                    if ((classes + "static/index.html").equals(name)) {
                        try (InputStream input = jar.getInputStream(entry)) {
                            index = input.read() >= 0;
                        }
                    }
                }
            }
        }
        return resolve(metadata, payload, index);
    }

    private static Properties parse(InputStream input) throws IOException {
        byte[] bytes = input.readNBytes(MAX_METADATA_BYTES + 1);
        if (bytes.length > MAX_METADATA_BYTES) throw invalid("Web metadata exceeds its size limit");
        Properties properties = new UniqueProperties();
        try {
            properties.load(new ByteArrayInputStream(bytes));
            return properties;
        } catch (IllegalArgumentException invalidProperties) {
            throw invalid("Web metadata is malformed or contains repeated keys");
        }
    }

    private static WebDistribution resolve(List<Properties> copies, boolean payload, boolean index) {
        if (copies.isEmpty()) {
            if (payload) throw invalid("Web metadata is missing");
            return new WebDistribution(Profile.SERVER_ONLY, null);
        }
        Properties metadata = copies.getFirst();
        if (copies.stream().anyMatch(copy -> !copy.equals(metadata))) {
            throw invalid("Web metadata copies conflict");
        }
        if (!index) throw invalid("Web index payload is missing or empty");
        String profile = metadata.getProperty("web.profile");
        if ("cloud".equals(profile)) {
            String consoleUrl = metadata.getProperty("cloud.console.url");
            validateConsoleUrl(consoleUrl);
            return new WebDistribution(Profile.CLOUD, consoleUrl);
        }
        if ("onprem".equals(profile)) {
            if (metadata.containsKey("cloud.console.url")) throw invalidConsoleUrl();
            return new WebDistribution(Profile.ON_PREM, null);
        }
        throw invalid("Web profile is missing or unsupported");
    }

    private static void validateConsoleUrl(String value) {
        if (value == null || value.isEmpty()
                || value.chars().anyMatch(character -> character <= 0x20 || character >= 0x7f)) {
            throw invalidConsoleUrl();
        }
        try {
            URI uri = new URI(value);
            if (!"https".equals(uri.getScheme()) || uri.getHost() == null || uri.getHost().isBlank()
                    || uri.getRawUserInfo() != null || uri.getRawFragment() != null || uri.isOpaque()
                    || uri.getRawAuthority().endsWith(":") || uri.getPort() == 0 || uri.getPort() > 65535) {
                throw invalidConsoleUrl();
            }
        } catch (URISyntaxException invalidUri) {
            throw invalidConsoleUrl();
        }
    }

    void validate(CloudRuntimeSettings settings) {
        if (profile == Profile.SERVER_ONLY) return;
        if ((profile == Profile.CLOUD) != settings.cloud()) {
            throw new TapstateException(BootError.WEB_PROFILE_MODE_MISMATCH, Map.of(
                    "profile", profile.value(), "mode", settings.cloud() ? "cloud" : "onprem"), null);
        }
    }

    private static TapstateException invalid(String reason) {
        return new TapstateException(BootError.WEB_PROFILE_INVALID, Map.of("reason", reason), null);
    }

    private static TapstateException invalidConsoleUrl() {
        return new TapstateException(BootError.WEB_CONSOLE_URL_INVALID, Map.of(), null);
    }

    private static final class UniqueProperties extends Properties {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Object put(Object key, Object value) {
            if (containsKey(key)) throw new IllegalArgumentException("duplicate metadata key");
            return super.put(key, value);
        }
    }
}
