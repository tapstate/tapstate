package io.tapstate.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/** Changes only test Web resources, retaining the real executable classes and nested dependencies. */
final class BootJarWebFixture {

    private static final String CLASSES = "BOOT-INF/classes/";
    private static final String METADATA = "META-INF/tapstate-web.properties";
    private static final String FILES = "META-INF/tapstate-web.files.sha256";

    private BootJarWebFixture() {
    }

    static Path create(Path original, Path destination, String profile) throws IOException {
        try (JarFile input = new JarFile(original.toFile());
                JarOutputStream output = new JarOutputStream(Files.newOutputStream(
                        destination, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            if (input.getJarEntry(CLASSES + "io/tapstate/app/Bootstrap.class") == null) {
                throw new AssertionError("The startup fixture requires a packaged executable Boot JAR");
            }
            Enumeration<JarEntry> entries = input.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (webResource(entry.getName())) continue;
                JarEntry copied = new JarEntry(entry);
                // Nested dependencies must remain STORED; recompressed entries get a fresh size.
                if (copied.getMethod() == ZipEntry.DEFLATED) copied.setCompressedSize(-1);
                output.putNextEntry(copied);
                if (!entry.isDirectory()) {
                    try (var bytes = input.getInputStream(entry)) {
                        bytes.transferTo(output);
                    }
                }
                output.closeEntry();
            }
            write(output, CLASSES + "static/index.html",
                    "<!doctype html><html><body>startup-profile-fixture</body></html>\n");
            if (profile != null) {
                write(output, CLASSES + METADATA, "web.profile=" + profile + "\n"
                        + ("cloud".equals(profile) ? "cloud.console.url=https://console.example.test/\n" : ""));
            }
        }
        return destination;
    }

    private static boolean webResource(String name) {
        return name.startsWith(CLASSES + "static/") || name.equals(METADATA)
                || name.equals(CLASSES + METADATA) || name.equals(FILES) || name.equals(CLASSES + FILES);
    }

    private static void write(JarOutputStream output, String name, String value) throws IOException {
        output.putNextEntry(new JarEntry(name));
        output.write(value.getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
    }
}
