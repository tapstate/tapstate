package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hazelcast.internal.util.ModularJavaUtils;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The shipped boot jar declares every package the clustering library checks it can reach.
 *
 * <p>The library reaches into a handful of JDK internals for its own memory handling and for the
 * management beans it reports through. Denied them it starts anyway, says so once at startup, and falls
 * back - and what it gives up is throughput on the paths between members, which is nothing at all on one
 * machine and a hot path the moment there are two. So the deployment that would notice is the one where
 * nobody is watching a single member's log.
 *
 * <p><b>The list is taken from the library rather than written down here.</b> It is the library's own
 * check that decides whether it warns, so a version that starts needing one more package would leave a
 * copied list passing while the shipped artifact quietly lost the access again. Asking it directly means
 * that upgrade fails here instead.
 *
 * <p>Manifest rather than a launcher flag, because the artifact operators run is this jar and {@code java
 * -jar} reads these attributes before the application starts. A fork that carries the flags on its own
 * command line proves nothing about the jar - measured on this one: launched so the manifest is bypassed,
 * the same jar starts identically and prints the warning.
 *
 * <p>The library's message also names a {@code java.se} module flag, which this deliberately does not
 * carry: that part of the message is fixed text rather than something the check looks at, no manifest
 * attribute expresses it, and a class-path application already resolves that module. Hence the parse
 * below reads only the package-level arguments.
 */
class BootJarDeclaresWhatTheClusterLibraryChecksForIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.app.boot-jar";

    /** `--add-opens java.base/sun.nio.ch=ALL-UNNAMED` and its export twin, as the library writes them. */
    private static final Pattern PACKAGE_ARGUMENT =
            Pattern.compile("--add-(?:opens|exports) (\\S+?/\\S+?)=");

    @Test
    void theBootJarManifestCoversEveryPackageTheLibraryAsksFor() throws Exception {
        String bootJar = System.getProperty(BOOT_JAR_PROPERTY);
        assumeTrue(bootJar != null && Files.isRegularFile(Path.of(bootJar)),
                "no packaged boot jar - not a packaging build, skipping");

        Set<String> askedFor = whatTheLibraryChecksFor();
        assertThat(askedFor)
                .describedAs("the library was asked and answered with a list. An empty one would mean this "
                        + "fork already has the access - which would make every assertion below pass "
                        + "without reading anything the jar declares")
                .isNotEmpty();

        Set<String> declared = declaredIn(bootJar);
        assertThat(declared)
                .describedAs("%s declares the packages the library checks for. Missing one costs no "
                        + "correctness and no error - only the warning at startup and the throughput "
                        + "between members", bootJar)
                .containsAll(askedFor);
    }

    /** Every module/package the library names when it finds it cannot reach them. */
    private static Set<String> whatTheLibraryChecksFor() {
        CapturedWarnings warnings = new CapturedWarnings();
        ModularJavaUtils.checkJavaInternalAccess(warnings);

        Set<String> packages = new LinkedHashSet<>();
        for (String warning : warnings.messages) {
            Matcher named = PACKAGE_ARGUMENT.matcher(warning);
            while (named.find()) {
                packages.add(named.group(1));
            }
        }
        return packages;
    }

    /** Every module/package the jar's manifest opens or exports, of either kind. */
    private static Set<String> declaredIn(String bootJar) throws Exception {
        try (JarFile jar = new JarFile(bootJar)) {
            Manifest manifest = jar.getManifest();
            assertThat(manifest).describedAs("%s has a manifest at all", bootJar).isNotNull();
            Set<String> declared = new LinkedHashSet<>();
            for (String attribute : List.of("Add-Opens", "Add-Exports")) {
                String value = manifest.getMainAttributes().getValue(attribute);
                if (value != null) {
                    declared.addAll(List.of(value.trim().split("\\s+")));
                }
            }
            return declared;
        }
    }

    /** A logger that keeps what it was told rather than printing it. */
    private static final class CapturedWarnings extends AbstractLogger {

        private final List<String> messages = new java.util.ArrayList<>();

        @Override
        public void log(Level level, String message) {
            messages.add(message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            messages.add(message);
        }

        @Override
        public void log(LogEvent logEvent) {
            messages.add(logEvent.getLogRecord().getMessage());
        }

        @Override
        public Level getLevel() {
            return Level.ALL;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }
    }
}
