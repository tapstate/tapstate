package io.tapstate.control.restapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

/** The product version from the build-filtered resource, shared by HTTP and runtime reporting. */
public final class RuntimeVersion {

    private static final String VERSION = versionIn(
            RuntimeVersion.class.getResourceAsStream("/tapstate-version.properties"));

    private RuntimeVersion() { }

    public static String current() {
        return VERSION;
    }

    /** Missing or unfiltered resources are build defects, not operator configuration errors. */
    static String versionIn(InputStream properties) {
        if (properties == null) {
            throw new IllegalStateException("tapstate-version.properties is not on the classpath");
        }
        try (properties) {
            Properties parsed = new Properties();
            parsed.load(properties);
            String version = parsed.getProperty("version");
            if (version == null || version.isBlank() || version.startsWith("${")) {
                throw new IllegalStateException(
                        "tapstate-version.properties carries no substituted version: " + version);
            }
            return version;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
