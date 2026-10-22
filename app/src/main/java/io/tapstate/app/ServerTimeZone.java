package io.tapstate.app;

import java.lang.management.ManagementFactory;
import java.util.TimeZone;
import org.slf4j.Logger;

/** Selects the server process timezone before logging, framework and connector initialization. */
record ServerTimeZone(String id, String source, String requested) {
    private static final String PROPERTY = "user.timezone";
    private static final String OPTION = "-D" + PROPERTY;

    static ServerTimeZone initialize() {
        String configured = null;
        // getDefault() can populate user.timezone from the host before this class is loaded.
        // The launcher arguments distinguish an explicit choice from that cached platform default,
        // including options supplied through JAVA_TOOL_OPTIONS and JDK_JAVA_OPTIONS.
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (argument.equals(OPTION)) {
                configured = "";
            } else if (argument.startsWith(OPTION + "=")) {
                configured = argument.substring(OPTION.length() + 1);
            }
        }
        boolean explicit = configured != null && !configured.isBlank();
        System.setProperty(PROPERTY, explicit ? configured : "UTC");
        // Resolve through the JDK after invalidating its cache, retaining its timezone-ID semantics.
        // Setting the property alone would leave an earlier getDefault() result in force.
        TimeZone.setDefault(null);
        return new ServerTimeZone(TimeZone.getDefault().getID(), explicit ? "JVM option" : "default",
                explicit ? configured : "UTC");
    }

    void log(Logger logger) {
        if (logger.isInfoEnabled()) {
            if (requested.equals(id)) {
                logger.info("Server time zone: {} ({})", id, source);
            } else {
                // The JDK can normalize a valid custom offset or fall back from an unknown ID.
                // Show both values without mislabelling a valid normalization as an invalid option.
                logger.info("Server time zone: {} ({}); requested=\"{}\", resolved differently by the JDK",
                        id, source, requested.replace("\r", "\\r").replace("\n", "\\n"));
            }
        }
    }
}
