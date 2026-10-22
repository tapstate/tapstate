package io.tapstate.app;

import java.lang.management.ManagementFactory;
import java.util.TimeZone;

/** Selects the server process timezone before logging, framework and connector initialization. */
record ServerTimeZone(String id, String source) {
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
        return new ServerTimeZone(TimeZone.getDefault().getID(), explicit ? "JVM option" : "default");
    }
}
