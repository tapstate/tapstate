package io.tapstate.testsupport;

import java.util.Map;
import java.util.Set;

/** Prevents the invoking shell's JVM options from changing a child-process test scenario. */
public final class JvmTestEnvironment {
    private static final Set<String> INJECTION_VARIABLES =
            Set.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS");

    private JvmTestEnvironment() {}

    public static void configure(ProcessBuilder builder, Map<String, String> scenario) {
        builder.environment().keySet().removeAll(INJECTION_VARIABLES);
        builder.environment().putAll(scenario);
    }
}
