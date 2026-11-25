package io.tapstate.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JvmTestEnvironmentTest {
    @Test
    void removesInheritedInjectionBeforeApplyingTheScenarioAndRetainsOtherEnvironment() {
        ProcessBuilder builder = new ProcessBuilder("java");
        builder.environment().putAll(Map.of("JAVA_TOOL_OPTIONS", "inherited-tool",
                "JDK_JAVA_OPTIONS", "inherited-launcher", "_JAVA_OPTIONS", "inherited-legacy", "TZ", "Etc/UTC"));
        JvmTestEnvironment.configure(builder, Map.of("JAVA_TOOL_OPTIONS", "-Duser.timezone=Asia/Tokyo"));
        assertThat(builder.environment()).containsEntry("JAVA_TOOL_OPTIONS", "-Duser.timezone=Asia/Tokyo")
                .containsEntry("TZ", "Etc/UTC").doesNotContainKeys("JDK_JAVA_OPTIONS", "_JAVA_OPTIONS");
    }
}
