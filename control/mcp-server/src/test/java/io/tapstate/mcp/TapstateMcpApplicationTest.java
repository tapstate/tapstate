package io.tapstate.mcp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TapstateMcpApplicationTest {

    private static final String STDIO_PROPERTY = "spring.ai.mcp.server.stdio";

    @AfterEach
    void clearProcessProperties() {
        System.clearProperty(TapstateMcpApplication.AOT_PROCESSING_PROPERTY);
        System.clearProperty(TapstateMcpApplication.LOGBACK_STATUS_LISTENER_PROPERTY);
        System.clearProperty(STDIO_PROPERTY);
    }

    @Test
    void stdioLoggingSuppressesLogbackStatusFrames() {
        TapstateMcpApplication.prepareStdioLogging();

        assertThat(System.getProperty(TapstateMcpApplication.LOGBACK_STATUS_LISTENER_PROPERTY))
                .isEqualTo("ch.qos.logback.core.status.NopStatusListener");
    }

    @Test
    void aotProcessingDoesNotRequireADeployTimeToken() {
        System.setProperty(TapstateMcpApplication.AOT_PROCESSING_PROPERTY, "true");

        assertThat(TapstateMcpApplication.options(new String[0], Map.of()).token())
                .isEqualTo("tapstate-aot-context-only");
    }

    @Test
    void startsAsAStdioOnlyApplicationAndRegistersTheConfiguredToolSurface() {
        System.setProperty(STDIO_PROPERTY, "false");
        try (ConfigurableApplicationContext context = TapstateMcpApplication.start(
                new String[] {"--server", "http://127.0.0.1:1", "--allow-write"},
                Map.of("TAPSTATE_TOKEN", "machine-token"))) {
            assertThat(context.isRunning()).isTrue();
            assertThat(context.getBean(McpOptions.class).allowWrite()).isTrue();
            assertThat(context.getBean(McpOperationExecutor.class)).isNotNull();
            // The whole write surface: fourteen reads plus eight writes. Pinned as a count here and by
            // name in McpToolCatalogTest, so a tool that appears by accident fails one of the two.
            assertThat((List<?>) context.getBean("mcpTools")).hasSize(22);
        }
    }
}
