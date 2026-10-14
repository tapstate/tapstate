package io.tapstate.mcp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Isolated("Owns process standard input and logging properties")
class TapstateMcpApplicationTest {

    private final InputStream processInput = System.in;

    @BeforeEach
    void isolateStdioInput() {
        // The default stdio transport still starts when stdio=false. It must not read
        // Surefire's command pipe or prevent the test JVM from exiting and writing coverage.
        System.setIn(InputStream.nullInputStream());
    }

    @AfterEach
    void clearProcessProperties() {
        System.setIn(processInput);
        System.clearProperty(TapstateMcpApplication.AOT_PROCESSING_PROPERTY);
        System.clearProperty(TapstateMcpApplication.LOGBACK_STATUS_LISTENER_PROPERTY);
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
        try (ConfigurableApplicationContext context = TapstateMcpApplication.start(
                new String[] {"--server", "http://127.0.0.1:1", "--allow-write"},
                Map.of("TAPSTATE_TOKEN", "machine-token"))) {
            assertThat(context.isRunning()).isTrue();
            assertThat(context.getBean(McpOptions.class).allowWrite()).isTrue();
            assertThat(context.getBean(McpOperationExecutor.class)).isNotNull();
            // The whole write surface: nineteen reads plus eight writes. Pinned as a count here and by
            // name in McpToolCatalogTest, so a tool that appears by accident fails one of the two.
            assertThat((List<?>) context.getBean("mcpTools")).hasSize(27);
        }
    }
}
