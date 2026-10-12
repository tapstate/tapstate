package io.tapstate.mcp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        InputStream previous = System.in;
        OwnedEofInput input = new OwnedEofInput();
        System.setIn(input);
        try (ConfigurableApplicationContext context = TapstateMcpApplication.start(
                new String[] {"--server", "http://127.0.0.1:1", "--allow-write"},
                Map.of("TAPSTATE_TOKEN", "machine-token"))) {
            assertThat(context.isRunning()).isTrue();
            assertThat(context.getBean(McpOptions.class).allowWrite()).isTrue();
            assertThat(context.getBean(McpOperationExecutor.class)).isNotNull();
            // The whole write surface: nineteen reads plus eight writes. Pinned as a count here and by
            // name in McpToolCatalogTest, so a tool that appears by accident fails one of the two.
            assertThat((List<?>) context.getBean("mcpTools")).hasSize(27);
            // The default transport also starts when stdio is false. It must consume only owned EOF,
            // never the test runner's command channel. The separate process test drives real stdio.
            assertThat(input.awaitEof()).isTrue();
        } finally {
            System.setIn(previous);
        }
    }

    @Test
    void applicationContextLeavesTheTestRunnerInputUnreadAndRestoresIt() {
        InputStream previous = System.in;
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        InputStream runnerInput = new InputStream() {
            @Override
            public int read() {
                reads.incrementAndGet();
                return -1;
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
        System.setIn(runnerInput);
        try {
            startsAsAStdioOnlyApplicationAndRegistersTheConfiguredToolSurface();
            assertThat(System.in).isSameAs(runnerInput);
            assertThat(reads.get()).isZero();
            assertThat(closes.get()).isZero();
        } finally {
            System.setIn(previous);
        }
    }

    private static final class OwnedEofInput extends InputStream {
        private final CountDownLatch eofObserved = new CountDownLatch(1);

        boolean awaitEof() {
            try {
                return eofObserved.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        }

        @Override
        public int read() {
            eofObserved.countDown();
            return -1;
        }
    }
}
