package io.tapstate.mcp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Isolated("Owns process standard input and logging properties")
class TapstateMcpApplicationTest {

    private InputStream processInput;

    @BeforeEach
    void isolateStdioInput() {
        processInput = System.in;
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
        InputStream previous = System.in;
        OwnedEofInput input = new OwnedEofInput();
        System.setIn(input);
        try (ConfigurableApplicationContext context = TapstateMcpApplication.start(
                new String[] {"--server", "http://127.0.0.1:1", "--allow-write"},
                Map.of("TAPSTATE_TOKEN", "machine-token"))) {
            assertThat(context.isRunning()).isTrue();
            assertThat(context.getBean(McpOptions.class).allowWrite()).isTrue();
            assertThat(context.getBean(McpOperationExecutor.class)).isNotNull();
            // The whole write surface: twenty-four reads plus fifteen writes. Pinned as a count here and by
            // name in McpToolCatalogTest, so a tool that appears by accident fails one of the two.
            assertThat((List<?>) context.getBean("mcpTools")).hasSize(39);
            // The transport must consume only owned EOF,
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
