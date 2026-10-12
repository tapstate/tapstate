package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelinePreviewService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.runtime.probe.PipelinePreviewEvent;
import io.tapstate.runtime.probe.PipelinePreviewStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Actual MVC async ownership over the real candidate compiler and a controlled finite probe. */
class PipelinePreviewOwnershipMvcTest {

    @Test
    void completionKeepsTheSessionOpenUntilWritingFinishesAndClosesItOnce() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            MvcResult result = fixture.perform();
            assertThat(fixture.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.cancelCalls).hasValue(0);

            fixture.release.countDown();
            fixture.mvc.perform(asyncDispatch(result)).andExpect(status().isOk());
            assertThat(result.getResponse().getContentAsString().lines())
                    .hasSize(3).anyMatch(line -> line.contains("run.completed"));
            assertThat(fixture.cancelCalls).hasValue(1);
            result.getRequest().getAsyncContext().complete();
            assertThat(fixture.cancelCalls).hasValue(1);
        }
    }

    @Test
    void aWriteDisconnectCancelsAndClosesTheSessionWithoutAnExtraCompletionClose() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            MvcResult result = fixture.perform();
            assertThat(result.getAsyncResult(5_000)).isInstanceOf(IOException.class);
            assertThat(fixture.cancelCalls).hasValue(2);
            result.getRequest().getAsyncContext().complete();
            assertThat(fixture.cancelCalls).hasValue(2);
        }
    }

    @Test
    void rejectedAsyncSchedulingClosesTheSessionEvenWhenTheResponseBodyNeverRuns() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            AtomicInteger rejections = new AtomicInteger();
            AsyncTaskExecutor rejected = task -> {
                rejections.incrementAndGet();
                throw new RejectedExecutionException("owned test executor rejects the streaming body");
            };
            fixture.adapter().setTaskExecutor(rejected);

            MvcResult result = fixture.perform();
            assertThat(rejections).hasValue(1);
            assertThat(fixture.opens).hasValue(1);
            assertThat(fixture.entered.getCount()).isEqualTo(1);
            result.getRequest().getAsyncContext().complete();
            assertThat(fixture.cancelCalls).hasValue(1);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("preview-ownership-test-");
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger cancelCalls = new AtomicInteger();
        private final MockMvc mvc;
        private final String body;

        private Fixture(boolean disconnect) throws IOException {
            Clock clock = Clock.systemUTC();
            var apply = new PipelineApiTest.TestApp().applyService(new PipelineApiTest.FakeArtifactStore(),
                    new AuditGate(new PipelineApiTest.RecordingAuditStore(), clock));
            PipelinePreviewService previews = new PipelinePreviewService(apply, coordinates -> {
                opens.incrementAndGet();
                return new PipelinePreviewStream() {
                    private boolean completed;

                    @Override
                    public PipelinePreviewEvent next() throws InterruptedException {
                        entered.countDown();
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("owned preview probe was not released");
                        }
                        if (completed) return null;
                        completed = true;
                        return new PipelinePreviewEvent(coordinates.runId(), coordinates.candidateHash(), 0,
                                "run.completed", clock.instant(), Map.of("rowCount", 0));
                    }

                    @Override
                    public void cancel() {
                        cancelCalls.incrementAndGet();
                        release.countDown();
                    }
                };
            }, clock);
            ObjectMapper json = new ObjectMapper();
            var builder = MockMvcBuilders.standaloneSetup(new PipelinePreviewController(previews, json));
            if (disconnect) {
                builder.addFilters((request, response, chain) -> chain.doFilter(request,
                        new HttpServletResponseWrapper((jakarta.servlet.http.HttpServletResponse) response) {
                            @Override
                            public ServletOutputStream getOutputStream() {
                                return new ServletOutputStream() {
                                    @Override
                                    public boolean isReady() { return true; }

                                    @Override
                                    public void setWriteListener(WriteListener listener) {
                                        throw new UnsupportedOperationException("owned synchronous failure stream");
                                    }

                                    @Override
                                    public void write(int value) throws IOException {
                                        throw new IOException("owned response disconnect");
                                    }

                                    @Override
                                    public void write(byte[] bytes, int offset, int length) throws IOException {
                                        throw new IOException("owned response disconnect");
                                    }
                                };
                            }
                        }));
            }
            mvc = builder.build();
            executor.setDaemon(true);
            executor.setCancelRemainingTasksOnClose(true);
            executor.setTaskTerminationTimeout(1_000);
            adapter().setTaskExecutor(executor);
            body = json.writeValueAsString(Map.of("pipelineId", "preview",
                    "drafts", List.of(Map.of("content", SOURCE), Map.of("content", TARGET),
                            Map.of("content", PIPELINE))));
            var principal = TapstatePrincipal.humanJwt(new VerifiedToken("preview-author", Scope.READ));
            SecurityContextHolder.getContext().setAuthentication(
                    UsernamePasswordAuthenticationToken.authenticated(principal, "unused", List.of()));
        }

        private RequestMappingHandlerAdapter adapter() {
            return mvc.getDispatcherServlet().getWebApplicationContext()
                    .getBean(RequestMappingHandlerAdapter.class);
        }

        private MvcResult perform() throws Exception {
            return mvc.perform(post("/artifacts:preview").contentType("application/json").content(body))
                    .andExpect(request().asyncStarted()).andReturn();
        }

        @Override
        public void close() {
            release.countDown();
            executor.close();
            SecurityContextHolder.clearContext();
        }
    }

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: input
            connector: mysql
            config: { host: preview-source.invalid, port: 3306, database: preview, username: fixture, password: fixture }
            mode: cdc
            """;
    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: target
            connector: mongodb
            config: { isUri: true, uri: "mongodb://preview-target.invalid/preview" }
            """;
    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: preview
            source: input
            serve:
              from: /.*/
              sync:
                - id: output
                  source: target
                  write_mode: upsert
            """;
}
