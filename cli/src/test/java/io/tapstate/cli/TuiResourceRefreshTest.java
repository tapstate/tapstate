package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TuiResourceRefreshTest {

    @Test
    void projectsRemoteArtifactsAndPipelineStatusIntoAnImmutableCompletionEvent() {
        ControlPlaneClient client = client(
                new ListOutcome.Listed(List.of(
                        new RemoteArtifact("orders", "pipeline",
                                "{\"password\":\"hunter2\"}"),
                        new RemoteArtifact("customers", "source", "secret=tok_live"))),
                new StatusOutcome.Found("orders", "RUNNING"));
        AtomicReference<TuiEvent> posted = new AtomicReference<>();

        new TuiResourceRefresh(client, Runnable::run).refresh(7L, 3L,
                URI.create("https://control.example"), "bearer-secret", posted::set);

        TuiEvent.ResourceRefreshCompleted event = (TuiEvent.ResourceRefreshCompleted) posted.get();
        assertThat(event.result().requestId()).isEqualTo(7L);
        assertThat(event.result().contextGeneration()).isEqualTo(3L);
        assertThat(event.result().resources()).extracting(TuiDashboard.ResourceSummary::id)
                .containsExactly("orders", "customers");
        assertThat(event.result().pipelines()).extracting(TuiDashboard.PipelineSummary::state)
                .containsExactly("RUNNING");
        assertThat(event.result().notice()).contains("refreshed");
        assertThat(event.result().toString()).doesNotContain("hunter2", "tok_live", "bearer-secret");
    }

    @Test
    void turnsRejectedAndUnreachableReadsIntoStableNoticesWithoutThrowing() {
        AtomicReference<TuiEvent> rejected = new AtomicReference<>();
        new TuiResourceRefresh(client(new ListOutcome.Rejected("control.forbidden", "do not render here"),
                        new StatusOutcome.Unreachable()), Runnable::run)
                .refresh(1L, 0L, URI.create("https://control.example"), "secret", rejected::set);

        TuiResourceRefreshResult result = ((TuiEvent.ResourceRefreshCompleted) rejected.get()).result();
        assertThat(result.notice()).contains("control.forbidden");
        assertThat(result.notice()).doesNotContain("do not render here");
        assertThat(result.notice()).doesNotContain("secret");
    }

    @Test
    void cancelInterruptsRefreshWorkerForAnOldContext() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        ControlPlaneClient client = (ControlPlaneClient) Proxy.newProxyInstance(
                ControlPlaneClient.class.getClassLoader(), new Class<?>[]{ControlPlaneClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("list")) {
                        entered.countDown();
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException failure) {
                            interrupted.countDown();
                            throw failure;
                        }
                        return new ListOutcome.Unreachable();
                    }
                    if (method.getName().equals("toString")) {
                        return "refresh-test-client";
                    }
                    return null;
                });
        AtomicReference<Thread> worker = new AtomicReference<>();
        TuiResourceRefresh refresh = new TuiResourceRefresh(client, command -> {
            Thread thread = Thread.ofVirtual().unstarted(command);
            worker.set(thread);
            thread.start();
        });

        refresh.refresh(7, 2, URI.create("http://127.0.0.1:8081"), "credential", ignored -> { });

        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        refresh.cancel(7);
        assertThat(interrupted.await(2, TimeUnit.SECONDS)).isTrue();
        Thread refreshWorker = worker.get();
        refreshWorker.join(2_000);
        assertThat(refreshWorker.isAlive()).isFalse();
    }

    private static ControlPlaneClient client(ListOutcome list, StatusOutcome status) {
        return (ControlPlaneClient) Proxy.newProxyInstance(
                ControlPlaneClient.class.getClassLoader(), new Class<?>[]{ControlPlaneClient.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "list" -> list;
                    case "status" -> status;
                    case "toString" -> "test-control-plane";
                    case "close" -> null;
                    default -> throw new AssertionError("unexpected control-plane call: " + method.getName());
                });
    }
}
