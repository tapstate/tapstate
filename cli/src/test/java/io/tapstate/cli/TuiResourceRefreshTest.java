package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
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
