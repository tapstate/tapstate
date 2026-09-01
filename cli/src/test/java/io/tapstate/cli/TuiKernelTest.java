package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class TuiKernelTest {

    @Test
    void reducesPostedWorkerEventsOnlyWhenTheUiThreadDrainsThem() {
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("boot"));

        kernel.post(new TuiEvent.ActionPosted(new TuiAction.SetNotice("connected")));

        assertThat(kernel.state().notice()).isEqualTo("boot");
        assertThat(kernel.drain()).isTrue();
        assertThat(kernel.state().notice()).isEqualTo("connected");
    }

    @Test
    void routesTimerAndDecodedNavigationThroughActions() {
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("ready"));
        kernel.dispatch(new TuiEvent.ActionPosted(new TuiAction.OpenPalette(List.of("ls", "pwd"), "commands")));

        kernel.dispatch(new TuiEvent.ActionPosted(new TuiAction.MovePalette(1)));
        kernel.dispatch(new TuiEvent.Tick());

        assertThat(kernel.state().paletteIndex()).isEqualTo(1);
        assertThat(kernel.state().ticks()).isEqualTo(1);
    }

    @Test
    void mapsInputAndResizeEventsToPureActions() {
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("ready"));

        kernel.dispatch(new TuiEvent.Key('l'));
        kernel.dispatch(new TuiEvent.Key('s'));
        kernel.dispatch(new TuiEvent.Resize(120, 30));

        assertThat(kernel.state().command()).isEqualTo("ls");
        assertThat(kernel.viewport()).isEqualTo(new TuiViewport(120, 30));
    }

    @Test
    void refusesToReduceStateFromANonUiThread() {
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("ready"));
        kernel.dispatch(new TuiEvent.Tick());

        Throwable failure = CompletableFuture.supplyAsync(() -> {
            try {
                kernel.dispatch(new TuiEvent.Tick());
                return null;
            } catch (Throwable thrown) {
                return thrown;
            }
        }).join();

        assertThat(failure).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UI thread");
    }

    @Test
    void inputClosedRequestsACleanUiExit() {
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("ready"));

        kernel.dispatch(new TuiEvent.InputClosed());

        assertThat(kernel.exitRequested()).isTrue();
    }

    @Test
    void queuesContextRecoveryOnTheWorkerBoundaryBeforeTheUiThreadReducesIt() {
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("boot"));
        ResolvedContext.Named context = new ResolvedContext.Named("dev", new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);

        kernel.post(new TuiEvent.ContextSessionPosted(new TuiContextSessionAction.Initialize(context)));

        assertThat(kernel.state().contextSession().connection()).isEqualTo(TuiDashboard.Connection.ONBOARDING);
        kernel.drain();
        assertThat(kernel.state().contextSession().connection()).isEqualTo(TuiDashboard.Connection.CONNECTING);
        assertThat(kernel.state().contextSession().context()).isEqualTo(context);
    }

    @Test
    void keepsKernelStateInSyncWhenInitialContextSessionIsInstalled() {
        TuiAppState initial = TuiAppState.initial("boot");
        TuiKernel kernel = new TuiKernel(initial);
        ResolvedContext.Named context = new ResolvedContext.Named("dev", new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);

        TuiAppState installed = TuiApp.initializeContextSessionState(kernel, initial, context, "");

        assertThat(installed).isSameAs(kernel.state());
        assertThat(kernel.state().contextSession().context()).isEqualTo(context);
        assertThat(kernel.state().contextSession().connection()).isEqualTo(TuiDashboard.Connection.CONNECTING);
    }
}
