package io.tapstate.cli;

import org.junit.jupiter.api.Test;

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
}
