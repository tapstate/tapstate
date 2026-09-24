package io.tapstate.cli;

import dev.tamboui.tui.TuiRunner;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.jline.utils.InfoCmp;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class WorkbenchTerminalBackendTest {

    @Test
    void closeRestoresRawModeAndPriorSignalHandlersExactlyOnce() throws Exception {
        TerminalSpy spy = new TerminalSpy(testTerminal());
        Terminal.SignalHandler priorInterrupt = signal -> { };
        Terminal.SignalHandler priorResize = signal -> { };
        spy.handlers.put(Terminal.Signal.INT, priorInterrupt);
        spy.handlers.put(Terminal.Signal.WINCH, priorResize);
        WorkbenchTerminalBackend backend = new WorkbenchTerminalBackend(spy.terminal());
        backend.quitOnInterrupt(() -> { });
        backend.onResize(() -> { });
        backend.enableRawMode();
        int attributesWrittenDuringSetup = spy.attributeWrites.get();

        backend.close();
        backend.close();

        assertThat(spy.handlers.get(Terminal.Signal.INT)).isSameAs(priorInterrupt);
        assertThat(spy.handlers.get(Terminal.Signal.WINCH)).isSameAs(priorResize);
        assertThat(spy.attributeWrites.get() - attributesWrittenDuringSetup)
                .as("cooked attributes restored by close")
                .isEqualTo(1);
        assertThat(spy.closeCalls).hasValue(1);
    }

    @Test
    void realRunnerCreationFailureKeepsSetupFailurePrimaryAndSuppressesCleanup() throws Exception {
        TerminalSpy spy = new TerminalSpy(testTerminal());
        IllegalStateException setupFailure = new IllegalStateException("alternate screen setup failed");
        IOException cleanupFailure = new IOException("terminal close failed");
        spy.enterAlternateScreenFailure = setupFailure;
        spy.closeFailure = cleanupFailure;
        WorkbenchTerminalBackend backend = new WorkbenchTerminalBackend(spy.terminal());

        Throwable thrown = catchThrowable(() -> Workbench.createRunner(backend));

        assertThat(thrown).isSameAs(setupFailure);
        assertThat(thrown.getSuppressed()).containsExactly(cleanupFailure);
        assertThat(spy.attributeWrites).as("raw setup plus one cooked restoration").hasValue(2);
        assertThat(spy.closeCalls).hasValue(1);

        backend.close();
        assertThat(spy.attributeWrites).hasValue(2);
        assertThat(spy.closeCalls).hasValue(1);
    }

    @Test
    void normalRunnerCloseSurfacesRecordedCleanupFailure() throws Exception {
        TerminalSpy spy = new TerminalSpy(testTerminal());
        IOException cleanupFailure = new IOException("terminal close failed");
        spy.closeFailure = cleanupFailure;
        WorkbenchTerminalBackend backend = new WorkbenchTerminalBackend(spy.terminal());
        TuiRunner runner = Workbench.createRunner(backend);

        Throwable thrown = catchThrowable(() -> Workbench.closeRunner(runner, backend));

        assertThat(thrown).isSameAs(cleanupFailure);
        assertThat(spy.closeCalls).hasValue(1);
    }

    private static Terminal testTerminal() throws Exception {
        return new DumbTerminal(
                new ByteArrayInputStream("\u001b[?2027;0$y".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new ByteArrayOutputStream());
    }

    private static final class TerminalSpy {
        private final Terminal delegate;
        private final EnumMap<Terminal.Signal, Terminal.SignalHandler> handlers =
                new EnumMap<>(Terminal.Signal.class);
        private final AtomicInteger attributeWrites = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private RuntimeException enterAlternateScreenFailure;
        private IOException closeFailure;

        private TerminalSpy(Terminal delegate) {
            this.delegate = delegate;
        }

        private Terminal terminal() {
            return (Terminal) Proxy.newProxyInstance(
                    Terminal.class.getClassLoader(), new Class<?>[] {Terminal.class}, this::invoke);
        }

        private Object invoke(Object proxy, java.lang.reflect.Method method, Object[] arguments) throws Throwable {
            if (method.getName().equals("handle")) {
                return handlers.put((Terminal.Signal) arguments[0], (Terminal.SignalHandler) arguments[1]);
            }
            if (method.getName().equals("setAttributes")) {
                attributeWrites.incrementAndGet();
            }
            if (method.getName().equals("puts")
                    && arguments[0] == InfoCmp.Capability.enter_ca_mode
                    && enterAlternateScreenFailure != null) {
                RuntimeException failure = enterAlternateScreenFailure;
                enterAlternateScreenFailure = null;
                throw failure;
            }
            if (method.getName().equals("close")) {
                closeCalls.incrementAndGet();
                if (closeFailure != null) {
                    throw closeFailure;
                }
            }
            try {
                return method.invoke(delegate, arguments);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        }
    }
}
