package io.tapstate.cli;

import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.KeyEvent;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-process entry point for lifecycle paths that the production command cannot inject. */
public final class WorkbenchPtyProbe {

    private WorkbenchPtyProbe() {
    }

    public static void main(String[] arguments) throws Exception {
        System.out.println("__TAPSTATE_PTY_JVM_PID__" + ProcessHandle.current().pid());
        System.out.flush();
        switch (arguments[0]) {
            case "bare" -> Cli.main(new String[0]);
            case "backend-selection" -> runWithCompetingBackend();
            case "injected-exception" -> runWithInjectedException();
            case "exceptional-unwind" -> runWithExceptionalUnwind();
            case "setup-failure" -> runWithSetupFailure();
            default -> throw new IllegalArgumentException("unknown PTY probe mode: " + arguments[0]);
        }
    }

    private static void runWithInjectedException() throws Exception {
        Terminal terminal = TerminalBuilder.builder().system(true).dumb(true).build();
        TuiRunner runner;
        try {
            runner = createWorkbenchRunner(terminal);
        } catch (Exception failure) {
            terminal.close();
            throw failure;
        }
        AtomicBoolean injected = new AtomicBoolean();
        try (runner) {
            runner.run((event, activeRunner) -> {
                if (event instanceof KeyEvent key && key.isChar('x') && injected.compareAndSet(false, true)) {
                    throw new IllegalStateException("Injected lifecycle failure");
                }
                return false;
            }, Workbench::render);
        }
    }

    private static void runWithCompetingBackend() throws Exception {
        System.setProperty("tamboui.backend", "competing-test-backend");
        CompetingBackendProvider.CREATE_CALLED.set(false);
        Terminal terminal = TerminalBuilder.builder().system(true).dumb(true).build();
        try (TuiRunner runner = createWorkbenchRunner(terminal)) {
            System.out.println("__TAPSTATE_BACKEND__" + runner.backend().getClass().getName());
            System.out.flush();
            if (CompetingBackendProvider.CREATE_CALLED.get()) {
                throw new AssertionError("the competing backend was selected");
            }
            runner.run((event, activeRunner) -> {
                if (event instanceof KeyEvent key && key.isCharIgnoreCase('q')) {
                    activeRunner.quit();
                    return true;
                }
                return false;
            }, Workbench::render);
        } finally {
            System.clearProperty("tamboui.backend");
        }
    }

    private static TuiRunner createWorkbenchRunner(Terminal terminal) throws Exception {
        Method factory = Workbench.class.getDeclaredMethod("createRunner", Terminal.class);
        factory.setAccessible(true);
        try {
            return (TuiRunner) factory.invoke(null, terminal);
        } catch (InvocationTargetException failure) {
            if (failure.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw failure;
        }
    }

    private static void runWithExceptionalUnwind() throws Exception {
        Terminal terminal = TerminalBuilder.builder().system(true).dumb(true).build();
        try (TuiRunner runner = createWorkbenchRunner(terminal)) {
            throw new IllegalStateException("Injected runner-boundary failure");
        }
    }

    private static void runWithSetupFailure() throws Exception {
        Terminal terminal = TerminalBuilder.builder().system(true).dumb(true).build();
        WorkbenchTerminalBackend backend = new WorkbenchTerminalBackend(terminal);
        try {
            backend.enableRawMode();
            backend.enterAlternateScreen();
            backend.hideCursor();
            throw new IllegalStateException("Injected setup failure");
        } catch (IllegalStateException expected) {
            backend.close();
            backend.close();
        }
    }
}
