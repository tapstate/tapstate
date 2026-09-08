package io.tapstate.cli;

import dev.tamboui.backend.jline3.JLineBackend;
import org.jline.terminal.Attributes.ControlChar;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Owns the JLine terminal and bridges terminal lifecycle events to the workbench runner. */
final class WorkbenchTerminalBackend extends JLineBackend {

    private static final int END_OF_INPUT = -1;

    private final Terminal terminal;
    private final int eofControlCharacter;
    private final AtomicBoolean eofSeen = new AtomicBoolean();
    private final AtomicBoolean eofDelivered = new AtomicBoolean();
    private final AtomicReference<Runnable> eofHandler = new AtomicReference<>();

    private boolean rawModeEnabled;
    private boolean alternateScreenEntered;
    private boolean cursorHidden;
    private boolean mouseCaptureEnabled;
    private boolean bracketedPasteEnabled;
    private boolean closed;
    private Throwable cleanupFailure;
    private Terminal.SignalHandler previousInterruptHandler;
    private Terminal.SignalHandler previousResizeHandler;

    WorkbenchTerminalBackend(Terminal terminal) {
        super(Objects.requireNonNull(terminal, "terminal"));
        this.terminal = terminal;
        this.eofControlCharacter = terminal.getAttributes().getControlChar(ControlChar.VEOF);
    }

    void quitOnEof(Runnable handler) {
        if (!eofHandler.compareAndSet(null, Objects.requireNonNull(handler, "handler"))) {
            throw new IllegalStateException("EOF handler is already installed");
        }
        deliverEof();
    }

    synchronized void quitOnInterrupt(Runnable handler) {
        Runnable quit = Objects.requireNonNull(handler, "handler");
        if (previousInterruptHandler != null) {
            throw new IllegalStateException("Interrupt handler is already installed");
        }
        previousInterruptHandler = terminal.handle(Terminal.Signal.INT, ignored -> quit.run());
    }

    @Override
    public int read(int timeout) throws IOException {
        return detectEof(super.read(timeout));
    }

    @Override
    public int peek(int timeout) throws IOException {
        return detectEof(super.peek(timeout));
    }

    @Override
    public synchronized void onResize(Runnable handler) {
        Runnable resize = Objects.requireNonNull(handler, "handler");
        if (previousResizeHandler != null) {
            throw new IllegalStateException("Resize handler is already installed");
        }
        previousResizeHandler = terminal.handle(Terminal.Signal.WINCH, ignored -> resize.run());
    }

    @Override
    public synchronized void enableRawMode() throws IOException {
        if (rawModeEnabled) {
            return;
        }
        rawModeEnabled = true;
        super.enableRawMode();
    }

    @Override
    public synchronized void disableRawMode() throws IOException {
        if (!rawModeEnabled) {
            return;
        }
        super.disableRawMode();
        rawModeEnabled = false;
    }

    @Override
    public synchronized void enterAlternateScreen() throws IOException {
        if (alternateScreenEntered) {
            return;
        }
        alternateScreenEntered = true;
        super.enterAlternateScreen();
    }

    @Override
    public synchronized void leaveAlternateScreen() throws IOException {
        if (!alternateScreenEntered) {
            return;
        }
        super.leaveAlternateScreen();
        alternateScreenEntered = false;
    }

    @Override
    public synchronized void hideCursor() throws IOException {
        if (cursorHidden) {
            return;
        }
        cursorHidden = true;
        super.hideCursor();
    }

    @Override
    public synchronized void showCursor() throws IOException {
        if (!cursorHidden) {
            return;
        }
        super.showCursor();
        cursorHidden = false;
    }

    @Override
    public synchronized void enableMouseCapture() throws IOException {
        if (mouseCaptureEnabled) {
            return;
        }
        mouseCaptureEnabled = true;
        super.enableMouseCapture();
    }

    @Override
    public synchronized void disableMouseCapture() throws IOException {
        if (!mouseCaptureEnabled) {
            return;
        }
        super.disableMouseCapture();
        mouseCaptureEnabled = false;
    }

    @Override
    public synchronized void enableBracketedPaste() throws IOException {
        if (bracketedPasteEnabled) {
            return;
        }
        bracketedPasteEnabled = true;
        super.enableBracketedPaste();
    }

    @Override
    public synchronized void disableBracketedPaste() throws IOException {
        if (!bracketedPasteEnabled) {
            return;
        }
        super.disableBracketedPaste();
        bracketedPasteEnabled = false;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;

        Throwable failure = null;
        failure = attempt(failure, this::disableBracketedPaste);
        failure = attempt(failure, this::disableMouseCapture);
        failure = attempt(failure, this::showCursor);
        failure = attempt(failure, this::leaveAlternateScreen);
        failure = attempt(failure, this::disableRawMode);
        failure = attempt(failure, this::restoreSignalHandlers);
        failure = attempt(failure, this::resetStyle);
        failure = attempt(failure, terminal::close);
        cleanupFailure = failure;
    }

    synchronized void throwIfCleanupFailed() throws IOException {
        if (cleanupFailure instanceof IOException failure) {
            throw failure;
        }
        if (cleanupFailure instanceof RuntimeException failure) {
            throw failure;
        }
        if (cleanupFailure instanceof Error failure) {
            throw failure;
        }
    }

    private int detectEof(int value) {
        if (value == END_OF_INPUT || value == eofControlCharacter) {
            eofSeen.set(true);
            deliverEof();
            return END_OF_INPUT;
        }
        return value;
    }

    private void deliverEof() {
        Runnable handler = eofHandler.get();
        if (handler != null && eofSeen.get() && eofDelivered.compareAndSet(false, true)) {
            handler.run();
        }
    }

    private void restoreSignalHandlers() {
        if (previousInterruptHandler != null) {
            terminal.handle(Terminal.Signal.INT, previousInterruptHandler);
            previousInterruptHandler = null;
        }
        if (previousResizeHandler != null) {
            terminal.handle(Terminal.Signal.WINCH, previousResizeHandler);
            previousResizeHandler = null;
        }
    }

    private void resetStyle() throws IOException {
        terminal.writer().print("\u001b[0m");
        terminal.flush();
    }

    private static Throwable attempt(Throwable failure, IoAction action) {
        try {
            action.run();
        } catch (Throwable next) {
            if (failure == null) {
                return next;
            }
            if (failure != next) {
                failure.addSuppressed(next);
            }
        }
        return failure;
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }
}
