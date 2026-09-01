package io.tapstate.cli;

import io.tapstate.core.schema.SchemaNavigator;
import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class TuiRuntimeIntegrationTest {

    @Test
    void paletteIsDerivedFromTheSharedRegistryAndIncludesTuiHooks() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        List<String> palette = TuiApp.paletteCommands(registry);

        assertThat(palette).contains("validate", "auth", "help", ":login", ":help");
        assertThat(palette).doesNotContain("made-up-command");
    }

    @Test
    void runtimeClassifiesOperationsWithoutChangingCommandSemantics() {
        assertThat(TuiApp.operationFor("status orders --watch", 1).kind())
                .isEqualTo(TuiOperation.Kind.STREAM);
        assertThat(TuiApp.operationFor("pipeline.start", 2).kind())
                .isEqualTo(TuiOperation.Kind.WRITE);
        assertThat(TuiApp.operationFor("start orders", 3).kind())
                .isEqualTo(TuiOperation.Kind.WRITE);
    }

    @Test
    void networkBackedLoginCommandsDoNotRunOnTheUiThread() {
        assertThat(TuiApp.requiresUiThread("auth login alice")).isFalse();
        assertThat(TuiApp.requiresUiThread(":login")).isFalse();
        assertThat(TuiApp.requiresUiThread("auth status")).isFalse();
        assertThat(TuiApp.requiresUiThread("context")).isTrue();
        assertThat(TuiApp.requiresUiThread("disconnect")).isTrue();
        assertThat(TuiApp.requiresUiThread("auth logout")).isTrue();
    }

    @Test
    void paletteBuiltinsUseTheReplBuiltinSemantics() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        Repl repl = new Repl(registry.commandLine());
        TuiApp app = new TuiApp(repl, new StringWriter(), new StringWriter(), null);
        Method dispatch = TuiApp.class.getDeclaredMethod("dispatchOnUiThread", String.class);
        dispatch.setAccessible(true);

        CommandResult help = (CommandResult) dispatch.invoke(app, ":help");
        CommandResult quit = (CommandResult) dispatch.invoke(app, ":quit");

        assertThat(help.keepRunning()).isTrue();
        assertThat(output.toString()).contains("Usage:");
        assertThat(quit.keepRunning()).isFalse();
    }

    @Test
    void completionReplacesOnlyTheCurrentWord() {
        TuiCommandBar.Completion completion = new TuiCommandBar.Completion(
                List.of("validate", "version"), 0);

        assertThat(TuiApp.applyCompletion("val", completion)).isEqualTo("validate");
        assertThat(TuiApp.applyCompletion("validate ", completion)).isEqualTo("validate validate");
        assertThat(TuiApp.applyCompletion("auth st", new TuiCommandBar.Completion(
                List.of("status"), 0))).isEqualTo("auth status");
    }

    @Test
    void typedCredentialArgumentsDoNotEnterTheKernelSnapshot() {
        String token = "tok-typed-secret";
        TuiKernel kernel = new TuiKernel(TuiAppState.initial("ready"));

        for (int codePoint : ("connect --token " + token).codePoints().toArray()) {
            kernel.dispatch(new TuiEvent.Key(codePoint));
        }

        assertThat(kernel.state().command()).doesNotContain(token);
        assertThat(kernel.state().toString()).doesNotContain(token);
    }

    @Test
    void renderedFrameDoesNotEchoUntrustedCredentialBearingState() {
        String password = "pw-frame-secret";
        String token = "tok-frame-secret";
        String ansi = "\u001b[31m";
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), "production", "alice", TuiDashboard.Connection.ONLINE,
                "prompt failed: password=" + password + ansi,
                "connect --token " + token + ansi, List.of(), 0, null,
                null, null, null,
                List.of("Authorization: Bearer " + token + ansi),
                List.of(), List.of(), null, null);

        List<String> frame = new TuiDashboard().render(state, 100, 24).stream()
                .map(AttributedString::toString)
                .toList();

        assertThat(frame).allSatisfy(line -> assertThat(line)
                .doesNotContain(password, token, ansi));
    }

    @Test
    void stdoutAndStderrProjectionRedactsSecretsBeforeResultStorage() {
        String password = "pw-output-secret";
        String token = "tok-output-secret";
        String output = "stdout password=" + password + "\n"
                + "stderr Authorization: Bearer " + token + "\u001b[2J";

        TuiCommandBar.ResultPane pane = TuiCommandBar.project(
                new CommandResult(false, Cli.EXIT_DIAGNOSTIC), output);

        assertThat(pane.lines()).allSatisfy(line -> assertThat(line)
                .doesNotContain(password, token, "\u001b"));
        assertThat(pane.notice()).doesNotContain(password, token, "\u001b");
        assertThat(pane.toString()).doesNotContain(password, token, "\u001b");
    }

    @Test
    void commandHistoryDoesNotRetainCredentialArgumentsOrTerminalControls() {
        String token = "tok-history-secret";
        TuiCommandHistory history = new TuiCommandHistory();

        history.record("connect --token " + token + "\u001b[2J");

        assertThat(history.entries().toString()).doesNotContain(token, "\u001b");
        assertThat(history.matches("connect").toString()).doesNotContain(token, "\u001b");
    }

    @Test
    void writeCannotBuildAConfirmationWithAPlaceholderIssuerWhileContextIsConnecting() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        Repl repl = new Repl(registry.commandLine());
        TuiApp app = new TuiApp(repl, new StringWriter(), new StringWriter(), null);

        app.switchContext(context("prod"));

        Method writeIssuer = TuiApp.class.getDeclaredMethod("writeIssuer");
        writeIssuer.setAccessible(true);

        assertThat((String) writeIssuer.invoke(app))
                .as("a write confirmation must carry the issuer resolved for the selected context")
                .isNotEqualTo("unresolved");
    }

    @Test
    void commandCompletionClearsTheWriteLockForTheCompletedOperation() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        TuiApp app = new TuiApp(new Repl(registry.commandLine()), new StringWriter(), new StringWriter(), null);
        TuiOperation write = TuiOperation.submittedWrite("op-write", "pipeline.start");
        invokeReduce(app, new TuiAction.SetOperation(write));
        invokeReduce(app, new TuiAction.ContextSession(
                new TuiContextSessionAction.SetWriteInFlight(write.id())));
        setField(app, "commandRunning", true);
        setField(app, "activeOperationId", write.id());

        Method complete = TuiApp.class.getDeclaredMethod(
                "completeCommand", String.class, CommandResult.class, String.class, Throwable.class);
        complete.setAccessible(true);
        complete.invoke(app, write.id(), new CommandResult(true, Cli.EXIT_OK), "", null);

        TuiAppState state = (TuiAppState) getField(app, "uiState");
        assertThat(state.operation().status()).isEqualTo(TuiOperation.Status.COMPLETED);
        assertThat(state.contextSession().writeOperationId()).isNull();
    }

    @Test
    void cancelledSubmittedWriteIgnoresItsLateCompletion() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        TuiApp app = new TuiApp(new Repl(registry.commandLine()), new StringWriter(), new StringWriter(), null);
        TuiOperation write = TuiOperation.submittedWrite("op-write", "pipeline.start");
        invokeReduce(app, new TuiAction.SetOperation(write));
        invokeReduce(app, new TuiAction.ContextSession(
                new TuiContextSessionAction.SetWriteInFlight(write.id())));
        setField(app, "commandRunning", true);
        setField(app, "activeOperationId", write.id());

        Method cancel = TuiApp.class.getDeclaredMethod("cancelCurrentOperation");
        cancel.setAccessible(true);
        cancel.invoke(app);

        assertThat(((TuiAppState) getField(app, "uiState")).operation().status())
                .isEqualTo(TuiOperation.Status.WAITING_STOPPED);
        assertThat(((TuiAppState) getField(app, "uiState")).contextSession().writeOperationId())
                .isEqualTo(write.id());
        Method complete = TuiApp.class.getDeclaredMethod(
                "completeCommand", String.class, CommandResult.class, String.class, Throwable.class);
        complete.setAccessible(true);
        complete.invoke(app, write.id(), new CommandResult(true, Cli.EXIT_OK), "late", null);

        assertThat(((TuiAppState) getField(app, "uiState")).operation().status())
                .isEqualTo(TuiOperation.Status.WAITING_STOPPED);
        assertThat(((TuiAppState) getField(app, "uiState")).contextSession().writeOperationId())
                .isEqualTo(write.id());
    }

    @Test
    void ctrlCStopsWaitingForSubmittedWriteWithoutInterruptingItsWorker() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        TuiApp app = new TuiApp(new Repl(registry.commandLine()), new StringWriter(), new StringWriter(), null);
        TuiOperation write = TuiOperation.submittedWrite("op-write", "pipeline.start");
        invokeReduce(app, new TuiAction.SetOperation(write));
        setField(app, "commandRunning", true);
        setField(app, "activeOperationId", write.id());
        TuiCommandExecution execution = (TuiCommandExecution) getField(app, "commandExecution");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        execution.start(write.id(), () -> {
            started.countDown();
            try {
                release.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return new CommandResult(true, Cli.EXIT_OK);
        }, () -> "");
        assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

        Method cancel = TuiApp.class.getDeclaredMethod("cancelCurrentOperation");
        cancel.setAccessible(true);
        cancel.invoke(app);
        release.countDown();
        while (execution.isRunning()) {
            Thread.onSpinWait();
        }

        assertThat(interrupted).isFalse();
    }

    private static void invokeReduce(TuiApp app, TuiAction action) throws Exception {
        Method reduce = TuiApp.class.getDeclaredMethod("reduce", TuiAction.class);
        reduce.setAccessible(true);
        setField(app, "uiState", reduce.invoke(app, action));
    }

    private static Object getField(TuiApp app, String name) throws Exception {
        Field field = TuiApp.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(app);
    }

    private static void setField(TuiApp app, String name, Object value) throws Exception {
        Field field = TuiApp.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(app, value);
    }

    private static ResolvedContext.Named context(String name) {
        return new ResolvedContext.Named(name, new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);
    }
}
