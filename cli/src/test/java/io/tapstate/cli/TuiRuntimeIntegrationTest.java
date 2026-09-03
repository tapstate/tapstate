package io.tapstate.cli;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.terminal.Frame;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.schema.SchemaNavigator;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
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
    void contextSubcommandsAreAvailableToTheTuiCompleter() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        assertThat(registry.completer().candidates(List.of("context", ""), 1))
                .containsExactly("list");
    }

    @Test
    void localLsIsAvailableFromTheSharedTuiCompleter() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        assertThat(registry.completer().candidates(List.of("l"), 0)).contains("ls");
        assertThat(TuiCommandBar.complete(registry.completer(), new TuiCommandHistory(),
                List.of("context", ""), 1).candidates()).containsExactly("list");
    }

    @Test
    void suggestionsAreProjectedWhileTheCommandIsBeingTyped() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        TuiCommandBar.Completion suggestions = TuiCommandBar.suggestions(
                registry.completer(), new TuiCommandHistory(), List.of("l"), 0);

        assertThat(suggestions.candidates()).contains("ls");
        assertThat(TuiCommandBar.suggestions(registry.completer(), new TuiCommandHistory(),
                List.of("ls"), 0).candidates()).doesNotContain("ls");
    }

    @Test
    void promptArrowCodesOnlyMoveThePromptSelection() {
        assertThat(TuiApp.movePromptSelection(0, 1, 2)).isEqualTo(1);
        assertThat(TuiApp.movePromptSelection(1, 1, 2)).isEqualTo(1);
        assertThat(TuiApp.movePromptSelection(1, -1, 2)).isZero();
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
    void uiThreadCommandsUseTheReplDispatcher() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        Repl repl = new Repl(registry.commandLine(), Path.of("orders"));
        TuiApp app = new TuiApp(repl, new StringWriter(), new StringWriter(), null);
        Method dispatch = TuiApp.class.getDeclaredMethod("dispatchOnUiThread", String.class);
        dispatch.setAccessible(true);

        CommandResult result = (CommandResult) dispatch.invoke(app, "pwd");

        assertThat(result.keepRunning()).isTrue();
        assertThat(result.exitCode()).isZero();
        assertThat(output.toString()).contains("orders");
    }

    @Test
    void contextListRunsAsARealTuiCommand(@org.junit.jupiter.api.io.TempDir Path home) throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        ContextManager contexts = new ContextManager(ContextConfigStore.underHome(home));
        Repl repl = new Repl(registry.commandLine(), Path.of("orders"), new HttpControlPlaneClient(),
                null, System::getenv, null, null, null, contexts);
        TuiApp app = new TuiApp(repl, new StringWriter(), new StringWriter(), null);
        Method dispatch = TuiApp.class.getDeclaredMethod("dispatchOnUiThread", String.class);
        dispatch.setAccessible(true);

        CommandResult result = (CommandResult) dispatch.invoke(app, "context list");

        assertThat(result.keepRunning()).isTrue();
        assertThat(result.exitCode()).isZero();
        assertThat(output.toString()).contains("no saved contexts");
    }

    @Test
    void submittingContextListLeavesItsResultInTheWorkbenchState(@org.junit.jupiter.api.io.TempDir Path home)
            throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        Repl repl = new Repl(registry.commandLine(), Path.of("orders"), new HttpControlPlaneClient(),
                null, System::getenv, null, null, null,
                new ContextManager(ContextConfigStore.underHome(home)));
        TuiApp app = new TuiApp(repl, output, new StringWriter(), null);
        Field commandInput = TuiApp.class.getDeclaredField("commandInput");
        commandInput.setAccessible(true);
        commandInput.set(app, "context list");
        Method submit = TuiApp.class.getDeclaredMethod("submit");
        submit.setAccessible(true);

        assertThat((boolean) submit.invoke(app)).isTrue();

        Field uiState = TuiApp.class.getDeclaredField("uiState");
        uiState.setAccessible(true);
        TuiAppState state = (TuiAppState) uiState.get(app);
        assertThat(state.resultPane().lines()).containsExactly("no saved contexts");
    }

    @Test
    void asynchronousCommandsUseTheSameReplDispatcherAsUiThreadCommands() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        Repl repl = new Repl(registry.commandLine(), Path.of("orders"));
        TuiApp app = new TuiApp(repl, output, new StringWriter(), null);
        setField(app, "commandInput", "pwd");

        Method submit = TuiApp.class.getDeclaredMethod("submit");
        submit.setAccessible(true);
        assertThat((boolean) submit.invoke(app)).isTrue();

        TuiCommandExecution execution = (TuiCommandExecution) getField(app, "commandExecution");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (execution.isRunning() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(execution.isRunning()).isFalse();

        Method drain = TuiApp.class.getDeclaredMethod("drainCommandCompletions");
        drain.setAccessible(true);
        drain.invoke(app);
        TuiAppState state = (TuiAppState) getField(app, "uiState");
        assertThat(state.resultPane().lines()).containsExactly("orders");
    }

    @Test
    void offlineLsRemainsLocalWhenTheTuiHasAContextResolver(@org.junit.jupiter.api.io.TempDir Path base)
            throws Exception {
        Path workspace = Files.createDirectory(base.resolve("workspace"));
        Path sourceDirectory = Files.createDirectory(workspace.resolve("source"));
        Files.writeString(sourceDirectory.resolve("orders.tap.yml"),
                "kind: source\nid: orders\nconnector: mysql\nmode: cdc\n");
        ContextConfigStore store = ContextConfigStore.underHome(base.resolve("home"));
        ContextResolver resolver = new ContextResolver(store, name -> null);
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        registry.commandLine().setErr(new java.io.PrintWriter(output));
        Repl repl = new Repl(registry.commandLine(), workspace, new HttpControlPlaneClient(),
                null, name -> null, resolver, null, null, new ContextManager(store));
        TuiApp app = new TuiApp(repl, output, new StringWriter(), null, resolver, null);
        Method dispatch = TuiApp.class.getDeclaredMethod("dispatchOnUiThread", String.class);
        dispatch.setAccessible(true);

        CommandResult result = (CommandResult) dispatch.invoke(app, "ls");

        assertThat(result.exitCode()).isZero();
        assertThat(output.toString()).contains("orders");
    }

    @Test
    void offlineLsRemainsLocalWhenTheContextConfigIsUnsafe(@org.junit.jupiter.api.io.TempDir Path base)
            throws Exception {
        Path workspace = Files.createDirectory(base.resolve("workspace"));
        Path sourceDirectory = Files.createDirectory(workspace.resolve("source"));
        Files.writeString(sourceDirectory.resolve("orders.tap.yml"),
                "kind: source\nid: orders\nconnector: mysql\nmode: cdc\n");
        ContextResolver resolver = new ContextResolver(() -> {
            throw new TapstateException(CliError.CONTEXT_CONFIG_PERMISSIONS, Map.of(
                    "path", base.resolve(".tapstate"), "reason", "POSIX mode is not owner-only"), null);
        }, name -> null);
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new java.io.PrintWriter(output));
        registry.commandLine().setErr(new java.io.PrintWriter(output));
        Repl repl = new Repl(registry.commandLine(), workspace, new HttpControlPlaneClient(),
                null, name -> null, resolver, null, null, null);
        TuiApp app = new TuiApp(repl, output, new StringWriter(), null, resolver, null);
        Method dispatch = TuiApp.class.getDeclaredMethod("dispatchOnUiThread", String.class);
        dispatch.setAccessible(true);

        CommandResult result = (CommandResult) dispatch.invoke(app, "ls");

        assertThat(result.exitCode()).isZero();
        assertThat(output.toString()).contains("orders");
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

        Buffer buffer = Buffer.empty(new Rect(0, 0, 100, 24));
        new TamboDashboard().render(Frame.forTesting(buffer), state);

        assertThat(buffer.toAnsiString()).doesNotContain(password, token, ansi);
    }

    @Test
    void offlineWorkbenchUsesOneCleanWorkspaceAndOneCommandContainer() {
        TuiDashboard.State state = TuiDashboard.State.offline(Path.of("orders"), null);
        Buffer buffer = Buffer.empty(new Rect(0, 0, 100, 24));

        new TamboDashboard().render(Frame.forTesting(buffer), state);

        assertThat(buffer.toAnsiString()).contains("TAPSTATE", "Try: validate ./work")
                .doesNotContain("[COMMAND]");
        assertThat(buffer.toAnsiString()).doesNotContain("ACTIVITY", "WORKSPACE", "WELCOME");
    }

    @Test
    void dashboardKeepsResultLinesReadableAndComposerUnlabelled() {
        String output = "error: cli.context-config-permissions\n"
                + "The context directory permissions are too broad. Run chmod 700 on the directory.";
        TuiCommandBar.ResultPane result = TuiCommandBar.project(
                new CommandResult(false, Cli.EXIT_DIAGNOSTIC), output);
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), null, null, TuiDashboard.Connection.OFFLINE,
                null, "", List.of(), 0, null, null, null, null, List.of(), List.of(),
                List.of(), null, result);
        Buffer buffer = Buffer.empty(new Rect(0, 0, 60, 16));

        new TamboDashboard().render(Frame.forTesting(buffer), state);

        String rendered = buffer.toAnsiStringTrimmed();
        assertThat(rendered).contains("error: cli.context-config-permissions",
                "permissions are too broad");
        assertThat(rendered).doesNotContain("[COMMAND]", "[PROMPT]");
        assertThat(rendered).doesNotContain("› ");
    }

    @Test
    void dashboardScrollsTheResultViewport() {
        TuiCommandBar.ResultPane result = TuiCommandBar.project(
                new CommandResult(true, Cli.EXIT_OK), "line-0\nline-1\nline-2\nline-3");
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), null, null, TuiDashboard.Connection.OFFLINE,
                null, "", List.of(), 0, null, null, null, null, List.of(), List.of(), List.of(), null, result);
        Buffer buffer = Buffer.empty(new Rect(0, 0, 60, 10));

        new TamboDashboard().render(Frame.forTesting(buffer), state, 4);

        String rendered = buffer.toAnsiStringTrimmed();
        assertThat(rendered).contains("line-2").doesNotContain("line-0", "line-1");
    }

    @Test
    void dashboardUsesAQuietRoundedComposerAndVisibleWorkspaceScrollbar() {
        List<String> output = java.util.stream.IntStream.range(0, 12)
                .mapToObj(index -> "line-" + index)
                .toList();
        TuiCommandBar.ResultPane result = TuiCommandBar.project(
                new CommandResult(true, Cli.EXIT_OK), String.join("\n", output));
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), null, null, TuiDashboard.Connection.OFFLINE,
                "scroll 3", "ls", List.of(), 0, null, null, null, null, List.of(), List.of(),
                List.of(), null, result);
        Buffer buffer = Buffer.empty(new Rect(0, 0, 60, 16));

        new TamboDashboard().render(Frame.forTesting(buffer), state, 3);

        String rendered = buffer.toAnsiStringTrimmed();
        assertThat(rendered).contains("╭", "╮", "╰", "╯", "┃", "ls▌")
                .doesNotContain("scroll 3");
    }

    @Test
    void dashboardRendersLiveSuggestionsAboveTheComposer() {
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), null, null, TuiDashboard.Connection.OFFLINE,
                "suggestions · ↑/↓ choose · Enter select", "l", List.of("ls", "login"), 0, null);
        Buffer buffer = Buffer.empty(new Rect(0, 0, 80, 16));

        new TamboDashboard().render(Frame.forTesting(buffer), state);

        String rendered = buffer.toAnsiStringTrimmed();
        assertThat(rendered).contains("ls", "List workspace resources", "l▌");
        assertThat(rendered).doesNotContain("[COMMAND]", "> ");
    }

    @Test
    void liveSuggestionsSitImmediatelyAboveTheComposer() {
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), null, null, TuiDashboard.Connection.OFFLINE,
                "suggestions · ↑/↓ choose · Enter select", "l", List.of("ls", "login"), 0, null);
        Buffer buffer = Buffer.empty(new Rect(0, 0, 80, 16));

        new TamboDashboard().render(Frame.forTesting(buffer), state);

        int composerTop = rowContaining(buffer, "╭");
        assertThat(composerTop).isGreaterThan(0);
        assertThat(row(buffer, composerTop - 2)).contains("› ls");
        assertThat(row(buffer, composerTop - 1)).contains("login");
        assertThat(row(buffer, composerTop + 1)).contains("l▌");
        assertThat(row(buffer, composerTop + 3)).contains("Enter select");
    }

    @Test
    void movingThroughLiveSuggestionsKeepsTheInlineSurfaceOpen() throws Exception {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        TuiApp app = new TuiApp(new Repl(registry.commandLine()), new StringWriter(), new StringWriter(), null);
        invokeReduce(app, new TuiAction.OpenPalette(List.of("ls", "login"),
                "suggestions · ↑/↓ choose · Enter select"));
        setField(app, "suggestionsVisible", true);

        Class<?> escapeKey = java.util.Arrays.stream(TuiApp.class.getDeclaredClasses())
                .filter(type -> type.getSimpleName().equals("EscapeKey"))
                .findFirst().orElseThrow();
        Method navigate = TuiApp.class.getDeclaredMethod("navigate", escapeKey);
        navigate.setAccessible(true);
        Object down = Enum.valueOf((Class) escapeKey, "DOWN");
        navigate.invoke(app, down);

        TuiAppState state = (TuiAppState) getField(app, "uiState");
        assertThat(state.paletteIndex()).isEqualTo(1);
        assertThat(state.notice()).startsWith("suggestions");
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
    void resultProjectionPreservesTheDiagnosticExplanation() {
        TuiCommandBar.ResultPane pane = TuiCommandBar.project(new CommandResult(true, Cli.EXIT_DIAGNOSTIC),
                "error: cli.server-required\nConnect to a server before running ls.\n(cli 0.3.0, server not connected)");

        assertThat(pane.lines()).containsExactly("error: cli.server-required",
                "Connect to a server before running ls.", "(cli 0.3.0, server not connected)");
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

    private static int rowContaining(Buffer buffer, String value) {
        for (int y = 0; y < buffer.height(); y++) {
            if (row(buffer, y).contains(value)) {
                return y;
            }
        }
        return -1;
    }

    private static String row(Buffer buffer, int y) {
        StringBuilder value = new StringBuilder();
        for (int x = 0; x < buffer.width(); x++) {
            value.append(buffer.get(x, y).symbol());
        }
        return value.toString();
    }

    private static ResolvedContext.Named context(String name) {
        return new ResolvedContext.Named(name, new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);
    }
}
