package io.tapstate.cli;

import dev.tamboui.terminal.Frame;
import dev.tamboui.layout.Rect;
import dev.tamboui.tui.TuiConfig;
import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.MouseEvent;
import dev.tamboui.tui.event.PasteEvent;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The full-screen terminal owner for a bare CLI launch.
 *
 * <p>This is deliberately the only place that creates a TamboUI runner. Business verbs stay outside
 * this lifecycle and continue through Picocli's one-shot command path.
 */
final class Workbench {

    private Workbench() {
    }

    /** Opens the initial workbench shell and returns after its runner has restored the terminal. */
    static int run(Repl session) {
        Terminal terminal = null;
        try {
            terminal = TerminalBuilder.builder().system(true).dumb(true).build();
            if ("dumb".equalsIgnoreCase(terminal.getType())) {
                terminal.close();
                Diagnostics.printText(session.errorOutput(), CliError.WORKBENCH_NEEDS_A_TERMINAL, Map.of());
                return Cli.EXIT_DIAGNOSTIC;
            }
            WorkbenchTerminalBackend backend = new WorkbenchTerminalBackend(terminal);
            terminal = null;
            TuiRunner runner = createRunner(backend);
            try {
                Session workbench = new Session(
                        runner, session, session.workbenchDataSource(), session.workbenchActionGateway());
                try {
                    runner.runLater(workbench::refresh);
                    runner.run(workbench::handleEvent, workbench::render);
                } finally {
                    workbench.close();
                }
            } catch (Exception | Error failure) {
                closeAfterFailure(runner, backend, failure);
                throw failure;
            }
            closeRunner(runner, backend);
            return Cli.EXIT_OK;
        } catch (Exception ignored) {
            closeQuietly(terminal);
            Diagnostics.printText(session.errorOutput(), CliError.WORKBENCH_UNAVAILABLE, Map.of());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    /**
     * Creates a runner with an explicit JLine backend so service discovery cannot select another
     * terminal implementation when future optional integrations appear on the classpath.
     */
    private static TuiRunner createRunner(Terminal terminal) throws Exception {
        return createRunner(new WorkbenchTerminalBackend(terminal));
    }

    static TuiRunner createRunner(WorkbenchTerminalBackend backend) throws Exception {
        TuiRunner runner = null;
        try {
            runner = TuiRunner.create(runnerConfig(backend));
            backend.quitOnEof(runner::quit);
            backend.quitOnInterrupt(runner::quit);
            return runner;
        } catch (Exception | Error failure) {
            closeAfterFailure(runner, backend, failure);
            throw failure;
        }
    }

    static TuiConfig runnerConfig(WorkbenchTerminalBackend backend) {
        return TuiConfig.builder()
                .backend(backend)
                .mouseCapture(true)
                .bracketedPaste(true)
                .noTick()
                .build();
    }

    static void closeRunner(TuiRunner runner, WorkbenchTerminalBackend backend) throws Exception {
        try {
            runner.close();
        } catch (Exception | Error failure) {
            closeAfterFailure(null, backend, failure);
            throw failure;
        }
        backend.throwIfCleanupFailed();
    }

    private static void closeAfterFailure(
            TuiRunner runner, WorkbenchTerminalBackend backend, Throwable failure) {
        try {
            if (runner == null) {
                backend.close();
                backend.throwIfCleanupFailed();
            } else {
                closeRunner(runner, backend);
            }
        } catch (Exception | Error cleanupFailure) {
            addSuppressedOnce(failure, cleanupFailure);
        }
    }

    private static void addSuppressedOnce(Throwable failure, Throwable cleanupFailure) {
        if (cleanupFailure == failure) {
            return;
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            if (suppressed == cleanupFailure) {
                return;
            }
        }
        failure.addSuppressed(cleanupFailure);
    }

    /** Renders from the runner-owned frame so resize changes take effect without a second terminal owner. */
    static void render(Frame frame) {
        WorkbenchRenderer.render(frame, WorkbenchState.initial());
    }

    private static void closeQuietly(Terminal terminal) {
        if (terminal == null) {
            return;
        }
        try {
            terminal.close();
        } catch (Exception ignored) {
            // The original terminal initialization failure is the diagnosable outcome.
        }
    }

    /** The render-thread session for one runner lifecycle. */
    static final class Session implements AutoCloseable {
        private final WorkbenchRuntime runtime;
        private final WorkbenchDataSource dataSource;
        private final WorkbenchActionGateway actionGateway;
        private final RefreshCoordinator refreshCoordinator;
        private final WorkbenchActionCoordinator actionCoordinator;
        private final WorkbenchShellPanel shellPanel;
        private volatile WorkbenchSnapshot lastSuccessfulSnapshot;
        private WorkbenchRenderer.RenderLayout layout = WorkbenchRenderer.RenderLayout.forTooSmallFrame();

        private Session(
                TuiRunner runner,
                Repl repl,
                WorkbenchDataSource dataSource,
                WorkbenchActionGateway actionGateway) {
            this(new WorkbenchRuntime(
                            WorkbenchState.initial(),
                            runner::runLater,
                            runner::isRenderThread,
                            runner::dispatch),
                    dataSource,
                    actionGateway,
                    repl);
        }

        Session(WorkbenchRuntime runtime) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.dataSource = null;
            this.actionGateway = null;
            this.refreshCoordinator = null;
            this.actionCoordinator = null;
            this.shellPanel = null;
        }

        Session(WorkbenchRuntime runtime, WorkbenchDataSource dataSource) {
            this(runtime, dataSource, null);
        }

        Session(
                WorkbenchRuntime runtime,
                WorkbenchDataSource dataSource,
                WorkbenchActionGateway actionGateway) {
            this(runtime, dataSource, actionGateway, null);
        }

        private Session(
                WorkbenchRuntime runtime,
                WorkbenchDataSource dataSource,
                WorkbenchActionGateway actionGateway,
                Repl repl) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
            this.actionGateway = actionGateway;
            this.refreshCoordinator = new RefreshCoordinator(this::publishRefreshResult);
            this.actionCoordinator = actionGateway == null ? null : new WorkbenchActionCoordinator(runtime);
            this.shellPanel = repl == null ? null : new WorkbenchShellPanel(
                    repl, runtime::requestRender, runtime::runLater);
        }

        boolean handleEvent(Event event, TuiRunner runner) {
            if (event == WorkbenchRedrawEvent.INSTANCE) {
                return true;
            }
            if (runtime.state().overlay().isPresent()) {
                if (event instanceof KeyEvent key && key.isCtrlC()) {
                    clearOverlaySecret();
                    runner.quit();
                    return true;
                }
                return handleOverlayEvent(event, runner);
            }
            if (shellPanel != null && shellPanel.isOpen()) {
                if (event instanceof KeyEvent key) {
                    return shellPanel.handle(key);
                }
                if (event instanceof MouseEvent mouse) {
                    return shellPanel.handle(mouse);
                }
                if (event instanceof PasteEvent paste) {
                    return shellPanel.paste(paste.text());
                }
                return true;
            }
            if (runtime.state().selectedTab() == WorkbenchState.WorkbenchTab.WORKSPACE
                    && runtime.state().workspaceView().editing()) {
                return handleWorkspaceEditorEvent(event, runner);
            }
            if (event instanceof PasteEvent) {
                return false;
            }
            if (event instanceof KeyEvent key) {
                if (key.isKey(dev.tamboui.tui.event.KeyCode.F2)) {
                    return openActions();
                }
                if (key.isKey(dev.tamboui.tui.event.KeyCode.F6) && shellPanel != null) {
                    shellPanel.open();
                    return true;
                }
                if (key.isCharIgnoreCase('q') || key.isCtrlC()) {
                    runner.quit();
                    return true;
                }
                if (key.isCharIgnoreCase('r') && refreshCoordinator != null) {
                    refresh();
                    return true;
                }
                if (runtime.state().selectedTab() == WorkbenchState.WorkbenchTab.WORKSPACE) {
                    if (runtime.state().workspaceView().focus()
                            == WorkbenchWorkspaceState.Focus.VIEWER
                            && (key.isUp() || key.isDown() || key.isLeft() || key.isRight()
                                    || key.isHome() || key.isEnd())) {
                        return runtime.updateState(state -> state.withWorkspaceView(
                                state.workspaceView().navigate(key)));
                    }
                    if (key.isKey(dev.tamboui.tui.event.KeyCode.TAB)) {
                        return runtime.updateState(state -> state.withWorkspaceView(
                                state.workspaceView().toggleFocus()));
                    }
                    if (key.isConfirm() && runtime.state().workspaceView().focus()
                            == WorkbenchWorkspaceState.Focus.FILES) {
                        return openSelectedWorkspaceFile(false);
                    }
                    if (key.isKey(dev.tamboui.tui.event.KeyCode.F4)) {
                        if (runtime.state().workspaceView().focus()
                                == WorkbenchWorkspaceState.Focus.VIEWER
                                && runtime.state().workspaceView().document().isPresent()) {
                            return runtime.updateState(state -> state.withWorkspaceView(
                                    state.workspaceView().edit()));
                        }
                        return openSelectedWorkspaceFile(true);
                    }
                    if (key.isCancel() && !runtime.state().workspaceView()
                            .equals(WorkbenchWorkspaceState.empty())) {
                        return runtime.updateState(state -> state.withWorkspaceView(
                                state.workspaceView().back()));
                    }
                }
                if (key.isCharIgnoreCase('c')) {
                    return openContextEntry();
                }
                if (key.isCharIgnoreCase('a')) {
                    return openAuthEntry();
                }
                if (key.isChar('0')) {
                    return runtime.updateState(state -> state.withOverlay(
                            new WorkbenchOverlayState.More(0)));
                }
                return runtime.updateState(state -> state.reduce(key, visibleRows()));
            }
            if (event instanceof MouseEvent mouse && mouse.isClick()) {
                var action = layout.actionAt(mouse.x(), mouse.y());
                if (action.isPresent()) {
                    return switch (action.orElseThrow()) {
                        case CONTEXT -> openContextEntry();
                        case AUTH -> openAuthEntry();
                        case MORE -> runtime.updateState(state -> state.withOverlay(
                                new WorkbenchOverlayState.More(0)));
                    };
                }
                var footerAction = layout.footerActionAt(mouse.x(), mouse.y());
                if (footerAction.isPresent()) {
                    return handleFooterAction(footerAction.orElseThrow(), runner);
                }
                var clickedTab = layout.tabAt(mouse.x(), mouse.y());
                if (clickedTab.isPresent()) {
                    return runtime.updateState(state -> state.select(clickedTab.orElseThrow()));
                }
                var clickedRow = layout.rowAt(mouse.x(), mouse.y());
                if (clickedRow.isPresent()) {
                    WorkbenchRenderer.RowHit row = clickedRow.orElseThrow();
                    return runtime.updateState(state -> {
                        WorkbenchState selected = state.selectRow(
                                row.tab(), row.rowIndex(), visibleRows());
                        return row.tab() == WorkbenchState.WorkbenchTab.WORKSPACE
                                ? selected.withWorkspaceView(selected.workspaceView().focusFiles())
                                : selected;
                    });
                }
            }
            return false;
        }

        WorkbenchRenderer.RenderLayout layout() {
            return layout;
        }

        private boolean handleFooterAction(WorkbenchRenderer.FooterAction action, TuiRunner runner) {
            return switch (action) {
                case CONTEXT -> openContextEntry();
                case AUTH -> openAuthEntry();
                case MORE -> runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.More(0)));
                case REFRESH -> {
                    if (refreshCoordinator == null) {
                        yield false;
                    }
                    refresh();
                    yield true;
                }
                case QUIT -> {
                    if (runner == null) {
                        yield false;
                    }
                    runner.quit();
                    yield true;
                }
                case SHELL -> {
                    if (shellPanel == null) {
                        yield false;
                    }
                    shellPanel.open();
                    yield true;
                }
                case BACK -> runtime.updateState(state -> state.selectedTab() == WorkbenchState.WorkbenchTab.WORKSPACE
                        && !state.workspaceView().equals(WorkbenchWorkspaceState.empty())
                        ? state.withWorkspaceView(state.workspaceView().back())
                        : state.reduce(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.ESCAPE), visibleRows()));
                case SORT -> runtime.updateState(state -> state.reduce(KeyEvent.ofChar('s'), visibleRows()));
                case OPEN -> openSelectedWorkspaceFile(false);
                case EDIT -> runtime.state().workspaceView().focus() == WorkbenchWorkspaceState.Focus.VIEWER
                        && runtime.state().workspaceView().document().isPresent()
                        ? runtime.updateState(state -> state.withWorkspaceView(state.workspaceView().edit()))
                        : openSelectedWorkspaceFile(true);
                case TOGGLE_FOCUS -> runtime.updateState(state -> state.withWorkspaceView(
                        state.workspaceView().toggleFocus()));
                case SAVE -> saveWorkspaceFile(false);
                case SAVE_AND_CLOSE -> saveWorkspaceFile(true);
                case CANCEL_EDIT -> requestWorkspaceEditCancel();
                case DISCARD -> runtime.updateState(state -> state.withWorkspaceView(
                        state.workspaceView().edit(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.ENTER))));
                case CANCEL_DISCARD -> runtime.updateState(state -> state.withWorkspaceView(
                        state.workspaceView().edit(KeyEvent.ofKey(dev.tamboui.tui.event.KeyCode.ESCAPE))));
                case CONFIRM -> confirmOverlay();
                case CANCEL_CONFIRM -> cancelConfirmOverlay();
            };
        }

        private boolean handleWorkspaceEditorEvent(Event event, TuiRunner runner) {
            if (event instanceof PasteEvent paste) {
                return runtime.updateState(state -> state.withWorkspaceView(
                        state.workspaceView().paste(paste.text())));
            }
            if (!(event instanceof KeyEvent key)) {
                return true;
            }
            if (key.isCtrlC()) {
                runner.quit();
                return true;
            }
            if (key.isCancel()) {
                return requestWorkspaceEditCancel();
            }
            if (key.hasCtrl() && key.isCharIgnoreCase('s')) {
                return saveWorkspaceFile(false);
            }
            if (key.isKey(dev.tamboui.tui.event.KeyCode.F5)) {
                return saveWorkspaceFile(true);
            }
            return runtime.updateState(state -> state.withWorkspaceView(
                    state.workspaceView().edit(key)));
        }

        private boolean openSelectedWorkspaceFile(boolean edit) {
            if (actionGateway == null) {
                return false;
            }
            Optional<WorkbenchArtifactRow> selected = selectedWorkspaceRow();
            if (selected.isEmpty() || selected.orElseThrow().local().isEmpty()) {
                return true;
            }
            Path relativePath = selected.orElseThrow().local().getFirst().relativePath();
            return switch (actionGateway.readWorkspaceFile(relativePath)) {
                case WorkbenchActionGateway.FileReadResult.Loaded loaded -> runtime.updateState(state -> {
                    WorkbenchWorkspaceState opened = state.workspaceView()
                            .open(loaded.relativePath(), loaded.content());
                    return state.withWorkspaceView(edit ? opened.edit() : opened);
                });
                case WorkbenchActionGateway.FileReadResult.Unavailable ignored -> true;
            };
        }

        private Optional<WorkbenchArtifactRow> selectedWorkspaceRow() {
            WorkbenchState state = runtime.state();
            if (state.snapshot().isEmpty()) {
                return Optional.empty();
            }
            List<WorkbenchArtifactRow> rows = WorkbenchRenderer.sorted(
                    state.snapshot().orElseThrow().workspace().rows(), state.workspaceTable());
            if (rows.isEmpty()) {
                return Optional.empty();
            }
            int selected = Math.clamp(state.workspaceTable().selectedIndex(), 0, rows.size() - 1);
            return Optional.of(rows.get(selected));
        }

        private boolean saveWorkspaceFile(boolean closeEditor) {
            if (actionGateway == null) {
                return false;
            }
            WorkbenchWorkspaceState.Document document = runtime.state().workspaceView()
                    .document().orElseThrow();
            return switch (actionGateway.writeWorkspaceFile(
                    document.relativePath(), document.content())) {
                case WorkbenchActionGateway.FileWriteResult.Saved ignored -> {
                    runtime.updateState(state -> state.withWorkspaceView(
                            state.workspaceView().saved(closeEditor)));
                    if (refreshCoordinator != null) {
                        refresh();
                    }
                    yield true;
                }
                case WorkbenchActionGateway.FileWriteResult.Unavailable ignored -> true;
            };
        }

        private boolean handleOverlayEvent(Event event, TuiRunner runner) {
            WorkbenchOverlayState overlay = runtime.state().overlay().orElseThrow();
            if (event instanceof PasteEvent paste) {
                return handleOverlayPaste(overlay, paste.text());
            }
            if (event instanceof MouseEvent mouse && mouse.isClick()) {
                Optional<WorkbenchRenderer.FooterAction> footerAction = layout.footerActionAt(mouse.x(), mouse.y());
                if (footerAction.isPresent()) {
                    return handleFooterAction(footerAction.orElseThrow(), runner);
                }
                OptionalInt clicked = layout.overlayIndexAt(mouse.x(), mouse.y());
                if (clicked.isEmpty()) {
                    return true;
                }
                int index = clicked.orElseThrow();
                return switch (overlay) {
                        case WorkbenchOverlayState.More ignored -> runtime.updateState(state ->
                            state.withOverlay(new WorkbenchOverlayState.More(Math.clamp(index, 0, 2))));
                        case WorkbenchOverlayState.ContextPicker picker -> runtime.updateState(state ->
                            state.withOverlay(picker.select(index)));
                        case WorkbenchOverlayState.ContextCreate ignored -> true;
                        case WorkbenchOverlayState.Confirm ignored -> true;
                        case WorkbenchOverlayState.Login ignored -> true;
                        case WorkbenchOverlayState.Actions actions -> runtime.updateState(state ->
                                state.withOverlay(actions.select(index)));
                    case WorkbenchOverlayState.Help ignored -> true;
                };
            }
            if (!(event instanceof KeyEvent key)) {
                return true;
            }
            if (key.isCancel()) {
                return overlay instanceof WorkbenchOverlayState.Confirm confirm
                        ? cancelConfirm(confirm)
                        : closeOverlay(overlay);
            }
            return switch (overlay) {
                case WorkbenchOverlayState.More more -> handleMoreKey(more, key);
                case WorkbenchOverlayState.ContextPicker picker -> handleContextKey(picker, key);
                case WorkbenchOverlayState.ContextCreate create -> handleContextCreateKey(create, key);
                case WorkbenchOverlayState.Confirm confirm -> handleConfirmKey(confirm, key);
                case WorkbenchOverlayState.Login login -> handleLoginKey(login, key);
                case WorkbenchOverlayState.Actions actions -> handleActionsKey(actions, key);
                case WorkbenchOverlayState.Help ignored -> true;
            };
        }

        private boolean openActions() {
            return runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Actions(
                    List.of(
                            WorkbenchOverlayState.Actions.Action.CONTEXT,
                            WorkbenchOverlayState.Actions.Action.AUTHENTICATION,
                            WorkbenchOverlayState.Actions.Action.REFRESH,
                            WorkbenchOverlayState.Actions.Action.SHELL),
                    0)));
        }

        private boolean handleActionsKey(WorkbenchOverlayState.Actions actions, KeyEvent key) {
            if (key.isUp() || key.isDown()) {
                return runtime.updateState(state -> state.withOverlay(actions.select(
                        actions.selectedIndex() + (key.isUp() ? -1 : 1))));
            }
            if (!(key.isSelect() || key.isConfirm())) {
                return true;
            }
            WorkbenchOverlayState.Actions.Action action = actions.actions().get(actions.selectedIndex());
            return switch (action) {
                case CONTEXT -> openContextEntry();
                case AUTHENTICATION -> openAuthEntry();
                case REFRESH -> {
                    runtime.updateState(WorkbenchState::closeOverlay);
                    if (refreshCoordinator != null) {
                        refresh();
                    }
                    yield true;
                }
                case SHELL -> {
                    runtime.updateState(WorkbenchState::closeOverlay);
                    if (shellPanel != null) {
                        shellPanel.open();
                    }
                    yield true;
                }
            };
        }

        private boolean handleMoreKey(WorkbenchOverlayState.More more, KeyEvent key) {
            if (key.isUp() || key.isDown()) {
                int selected = key.isUp() ? Math.max(0, more.selectedIndex() - 1)
                        : Math.min(2, more.selectedIndex() + 1);
                runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.More(selected)));
                return true;
            }
            if (key.isSelect() || key.isConfirm()) {
                if (more.selectedIndex() == 0) {
                    return openContextEntry();
                }
                if (more.selectedIndex() == 1) {
                    return openAuthEntry();
                }
                runtime.updateState(state -> state.withOverlay(WorkbenchOverlayState.Help.INSTANCE));
                return true;
            }
            return true;
        }

        private boolean handleContextKey(WorkbenchOverlayState.ContextPicker picker, KeyEvent key) {
            if (picker.pending()) {
                return true;
            }
            if (key.isUp() || key.isDown()) {
                int selected = picker.selectedIndex() + (key.isUp() ? -1 : 1);
                runtime.updateState(state -> state.withOverlay(picker.select(selected)));
                return true;
            }
            if (key.isChar('d') && picker.selectedIndex() < picker.contexts().size()) {
                String contextName = picker.contexts().get(picker.selectedIndex()).name();
                runtime.updateState(state -> state.withOverlay(contextDeleteConfirm(contextName, picker)));
                return true;
            }
            if ((key.isSelect() || key.isConfirm()) && picker.selectedIndex() >= 0) {
                if (picker.selectedIndex() == picker.contexts().size()) {
                    runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.ContextCreate(
                            WorkbenchOverlayState.ContextCreate.Stage.NAME,
                            "", "", true, false, Optional.empty(), Optional.of(picker))));
                } else {
                    selectContext(picker);
                }
            }
            return true;
        }

        private boolean handleConfirmKey(WorkbenchOverlayState.Confirm confirm, KeyEvent key) {
            if (confirm.pending()) {
                return true;
            }
            if (key.isSelect() || key.isConfirm()) {
                confirmOverlay();
            }
            return true;
        }

        private boolean confirmOverlay() {
            Optional<WorkbenchOverlayState> overlay = runtime.state().overlay();
            if (overlay.isEmpty() || !(overlay.orElseThrow() instanceof WorkbenchOverlayState.Confirm confirm)
                    || confirm.pending()) {
                return false;
            }
            return switch (confirm.intent()) {
                case WorkbenchOverlayState.Confirm.Intent.DeleteContext ignored -> {
                    submitContextDelete(confirm);
                    yield true;
                }
                case WorkbenchOverlayState.Confirm.Intent.DiscardChanges ignored -> runtime.updateState(state ->
                        state.withWorkspaceView(state.workspaceView().cancelEdit()).closeOverlay());
            };
        }

        private boolean cancelConfirmOverlay() {
            return runtime.state().overlay()
                    .filter(WorkbenchOverlayState.Confirm.class::isInstance)
                    .map(WorkbenchOverlayState.Confirm.class::cast)
                    .map(this::cancelConfirm)
                    .orElse(false);
        }

        private boolean cancelConfirm(WorkbenchOverlayState.Confirm confirm) {
            return confirm.pending() ? true : closeOverlay(confirm);
        }

        private boolean closeOverlay(WorkbenchOverlayState overlay) {
            clearOverlaySecret();
            return runtime.updateState(state -> previousOverlay(overlay)
                    .map(state::withOverlay)
                    .orElseGet(state::closeOverlay));
        }

        private boolean requestWorkspaceEditCancel() {
            return runtime.updateState(state -> state.workspaceView().document()
                    .filter(document -> document.editing() && document.dirty())
                    .map(ignored -> state.withOverlay(discardChangesConfirm()))
                    .orElseGet(() -> state.withWorkspaceView(state.workspaceView().cancelEdit())));
        }

        private static WorkbenchOverlayState.Confirm contextDeleteConfirm(
                String contextName, WorkbenchOverlayState.ContextPicker previous) {
            return new WorkbenchOverlayState.Confirm(
                    new WorkbenchOverlayState.Confirm.Intent.DeleteContext(contextName),
                    "Delete Context",
                    "Delete context " + contextName + "? Workspace bindings will be removed; auth cache is kept.",
                    false,
                    Optional.of(previous));
        }

        private static WorkbenchOverlayState.Confirm discardChangesConfirm() {
            return new WorkbenchOverlayState.Confirm(
                    WorkbenchOverlayState.Confirm.Intent.DiscardChanges.INSTANCE,
                    "Discard Changes?",
                    "Unsaved changes will be lost.",
                    false,
                    Optional.empty());
        }

        private boolean handleContextCreateKey(
                WorkbenchOverlayState.ContextCreate create, KeyEvent key) {
            if (create.pending()) {
                return true;
            }
            if (key.isUp() || key.isDown()) {
                WorkbenchOverlayState.ContextCreate.Stage[] stages =
                        WorkbenchOverlayState.ContextCreate.Stage.values();
                int current = create.stage().ordinal();
                int next = Math.floorMod(current + (key.isUp() ? -1 : 1), stages.length);
                return updateContextCreate(create, stages[next], create.name(), create.server(),
                        create.verifyTls(), Optional.empty());
            }
            if (key.isDeleteBackward()) {
                String name = create.name();
                String server = create.server();
                if (create.stage() == WorkbenchOverlayState.ContextCreate.Stage.NAME) {
                    name = deleteLastCodePoint(name);
                } else if (create.stage() == WorkbenchOverlayState.ContextCreate.Stage.SERVER) {
                    server = deleteLastCodePoint(server);
                }
                return updateContextCreate(create, create.stage(), name, server,
                        create.verifyTls(), Optional.empty());
            }
            if (key.isSelect() || key.isConfirm()) {
                return advanceContextCreate(create);
            }
            if (create.stage() == WorkbenchOverlayState.ContextCreate.Stage.VERIFY_TLS
                    && key.code() == dev.tamboui.tui.event.KeyCode.CHAR
                    && (key.isCharIgnoreCase('y') || key.isCharIgnoreCase('n') || key.isChar(' '))) {
                boolean verifyTls = key.isCharIgnoreCase('y')
                        || key.isChar(' ') && !create.verifyTls();
                return updateContextCreate(create, create.stage(), create.name(), create.server(),
                        verifyTls, Optional.empty());
            }
            if (key.code() == dev.tamboui.tui.event.KeyCode.CHAR
                    && create.stage() != WorkbenchOverlayState.ContextCreate.Stage.VERIFY_TLS) {
                return appendContextCreateText(create, key.string());
            }
            return true;
        }

        private boolean handleLoginKey(WorkbenchOverlayState.Login login, KeyEvent key) {
            if (login.pending()) {
                return true;
            }
            if (key.isUp() || key.isDown()) {
                WorkbenchOverlayState.Login.Stage[] stages = transientLogin(login)
                        ? WorkbenchOverlayState.Login.Stage.values()
                        : new WorkbenchOverlayState.Login.Stage[]{
                            WorkbenchOverlayState.Login.Stage.USERNAME,
                            WorkbenchOverlayState.Login.Stage.PASSWORD};
                int current = 0;
                for (int index = 0; index < stages.length; index++) {
                    if (stages[index] == login.stage()) {
                        current = index;
                        break;
                    }
                }
                int next = Math.floorMod(current + (key.isUp() ? -1 : 1), stages.length);
                return updateLogin(login, stages[next], login.server(), login.username(), Optional.empty());
            }
            if (key.isDeleteBackward()) {
                if (login.stage() == WorkbenchOverlayState.Login.Stage.SERVER) {
                    String server = deleteLastCodePoint(login.server());
                    runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                            login.contextName(), login.stage(), server, login.username(),
                            login.password(), false, Optional.empty(), login.previous())));
                } else if (login.stage() == WorkbenchOverlayState.Login.Stage.USERNAME) {
                    String username = deleteLastCodePoint(login.username());
                    runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                            login.contextName(), login.stage(), login.server(), username,
                            login.password(), false, Optional.empty(), login.previous())));
                } else {
                    login.password().deleteLast();
                    runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                            login.contextName(), login.stage(), login.server(), login.username(),
                            login.password(), false, Optional.empty(), login.previous())));
                }
                return true;
            }
            if (key.isSelect() || key.isConfirm()) {
                if (login.stage() == WorkbenchOverlayState.Login.Stage.SERVER) {
                    if (validServer(login.server())) {
                        runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                                login.server(), WorkbenchOverlayState.Login.Stage.USERNAME,
                                login.server(), login.username(), login.password(), false,
                                Optional.empty(), login.previous())));
                    } else {
                        runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                                login.contextName(), login.stage(), login.server(), login.username(),
                                login.password(), false, Optional.of("Enter an absolute server URL"),
                                login.previous())));
                    }
                } else if (login.stage() == WorkbenchOverlayState.Login.Stage.USERNAME) {
                    if (!login.username().isBlank()) {
                        runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                                login.contextName(), WorkbenchOverlayState.Login.Stage.PASSWORD,
                                login.server(), login.username(), login.password(), false,
                                Optional.empty(), login.previous())));
                    }
                } else if (login.password().length() > 0) {
                    submitLogin(login);
                }
                return true;
            }
            if (key.code() == dev.tamboui.tui.event.KeyCode.CHAR) {
                return appendLoginText(login, key.string());
            }
            return true;
        }

        private boolean handleOverlayPaste(WorkbenchOverlayState overlay, String text) {
            if (overlay instanceof WorkbenchOverlayState.Login login && !login.pending()) {
                return appendLoginText(login, text);
            }
            if (overlay instanceof WorkbenchOverlayState.ContextCreate create && !create.pending()
                    && create.stage() != WorkbenchOverlayState.ContextCreate.Stage.VERIFY_TLS) {
                return appendContextCreateText(create, text);
            }
            return true;
        }

        private boolean appendContextCreateText(
                WorkbenchOverlayState.ContextCreate create, String text) {
            StringBuilder value = new StringBuilder(
                    create.stage() == WorkbenchOverlayState.ContextCreate.Stage.NAME
                            ? create.name() : create.server());
            text.codePoints()
                    .filter(codePoint -> !Character.isISOControl(codePoint))
                    .forEach(value::appendCodePoint);
            return create.stage() == WorkbenchOverlayState.ContextCreate.Stage.NAME
                    ? updateContextCreate(create, create.stage(), value.toString(), create.server(),
                            create.verifyTls(), Optional.empty())
                    : updateContextCreate(create, create.stage(), create.name(), value.toString(),
                            create.verifyTls(), Optional.empty());
        }

        private boolean advanceContextCreate(WorkbenchOverlayState.ContextCreate create) {
            if (create.stage() == WorkbenchOverlayState.ContextCreate.Stage.NAME) {
                if (create.name().isBlank()) {
                    return updateContextCreate(create, create.stage(), create.name(), create.server(),
                            create.verifyTls(), Optional.of("Context name is required"));
                }
                return updateContextCreate(create, WorkbenchOverlayState.ContextCreate.Stage.SERVER,
                        create.name(), create.server(), create.verifyTls(), Optional.empty());
            }
            if (create.stage() == WorkbenchOverlayState.ContextCreate.Stage.SERVER) {
                try {
                    URI server = URI.create(create.server());
                    if (server.getScheme() == null || server.getHost() == null) {
                        throw new IllegalArgumentException("absolute server URL required");
                    }
                } catch (IllegalArgumentException invalid) {
                    return updateContextCreate(create, create.stage(), create.name(), create.server(),
                            create.verifyTls(), Optional.of("Enter an absolute server URL"));
                }
                return updateContextCreate(create, WorkbenchOverlayState.ContextCreate.Stage.VERIFY_TLS,
                        create.name(), create.server(), create.verifyTls(), Optional.empty());
            }
            submitContextCreate(create);
            return true;
        }

        private boolean updateContextCreate(
                WorkbenchOverlayState.ContextCreate current,
                WorkbenchOverlayState.ContextCreate.Stage stage,
                String name,
                String server,
                boolean verifyTls,
                Optional<String> message) {
            return runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.ContextCreate(
                    stage, name, server, verifyTls, current.pending(), message, current.previous())));
        }

        private void submitContextCreate(WorkbenchOverlayState.ContextCreate create) {
            if (actionCoordinator == null) {
                return;
            }
            WorkbenchOverlayState.ContextCreate pending = new WorkbenchOverlayState.ContextCreate(
                    create.stage(), create.name(), create.server(), create.verifyTls(), true,
                    Optional.empty(), create.previous());
            runtime.updateState(state -> state.withOverlay(pending));
            actionCoordinator.submit(
                    () -> actionGateway.createContext(
                            create.name(), URI.create(create.server()), create.verifyTls()),
                    failure -> new WorkbenchActionGateway.ContextResult.Unavailable(),
                    this::completeContextSelection);
        }

        private boolean appendLoginText(WorkbenchOverlayState.Login login, String text) {
            if (login.stage() != WorkbenchOverlayState.Login.Stage.PASSWORD) {
                StringBuilder value = new StringBuilder(
                        login.stage() == WorkbenchOverlayState.Login.Stage.SERVER
                                ? login.server() : login.username());
                text.codePoints()
                        .filter(codePoint -> !Character.isISOControl(codePoint))
                        .forEach(value::appendCodePoint);
                if (login.stage() == WorkbenchOverlayState.Login.Stage.SERVER) {
                    runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                            login.contextName(), login.stage(), value.toString(), login.username(),
                            login.password(), false, Optional.empty(), login.previous())));
                } else {
                    runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                            login.contextName(), login.stage(), login.server(), value.toString(),
                            login.password(), false, Optional.empty(), login.previous())));
                }
            } else {
                login.password().append(text);
                runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                        login.contextName(), login.stage(), login.server(), login.username(), login.password(),
                        false, Optional.empty(), login.previous())));
            }
            return true;
        }

        private boolean openContextEntry() {
            if (actionGateway == null) {
                return false;
            }
            List<WorkbenchActionGateway.ContextOption> contexts = actionGateway.contexts();
            return runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.ContextPicker(
                    contexts,
                    0,
                    false,
                    Optional.empty(),
                    state.overlay().filter(WorkbenchOverlayState.More.class::isInstance))));
        }

        private boolean openAuthEntry() {
            if (actionGateway == null) {
                return false;
            }
            Optional<WorkbenchSessionSnapshot> current = runtime.state().snapshot()
                    .map(WorkbenchSnapshot::session);
            WorkbenchSessionSnapshot session = current.orElseGet(WorkbenchSessionSnapshot::empty);
            if (session.connection() != WorkbenchConnection.CONNECTED) {
                return runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                        "temporary server",
                        WorkbenchOverlayState.Login.Stage.SERVER,
                        "",
                        "",
                        new SecretBuffer(),
                        false,
                        Optional.empty(),
                        state.overlay().filter(WorkbenchOverlayState.More.class::isInstance))));
            }
            if (session.authentication() != WorkbenchAuthentication.SIGNED_OUT) {
                return false;
            }
            String target = session.contextName()
                    .or(() -> session.landingNode().map(Object::toString))
                    .orElse("current server");
            return runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                    target,
                    WorkbenchOverlayState.Login.Stage.USERNAME,
                    "",
                    "",
                    new SecretBuffer(),
                    false,
                    Optional.empty(),
                    state.overlay().filter(WorkbenchOverlayState.More.class::isInstance))));
        }

        private void selectContext(WorkbenchOverlayState.ContextPicker picker) {
            if (actionCoordinator == null) {
                return;
            }
            String contextName = picker.contexts().get(picker.selectedIndex()).name();
            runtime.updateState(state -> state.withOverlay(picker.asPending()));
            actionCoordinator.submit(
                    () -> actionGateway.selectContext(contextName),
                    failure -> new WorkbenchActionGateway.ContextResult.Unavailable(),
                    this::completeContextSelection);
        }

        private void completeContextSelection(WorkbenchActionGateway.ContextResult result) {
            switch (result) {
                case WorkbenchActionGateway.ContextResult.Ready ready -> {
                    if (ready.signedIn()) {
                        refreshAfterActivation();
                    } else {
                        Optional<WorkbenchOverlayState> previous = runtime.state().overlay()
                                .flatMap(Session::previousOverlay);
                        openContextLogin(ready.contextName(), previous);
                    }
                }
                case WorkbenchActionGateway.ContextResult.Offline offline -> runtime.updateState(state ->
                        state.withOverlay(contextMessage("Context is offline: " + offline.contextName())));
                case WorkbenchActionGateway.ContextResult.Unavailable ignored -> runtime.updateState(state ->
                        state.withOverlay(contextMessage("Context could not be selected")));
            }
        }

        private void openContextLogin(String contextName, Optional<WorkbenchOverlayState> previous) {
            runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                    contextName,
                    WorkbenchOverlayState.Login.Stage.USERNAME,
                    "",
                    "",
                    new SecretBuffer(),
                    false,
                    Optional.empty(),
                    previous)));
        }

        private WorkbenchOverlayState.ContextPicker contextMessage(String message) {
            List<WorkbenchActionGateway.ContextOption> contexts = actionGateway.contexts();
            return new WorkbenchOverlayState.ContextPicker(
                    contexts, 0, false, Optional.of(message));
        }

        private void submitLogin(WorkbenchOverlayState.Login login) {
            if (actionCoordinator == null) {
                return;
            }
            runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                    login.contextName(), login.stage(), login.server(), login.username(),
                    login.password(), true, Optional.empty(), login.previous())));
            Optional<URI> server = login.server().isBlank()
                    ? Optional.empty() : Optional.of(URI.create(login.server()));
            actionCoordinator.submit(
                    () -> actionGateway.login(
                            new WorkbenchActionGateway.LoginRequest(server, login.username()), login.password()),
                    failure -> new WorkbenchActionGateway.LoginResult.Unavailable(),
                    result -> completeLogin(
                            login.contextName(), login.server(), login.username(), login.previous(), result));
        }

        private void submitContextDelete(WorkbenchOverlayState.Confirm confirm) {
            if (actionCoordinator == null) {
                return;
            }
            WorkbenchOverlayState.Confirm.Intent.DeleteContext delete =
                    (WorkbenchOverlayState.Confirm.Intent.DeleteContext) confirm.intent();
            runtime.updateState(state -> state.withOverlay(confirm.asPending()));
            actionCoordinator.submit(
                    () -> actionGateway.deleteContext(delete.contextName()),
                    failure -> new WorkbenchActionGateway.ContextDeleteResult.Unavailable(),
                    this::completeContextDelete);
        }

        private void completeContextDelete(WorkbenchActionGateway.ContextDeleteResult result) {
            switch (result) {
                case WorkbenchActionGateway.ContextDeleteResult.Deleted ignored -> {
                    refreshCoordinator.advanceContext();
                    refresh();
                    runtime.updateState(state -> state.withOverlay(contextMessage("Context deleted")));
                }
                case WorkbenchActionGateway.ContextDeleteResult.Unavailable ignored -> runtime.updateState(state ->
                        state.withOverlay(contextMessage("Context could not be deleted")));
            }
        }

        private void completeLogin(
                String contextName,
                String server,
                String username,
                Optional<WorkbenchOverlayState> previous,
                WorkbenchActionGateway.LoginResult result) {
            switch (result) {
                case WorkbenchActionGateway.LoginResult.SignedIn ignored -> refreshAfterActivation();
                case WorkbenchActionGateway.LoginResult.Rejected rejected -> showLoginFailure(
                        contextName, server, username, previous, "Sign in rejected: " + rejected.code());
                case WorkbenchActionGateway.LoginResult.Unreachable ignored -> showLoginFailure(
                        contextName, server, username, previous, "Server is unreachable");
                case WorkbenchActionGateway.LoginResult.Unavailable ignored -> showLoginFailure(
                        contextName, server, username, previous, "Sign in is unavailable");
            }
        }

        private void showLoginFailure(
                String contextName,
                String server,
                String username,
                Optional<WorkbenchOverlayState> previous,
                String message) {
            runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                    contextName,
                    WorkbenchOverlayState.Login.Stage.PASSWORD,
                    server,
                    username,
                    new SecretBuffer(),
                    false,
                    Optional.of(message),
                    previous)));
        }

        private boolean updateLogin(
                WorkbenchOverlayState.Login login,
                WorkbenchOverlayState.Login.Stage stage,
                String server,
                String username,
                Optional<String> message) {
            return runtime.updateState(state -> state.withOverlay(new WorkbenchOverlayState.Login(
                    login.contextName(), stage, server, username, login.password(), false,
                    message, login.previous())));
        }

        private static boolean transientLogin(WorkbenchOverlayState.Login login) {
            return login.stage() == WorkbenchOverlayState.Login.Stage.SERVER || !login.server().isBlank();
        }

        private static Optional<WorkbenchOverlayState> previousOverlay(WorkbenchOverlayState overlay) {
            return switch (overlay) {
                case WorkbenchOverlayState.ContextPicker picker -> picker.previous();
                case WorkbenchOverlayState.ContextCreate create -> create.previous();
                case WorkbenchOverlayState.Confirm confirm -> confirm.previous();
                case WorkbenchOverlayState.Login login -> login.previous();
                case WorkbenchOverlayState.Actions ignored -> Optional.empty();
                case WorkbenchOverlayState.Help ignored -> Optional.of(new WorkbenchOverlayState.More(2));
                case WorkbenchOverlayState.More ignored -> Optional.empty();
            };
        }

        private static boolean validServer(String value) {
            try {
                URI server = URI.create(value);
                return server.getScheme() != null && server.getHost() != null;
            } catch (IllegalArgumentException invalid) {
                return false;
            }
        }

        private void refreshAfterActivation() {
            runtime.updateState(WorkbenchState::closeOverlay);
            refreshCoordinator.advanceContext();
            refresh();
        }

        private void clearOverlaySecret() {
            runtime.state().overlay()
                    .filter(WorkbenchOverlayState.Login.class::isInstance)
                    .map(WorkbenchOverlayState.Login.class::cast)
                    .ifPresent(login -> login.password().close());
        }

        private static String deleteLastCodePoint(String value) {
            if (value.isEmpty()) {
                return value;
            }
            return value.substring(0, value.offsetByCodePoints(value.length(), -1));
        }

        void refresh() {
            if (refreshCoordinator == null || dataSource == null) {
                throw new IllegalStateException("This workbench session has no refresh data source");
            }
            RefreshRequest request = refreshCoordinator.refresh((generation, sequence, token) ->
                    RefreshResult.success(dataSource.load(generation, sequence, token)));
            runtime.expectSnapshot(new WorkbenchSnapshot(
                    request.contextGeneration(), request.requestSequence()));
        }

        void expectSnapshot(WorkbenchSnapshot snapshot) {
            runtime.expectSnapshot(snapshot);
        }

        void publishSnapshot(WorkbenchSnapshot snapshot) {
            runtime.publishSnapshot(snapshot);
        }

        void render(Frame frame) {
            if (shellPanel == null || !shellPanel.isOpen() || runtime.state().overlay().isPresent()
                    || frame.area().height() < 24) {
                layout = WorkbenchRenderer.render(frame, runtime.state());
                return;
            }
            int panelHeight = shellPanel.heightFor(frame.area());
            int mainHeight = frame.area().height() - panelHeight - 1;
            Rect mainArea = new Rect(frame.area().x(), frame.area().y(), frame.area().width(), mainHeight);
            layout = WorkbenchRenderer.render(frame, runtime.state(), WorkbenchTheme.dark(), mainArea, false, false);
            Rect shellArea = new Rect(frame.area().x(), mainArea.bottom(), frame.area().width(), panelHeight);
            shellPanel.render(frame, shellArea, WorkbenchTheme.dark());
            shellPanel.renderFooter(frame, new Rect(frame.area().x(), frame.area().bottom() - 1,
                    frame.area().width(), 1), WorkbenchTheme.dark());
        }

        @Override
        public void close() {
            clearOverlaySecret();
            if (shellPanel != null) {
                shellPanel.close();
            }
            if (actionCoordinator != null) {
                actionCoordinator.close();
            }
            if (refreshCoordinator != null) {
                refreshCoordinator.close();
            }
        }

        void publishRefreshResult(RefreshResult result) {
            WorkbenchSnapshot baseline = currentBaseline(result.contextGeneration());
            WorkbenchSnapshot snapshot = switch (result.outcome()) {
                case RefreshResult.Success success -> remember(success.snapshot());
                case RefreshResult.Empty ignored -> unavailableSnapshot(
                        result, emptyRemoteState(baseline), baseline);
                case RefreshResult.Offline ignored -> unavailableSnapshot(
                        result, new WorkbenchRemoteState.Offline(), baseline);
                case RefreshResult.Diagnostic diagnostic -> unavailableSnapshot(
                        result, new WorkbenchRemoteState.Diagnostic(
                                diagnostic.code(), diagnostic.arguments()), baseline);
            };
            runtime.publishSnapshot(snapshot);
        }

        private WorkbenchSnapshot remember(WorkbenchSnapshot snapshot) {
            lastSuccessfulSnapshot = snapshot;
            return snapshot;
        }

        private WorkbenchSnapshot currentBaseline(long contextGeneration) {
            WorkbenchSnapshot snapshot = lastSuccessfulSnapshot;
            return snapshot != null && snapshot.contextGeneration() == contextGeneration
                    ? snapshot
                    : null;
        }

        private static WorkbenchRemoteState emptyRemoteState(WorkbenchSnapshot baseline) {
            return baseline != null && baseline.session().connection() != WorkbenchConnection.NO_CONTEXT
                    ? new WorkbenchRemoteState.Available(0)
                    : new WorkbenchRemoteState.NotConfigured();
        }

        private static WorkbenchSnapshot unavailableSnapshot(
                RefreshResult result,
                WorkbenchRemoteState remoteState,
                WorkbenchSnapshot baseline) {
            WorkbenchSessionSnapshot session = baseline == null
                    ? WorkbenchSessionSnapshot.empty()
                    : baseline.session();
            List<WorkbenchArtifactRow> rows = baseline == null
                    ? List.of()
                    : localRows(baseline.workspace().rows(), remoteState);
            return new WorkbenchSnapshot(
                    result.contextGeneration(),
                    result.requestSequence(),
                    session,
                    overview(rows, remoteState),
                    new WorkbenchWorkspaceSnapshot(remoteState, rows),
                    new WorkbenchResourceListSnapshot("source", remoteState, List.of()),
                    new WorkbenchResourceListSnapshot("pipeline", remoteState, List.of()));
        }

        private static List<WorkbenchArtifactRow> localRows(
                List<WorkbenchArtifactRow> baselineRows,
                WorkbenchRemoteState remoteState) {
            boolean remoteAvailable = remoteState instanceof WorkbenchRemoteState.Available;
            return baselineRows.stream()
                    .filter(row -> !row.local().isEmpty())
                    .map(row -> new WorkbenchArtifactRow(
                            row.key(),
                            row.local(),
                            List.of(),
                            invalidLocal(row)
                                    ? WorkbenchAlignment.INVALID_LOCAL
                                    : remoteAvailable
                                            ? WorkbenchAlignment.LOCAL_ONLY
                                            : WorkbenchAlignment.UNKNOWN))
                    .toList();
        }

        private static boolean invalidLocal(WorkbenchArtifactRow row) {
            return row.local().size() > 1 || row.local().stream().anyMatch(local -> !local.valid());
        }

        private static WorkbenchOverviewSnapshot overview(
                List<WorkbenchArtifactRow> rows,
                WorkbenchRemoteState remoteState) {
            boolean remoteAvailable = remoteState instanceof WorkbenchRemoteState.Available;
            List<WorkbenchKindCount> counts = WorkbenchProjection.VISIBLE_KINDS.stream()
                    .map(kind -> new WorkbenchKindCount(
                            kind,
                            (int) rows.stream()
                                    .filter(row -> row.key().kind().equals(kind))
                                    .count(),
                            remoteAvailable ? OptionalInt.of(0) : OptionalInt.empty()))
                    .toList();
            return new WorkbenchOverviewSnapshot(counts, new WorkbenchAlignmentCounts(
                    count(rows, WorkbenchAlignment.LOCAL_ONLY),
                    0,
                    0,
                    0,
                    count(rows, WorkbenchAlignment.INVALID_LOCAL),
                    count(rows, WorkbenchAlignment.UNKNOWN)));
        }

        private static int count(
                List<WorkbenchArtifactRow> rows,
                WorkbenchAlignment alignment) {
            return (int) rows.stream().filter(row -> row.alignment() == alignment).count();
        }

        private int visibleRows() {
            return Math.max(1, layout.visibleRowCapacity());
        }
    }
}
