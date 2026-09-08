package io.tapstate.cli;

import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.MouseEvent;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
                Session workbench = new Session(runner, session.workbenchDataSource());
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
        private final RefreshCoordinator refreshCoordinator;
        private volatile WorkbenchSnapshot lastSuccessfulSnapshot;
        private WorkbenchRenderer.RenderLayout layout = WorkbenchRenderer.RenderLayout.forTooSmallFrame();

        private Session(TuiRunner runner, WorkbenchDataSource dataSource) {
            this(new WorkbenchRuntime(
                            WorkbenchState.initial(),
                            runner::runLater,
                            runner::isRenderThread,
                            runner::dispatch),
                    dataSource);
        }

        Session(WorkbenchRuntime runtime) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.dataSource = null;
            this.refreshCoordinator = null;
        }

        Session(WorkbenchRuntime runtime, WorkbenchDataSource dataSource) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
            this.refreshCoordinator = new RefreshCoordinator(this::publishRefreshResult);
        }

        boolean handleEvent(Event event, TuiRunner runner) {
            if (event == WorkbenchRedrawEvent.INSTANCE) {
                return true;
            }
            if (event instanceof KeyEvent key) {
                if (key.isCharIgnoreCase('q') || key.isCtrlC()) {
                    runner.quit();
                    return true;
                }
                if (key.isCharIgnoreCase('r') && refreshCoordinator != null) {
                    refresh();
                    return true;
                }
                return runtime.updateState(state -> state.reduce(key, visibleRows()));
            }
            if (event instanceof MouseEvent mouse && mouse.isClick()) {
                var clickedTab = layout.tabAt(mouse.x(), mouse.y());
                if (clickedTab.isPresent()) {
                    return runtime.updateState(state -> state.select(clickedTab.orElseThrow()));
                }
                var clickedRow = layout.rowAt(mouse.x(), mouse.y());
                if (clickedRow.isPresent()) {
                    WorkbenchRenderer.RowHit row = clickedRow.orElseThrow();
                    return runtime.updateState(state -> state.selectRow(
                            row.tab(), row.rowIndex(), visibleRows()));
                }
            }
            return false;
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
            layout = WorkbenchRenderer.render(frame, runtime.state());
        }

        @Override
        public void close() {
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
                    resourceList("source", remoteState, rows),
                    resourceList("pipeline", remoteState, rows));
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
            List<String> kinds = new ArrayList<>(WorkspaceScan.KINDS);
            rows.stream()
                    .map(row -> row.key().kind())
                    .filter(kind -> !WorkspaceScan.KINDS.contains(kind))
                    .distinct()
                    .sorted()
                    .forEach(kinds::add);
            boolean remoteAvailable = remoteState instanceof WorkbenchRemoteState.Available;
            List<WorkbenchKindCount> counts = kinds.stream()
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

        private static WorkbenchResourceListSnapshot resourceList(
                String kind,
                WorkbenchRemoteState remoteState,
                List<WorkbenchArtifactRow> rows) {
            return new WorkbenchResourceListSnapshot(
                    kind,
                    remoteState,
                    rows.stream().filter(row -> row.key().kind().equals(kind)).toList());
        }

        private int visibleRows() {
            return Math.max(1, layout.visibleRowCapacity());
        }
    }
}
