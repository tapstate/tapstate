package io.tapstate.cli;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.MouseButton;
import dev.tamboui.tui.event.MouseEvent;
import dev.tamboui.tui.event.PasteEvent;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchRefreshIntegrationTest {

    @Test
    void refreshLoadsOnTheWorkerAndPublishesOnlyThroughTheRenderThreadMailbox() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        Thread owner = Thread.currentThread();
        List<Event> dispatched = new ArrayList<>();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> Thread.currentThread() == owner, dispatched::add);
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        AtomicReference<Thread> loadThread = new AtomicReference<>();

        try (Workbench.Session session = new Workbench.Session(runtime, (generation, sequence, token) -> {
            loadThread.set(Thread.currentThread());
            loadStarted.countDown();
            releaseLoad.await();
            return snapshot(generation, sequence, "orders");
        })) {
            session.refresh();
            await(loadStarted);

            assertThat(runtime.state().expectedSnapshot())
                    .map(WorkbenchSnapshot::identity)
                    .contains(new WorkbenchSnapshot.Identity(0, 1));
            assertThat(runtime.state().snapshot()).isEmpty();
            assertThat(dispatched).isEmpty();

            releaseLoad.countDown();
            Runnable drain = scheduler.awaitNext();

            assertThat(runtime.state().snapshot()).isEmpty();
            assertThat(dispatched).isEmpty();
            drain.run();

            assertThat(loadThread.get().getName()).isEqualTo("tapstate-refresh");
            assertThat(runtime.state().snapshot())
                    .map(WorkbenchSnapshot::identity)
                    .contains(new WorkbenchSnapshot.Identity(0, 1));
            assertThat(dispatched).containsExactly(WorkbenchRedrawEvent.INSTANCE);

            runtime.updateState(state -> state.select(WorkbenchState.WorkbenchTab.PIPELINES));
            Buffer buffer = Buffer.empty(new Rect(0, 0, 88, 24));
            session.render(Frame.forTesting(buffer));
            assertThat(textOf(buffer)).contains("orders").contains("remote only");
            assertThat(scheduler.pendingCount()).isZero();
        }
    }

    @Test
    void refreshKeyReplacesAnInFlightRequestAndSuppressesItsLateResult() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        Thread owner = Thread.currentThread();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> Thread.currentThread() == owner, event -> {
                });
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        AtomicReference<RefreshRequest.CancellationToken> firstToken = new AtomicReference<>();

        try (Workbench.Session session = new Workbench.Session(runtime, (generation, sequence, token) -> {
            if (calls.incrementAndGet() == 1) {
                firstToken.set(token);
                firstStarted.countDown();
                awaitIgnoringInterrupt(releaseFirst);
                return snapshot(generation, sequence, "stale");
            }
            secondStarted.countDown();
            return snapshot(generation, sequence, "current");
        })) {
            session.refresh();
            await(firstStarted);

            assertThat(session.handleEvent(KeyEvent.ofChar('r'), null)).isTrue();
            assertThat(firstToken.get().isCancelled()).isTrue();
            assertThat(runtime.state().expectedSnapshot())
                    .map(WorkbenchSnapshot::identity)
                    .contains(new WorkbenchSnapshot.Identity(0, 2));

            releaseFirst.countDown();
            await(secondStarted);
            scheduler.awaitNext().run();

            WorkbenchSnapshot accepted = runtime.state().snapshot().orElseThrow();
            assertThat(accepted.identity()).isEqualTo(new WorkbenchSnapshot.Identity(0, 2));
            assertThat(accepted.pipelines().rows())
                    .extracting(row -> row.key().id())
                    .containsExactly("current");
            assertThat(scheduler.pendingCount()).isZero();
        }
    }

    @Test
    void rendererHitMapDrivesTabSelection() {
        ImmediateScheduler scheduler = new ImmediateScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });
        Workbench.Session session = new Workbench.Session(runtime);
        WorkbenchSnapshot snapshot = snapshot(0, 1, "orders", "customers");
        runtime.expectSnapshot(snapshot);
        runtime.publishSnapshot(snapshot);

        session.render(Frame.forTesting(Buffer.empty(new Rect(0, 0, 88, 24))));
        assertThat(session.handleEvent(MouseEvent.press(MouseButton.LEFT, 14, 5), null)).isTrue();
        assertThat(runtime.state().selectedTab()).isEqualTo(WorkbenchState.WorkbenchTab.WORKSPACE);

    }

    @Test
    void overlayOwnsInputUntilItIsClosed() {
        ImmediateScheduler scheduler = new ImmediateScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });
        Workbench.Session session = new Workbench.Session(runtime);

        assertThat(session.handleEvent(KeyEvent.ofChar('0'), null)).isTrue();
        assertThat(runtime.state().overlay()).contains(new WorkbenchOverlayState.More(0));
        assertThat(session.handleEvent(KeyEvent.ofChar('2'), null)).isTrue();
        assertThat(runtime.state().selectedTab()).isEqualTo(WorkbenchState.WorkbenchTab.OVERVIEW);

        assertThat(session.handleEvent(KeyEvent.ofKey(KeyCode.ESCAPE), null)).isTrue();
        assertThat(runtime.state().overlay()).isEmpty();
        assertThat(session.handleEvent(KeyEvent.ofChar('2'), null)).isTrue();
        assertThat(runtime.state().selectedTab()).isEqualTo(WorkbenchState.WorkbenchTab.WORKSPACE);
    }

    @Test
    void signedOutSessionLogsInWithoutExposingOrRetainingThePassword() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        WorkbenchSnapshot signedOut = signedOutSnapshot(0, 1);
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial().expectSnapshot(signedOut).acceptSnapshot(signedOut),
                scheduler,
                () -> true,
                event -> {
                });
        AtomicReference<String> receivedPassword = new AtomicReference<>();
        AtomicReference<SecretBuffer> submittedBuffer = new AtomicReference<>();
        CountDownLatch loginCalled = new CountDownLatch(1);
        WorkbenchActionGateway gateway = new WorkbenchActionGateway() {
            @Override
            public List<ContextOption> contexts() {
                return List.of(new ContextOption("dev", true));
            }

            @Override
            public ContextResult selectContext(String name) {
                return new ContextResult.Ready(name, false);
            }

            @Override
            public LoginResult login(String username, SecretBuffer password) {
                submittedBuffer.set(password);
                receivedPassword.set(password.consume(value -> value));
                loginCalled.countDown();
                return new LoginResult.SignedIn(username);
            }
        };

        try (Workbench.Session session = new Workbench.Session(
                runtime,
                (generation, sequence, token) -> snapshot(generation, sequence, "orders"),
                gateway)) {
            assertThat(session.handleEvent(KeyEvent.ofChar('c'), null)).isTrue();
            for (char character : "alice".toCharArray()) {
                assertThat(session.handleEvent(KeyEvent.ofChar(character), null)).isTrue();
            }
            assertThat(session.handleEvent(KeyEvent.ofKey(KeyCode.ENTER), null)).isTrue();
            assertThat(session.handleEvent(new PasteEvent("p@ssword"), null)).isTrue();

            Buffer buffer = Buffer.empty(new Rect(0, 0, 88, 24));
            session.render(Frame.forTesting(buffer));
            assertThat(textOf(buffer)).contains("Username: alice", "Password: ********");
            assertThat(textOf(buffer)).doesNotContain("p@ssword");
            assertThat(runtime.state().toString()).doesNotContain("p@ssword");

            assertThat(session.handleEvent(KeyEvent.ofKey(KeyCode.ENTER), null)).isTrue();
            await(loginCalled);
            scheduler.awaitNext().run();

            assertThat(receivedPassword).hasValue("p@ssword");
            assertThat(submittedBuffer.get().cleared()).isTrue();
            assertThat(runtime.state().overlay()).isEmpty();
            assertThat(runtime.state().expectedSnapshot())
                    .map(WorkbenchSnapshot::identity)
                    .contains(new WorkbenchSnapshot.Identity(1, 1));

            scheduler.awaitNext().run();
            assertThat(runtime.state().snapshot())
                    .map(WorkbenchSnapshot::identity)
                    .contains(new WorkbenchSnapshot.Identity(1, 1));
        }
    }

    @Test
    void failedLoadBecomesASanitizedDiagnosticSnapshot() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });

        try (Workbench.Session session = new Workbench.Session(runtime, (generation, sequence, token) -> {
            throw new IllegalStateException("secret failure detail");
        })) {
            session.refresh();
            scheduler.awaitNext().run();

            WorkbenchSnapshot accepted = runtime.state().snapshot().orElseThrow();
            assertThat(accepted.workspace().remoteState())
                    .isEqualTo(new WorkbenchRemoteState.Diagnostic(
                            CliError.WORKBENCH_UNAVAILABLE, java.util.Map.of()));
            assertThat(accepted.toString()).doesNotContain("secret failure detail");

            Buffer buffer = Buffer.empty(new Rect(0, 0, 88, 24));
            session.render(Frame.forTesting(buffer));
            assertThat(textOf(buffer))
                    .contains("Diagnostic: " + CliError.WORKBENCH_UNAVAILABLE.code())
                    .doesNotContain("secret failure detail");
        }
    }

    @Test
    void failedSameContextRefreshKeepsTheLastValidSessionAndLocalWorkspace() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });
        AtomicInteger calls = new AtomicInteger();
        WorkbenchSnapshot successful = connectedLocalSnapshot(0, 1);

        try (Workbench.Session session = new Workbench.Session(runtime, (generation, sequence, token) -> {
            if (calls.incrementAndGet() == 1) {
                return successful;
            }
            throw new IllegalStateException("secret refresh failure");
        })) {
            session.refresh();
            scheduler.awaitNext().run();
            session.refresh();
            scheduler.awaitNext().run();

            WorkbenchSnapshot accepted = runtime.state().snapshot().orElseThrow();
            assertThat(accepted.identity()).isEqualTo(new WorkbenchSnapshot.Identity(0, 2));
            assertThat(accepted.session()).isEqualTo(successful.session());
            assertThat(accepted.workspace().remoteState())
                    .isEqualTo(new WorkbenchRemoteState.Diagnostic(
                            CliError.WORKBENCH_UNAVAILABLE, Map.of()));
            assertThat(accepted.workspace().rows()).singleElement().satisfies(row -> {
                assertThat(row.key()).isEqualTo(new WorkbenchArtifactKey("pipeline", "orders"));
                assertThat(row.local()).isEqualTo(successful.workspace().rows().getFirst().local());
                assertThat(row.remote()).isEmpty();
                assertThat(row.alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
            });
            assertThat(accepted.pipelines().rows()).isEmpty();
            assertThat(accepted.toString()).doesNotContain("secret refresh failure");
        }
    }

    @Test
    void offlineAndEmptyOutcomesReprojectOnlyTheKnownLocalBaseline() {
        ImmediateScheduler scheduler = new ImmediateScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });
        Workbench.Session session = new Workbench.Session(runtime);
        WorkbenchSnapshot successful = connectedLocalSnapshot(0, 1);
        session.expectSnapshot(successful);
        session.publishRefreshResult(new RefreshResult(0, 1, RefreshResult.success(successful)));

        WorkbenchSnapshot expectedOffline = new WorkbenchSnapshot(0, 2);
        session.expectSnapshot(expectedOffline);
        session.publishRefreshResult(new RefreshResult(0, 2, RefreshResult.offline()));

        WorkbenchSnapshot offline = runtime.state().snapshot().orElseThrow();
        assertThat(offline.session()).isEqualTo(successful.session());
        assertThat(offline.workspace().remoteState()).isEqualTo(new WorkbenchRemoteState.Offline());
        assertThat(offline.workspace().rows()).singleElement().satisfies(row -> {
            assertThat(row.remote()).isEmpty();
            assertThat(row.alignment()).isEqualTo(WorkbenchAlignment.UNKNOWN);
        });

        WorkbenchSnapshot expectedEmpty = new WorkbenchSnapshot(0, 3);
        session.expectSnapshot(expectedEmpty);
        session.publishRefreshResult(new RefreshResult(0, 3, RefreshResult.empty()));

        WorkbenchSnapshot empty = runtime.state().snapshot().orElseThrow();
        assertThat(empty.session()).isEqualTo(successful.session());
        assertThat(empty.workspace().remoteState()).isEqualTo(new WorkbenchRemoteState.Available(0));
        assertThat(empty.workspace().rows()).singleElement().satisfies(row -> {
            assertThat(row.remote()).isEmpty();
            assertThat(row.alignment()).isEqualTo(WorkbenchAlignment.LOCAL_ONLY);
        });
    }

    @Test
    void initialEmptyOutcomeDoesNotClaimRemoteAvailabilityWithoutAContext() {
        ImmediateScheduler scheduler = new ImmediateScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });
        Workbench.Session session = new Workbench.Session(runtime);
        session.expectSnapshot(new WorkbenchSnapshot(0, 1));

        session.publishRefreshResult(new RefreshResult(0, 1, RefreshResult.empty()));

        WorkbenchSnapshot accepted = runtime.state().snapshot().orElseThrow();
        assertThat(accepted.session().connection()).isEqualTo(WorkbenchConnection.NO_CONTEXT);
        assertThat(accepted.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.NotConfigured());
    }

    @Test
    void closingSessionCancelsRefreshWithoutSchedulingARedraw() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, () -> true, event -> {
                });
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<RefreshRequest.CancellationToken> token = new AtomicReference<>();
        AtomicReference<Thread> worker = new AtomicReference<>();
        Workbench.Session session = new Workbench.Session(runtime, (generation, sequence, cancellation) -> {
            worker.set(Thread.currentThread());
            token.set(cancellation);
            started.countDown();
            try {
                awaitIgnoringInterrupt(release);
                return snapshot(generation, sequence, "late");
            } finally {
                finished.countDown();
            }
        });
        session.refresh();
        await(started);

        session.close();

        assertThat(token.get().isCancelled()).isTrue();
        release.countDown();
        await(finished);
        Thread refreshWorker = worker.get();
        refreshWorker.join(TimeUnit.SECONDS.toMillis(2));
        assertThat(refreshWorker.isAlive()).isFalse();
        assertThat(scheduler.pendingCount()).isZero();
        assertThat(runtime.state().snapshot()).isEmpty();
    }

    private static WorkbenchSnapshot snapshot(long generation, long sequence, String... pipelineIds) {
        List<RemoteArtifact> remote = java.util.Arrays.stream(pipelineIds)
                .map(id -> new RemoteArtifact(id, "pipeline", "kind: Pipeline\nmetadata:\n  id: " + id))
                .toList();
        return WorkbenchProjection.project(
                generation,
                sequence,
                WorkbenchSessionSnapshot.empty(),
                List.of(),
                new WorkbenchRemoteState.Available(remote.size()),
                remote);
    }

    private static WorkbenchSnapshot connectedLocalSnapshot(long generation, long sequence) {
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                Path.of("/workspace"),
                Optional.of("dev"),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.SIGNED_IN,
                Optional.of("alice"),
                Optional.of(URI.create("https://tapstate.example")),
                "tapstate test");
        WorkbenchArtifactRow row = new WorkbenchArtifactRow(
                new WorkbenchArtifactKey("pipeline", "orders"),
                List.of(new WorkbenchLocalArtifact(
                        Path.of("pipeline/orders.tap.yml"), Optional.of("pipeline"), true, false, true)),
                List.of(new WorkbenchRemoteArtifact(true, true)),
                WorkbenchAlignment.IN_SYNC);
        List<WorkbenchKindCount> kinds = WorkbenchProjection.VISIBLE_KINDS.stream()
                .map(kind -> new WorkbenchKindCount(
                        kind,
                        kind.equals("pipeline") ? 1 : 0,
                        OptionalInt.of(kind.equals("pipeline") ? 1 : 0)))
                .toList();
        return new WorkbenchSnapshot(
                generation,
                sequence,
                session,
                new WorkbenchOverviewSnapshot(
                        kinds, new WorkbenchAlignmentCounts(0, 0, 1, 0, 0, 0)),
                new WorkbenchWorkspaceSnapshot(new WorkbenchRemoteState.Available(1), List.of(row)),
                new WorkbenchResourceListSnapshot(
                        "source", new WorkbenchRemoteState.Available(1), List.of()),
                new WorkbenchResourceListSnapshot(
                        "pipeline", new WorkbenchRemoteState.Available(1), List.of(row)));
    }

    private static WorkbenchSnapshot signedOutSnapshot(long generation, long sequence) {
        WorkbenchSessionSnapshot session = new WorkbenchSessionSnapshot(
                Path.of("/workspace"),
                Optional.of("dev"),
                Optional.of(ResolvedContext.Source.EXPLICIT),
                WorkbenchConnection.CONNECTED,
                WorkbenchAuthentication.SIGNED_OUT,
                Optional.empty(),
                Optional.of(URI.create("https://tapstate.example")),
                "tapstate test");
        WorkbenchRemoteState remote = new WorkbenchRemoteState.SignedOut();
        return new WorkbenchSnapshot(
                generation,
                sequence,
                session,
                WorkbenchOverviewSnapshot.empty(),
                new WorkbenchWorkspaceSnapshot(remote, List.of()),
                new WorkbenchResourceListSnapshot("source", remote, List.of()),
                new WorkbenchResourceListSnapshot("pipeline", remote, List.of()));
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static String textOf(Buffer buffer) {
        StringBuilder text = new StringBuilder();
        for (int y = 0; y < buffer.height(); y++) {
            for (int x = 0; x < buffer.width(); x++) {
                String symbol = buffer.get(x, y).symbol();
                text.append(symbol.isEmpty() ? ' ' : symbol);
            }
            text.append('\n');
        }
        return text.toString();
    }

    private static final class RecordingScheduler implements LatestOnlyMailbox.Scheduler {
        private final BlockingQueue<Runnable> callbacks = new LinkedBlockingQueue<>();

        @Override
        public void runLater(Runnable callback) {
            callbacks.add(callback);
        }

        private Runnable awaitNext() throws InterruptedException {
            Runnable callback = callbacks.poll(2, TimeUnit.SECONDS);
            assertThat(callback).isNotNull();
            return callback;
        }

        private int pendingCount() {
            return callbacks.size();
        }
    }

    private static final class ImmediateScheduler implements LatestOnlyMailbox.Scheduler {
        private final Queue<Runnable> callbacks = new java.util.ArrayDeque<>();
        private boolean draining;

        @Override
        public void runLater(Runnable callback) {
            callbacks.add(callback);
            if (draining) {
                return;
            }
            draining = true;
            try {
                while (!callbacks.isEmpty()) {
                    callbacks.remove().run();
                }
            } finally {
                draining = false;
            }
        }
    }
}
