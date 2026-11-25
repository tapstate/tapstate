package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/** Owned relays retain their direct path and close both halves even when cleanup is interrupted. */
@Isolated("This class temporarily owns the JVM proxy selector")
@Execution(ExecutionMode.SAME_THREAD)
class CuttableLinkCleanupTest {
    @Test
    void closingCancelsAnOwnedTargetConnectionBeforeTheRelayStarts() throws Exception {
        stopPendingConnection(true);
    }

    @Test
    void refusingAPeerCancelsItsOwnedTargetConnectionBeforeTheRelayStarts() throws Exception {
        stopPendingConnection(false);
    }

    @Test
    void admittingAPeerDoesNotReviveItsAcceptedConnectionWhileTheTargetIsBeingCreated() throws Exception {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        int linkPort = RealProcessServer.reservePort();
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch releaseCreation = new CountDownLatch(1);
        ConnectingSocket target = new ConnectingSocket();
        CuttableLink link = null;
        Throwable primary = null;
        try (Socket client = new Socket(Proxy.NO_PROXY)) {
            client.bind(new InetSocketAddress(loopback, 0));
            int diallerPort = client.getLocalPort();
            link = CuttableLink.open(linkPort, loopback.getHostAddress(), 1,
                    Map.of("owned-peer", new int[]{diallerPort, diallerPort}), () -> {
                        creating.countDown();
                        try {
                            if (!releaseCreation.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("the owned target creation was never released");
                            }
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError("the owned target creation was interrupted", failure);
                        }
                        return target;
                    });
            client.connect(new InetSocketAddress(loopback, linkPort), 5_000);
            assertThat(creating.await(5, TimeUnit.SECONDS)).as("the real accept reached target creation").isTrue();
            link.refuse("owned-peer");
            link.admit("owned-peer");
            client.setSoTimeout(1_000);
            Throwable retained = catchThrowable(() -> assertThat(client.getInputStream().read())
                    .as("the cut owns and closes the already accepted socket").isEqualTo(-1));
            releaseCreation.countDown();
            assertThat(target.progressed.await(5, TimeUnit.SECONDS))
                    .as("the accept thread handled the returned target socket").isTrue();
            assertAll("admitting new traffic cannot revive a connection accepted before the cut",
                    () -> assertThat(retained).as("the accepted socket closed during target creation").isNull(),
                    () -> assertThat(target.entered.getCount()).as("the cut connection is never dialled").isOne(),
                    () -> assertThat(target.closed.getCount()).as("the late target socket is also closed").isZero());
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            releaseCreation.countDown();
            List<Throwable> failures = new ArrayList<>();
            try { target.close(); } catch (Exception | Error failure) { failures.add(failure); }
            if (link != null) {
                try { link.close(); } catch (Exception | Error failure) { failures.add(failure); }
            }
            try { RealProcessServer.releasePort(linkPort); }
            catch (Exception | Error failure) { failures.add(failure); }
            if (!failures.isEmpty()) {
                if (primary != null) failures.forEach(primary::addSuppressed);
                else {
                    AssertionError failure = new AssertionError("the accepted connection control did not clean up");
                    failures.forEach(failure::addSuppressed);
                    throw failure;
                }
            }
        }
    }

    private static void stopPendingConnection(boolean closeLink) throws Exception {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        int linkPort = RealProcessServer.reservePort();
        CuttableLink link = null;
        Throwable primary = null;
        ConnectingSocket target = new ConnectingSocket();
        try (Socket client = new Socket(Proxy.NO_PROXY)) {
            client.bind(new InetSocketAddress(loopback, 0));
            int diallerPort = client.getLocalPort();
            link = CuttableLink.open(linkPort, loopback.getHostAddress(), 1,
                    Map.of("owned-peer", new int[]{diallerPort, diallerPort}), () -> target);
            Field field = CuttableLink.class.getDeclaredField("acceptThread");
            field.setAccessible(true);
            Thread accepting = (Thread) field.get(link);
            client.connect(new InetSocketAddress(loopback, linkPort), 5_000);
            assertThat(target.entered.await(5, TimeUnit.SECONDS))
                    .as("the real accept thread has entered the owned target connection").isTrue();
            CuttableLink stopping = link;
            Throwable failure = catchThrowable(() -> {
                if (closeLink) stopping.close();
                else stopping.refuse("owned-peer");
            });
            assertThat(failure).as("stopping a relay cancels its pending owned connection").isNull();
            assertThat(target.closed.await(1, TimeUnit.SECONDS))
                    .as("the pending target socket is closed before it can become a relay").isTrue();
            client.setSoTimeout(1_000);
            assertThat(client.getInputStream().read()).as("the accepted client is also released").isEqualTo(-1);
            assertThat(accepting.isAlive()).as("only closing the link stops its listener").isEqualTo(!closeLink);
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            List<Throwable> failures = new ArrayList<>();
            try { target.close(); } catch (Exception | Error failure) { failures.add(failure); }
            if (link != null) {
                try { link.close(); } catch (Exception | Error failure) { failures.add(failure); }
            }
            try { RealProcessServer.releasePort(linkPort); }
            catch (Exception | Error failure) { failures.add(failure); }
            if (!failures.isEmpty()) {
                if (primary != null) failures.forEach(primary::addSuppressed);
                else {
                    AssertionError failure = new AssertionError("the pending connection control did not clean up");
                    failures.forEach(failure::addSuppressed);
                    throw failure;
                }
            }
        }
    }

    /** A connection that only its owning socket's close can release. */
    private static final class ConnectingSocket extends Socket {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private final CountDownLatch progressed = new CountDownLatch(1);

        @Override
        public void connect(SocketAddress endpoint) throws IOException {
            entered.countDown();
            progressed.countDown();
            try {
                if (!closed.await(30, TimeUnit.SECONDS)) {
                    throw new SocketTimeoutException("the owned target connection was never cancelled");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException("the owned connection control was interrupted", failure);
            }
            throw new SocketException("the owned target connection was closed");
        }

        @Override
        public synchronized void close() throws IOException {
            closed.countDown();
            progressed.countDown();
            super.close();
        }
    }

    @Test
    void anInterruptedListenerJoinStillClosesBothOwnedRelayHalvesAndLeavesForeignTrafficAlone()
            throws Exception {
        assertThat(Thread.currentThread().isInterrupted()).as("the control starts without a foreign interrupt")
                .isFalse();
        Field acceptThread = CuttableLink.class.getDeclaredField("acceptThread");
        acceptThread.setAccessible(true);
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        CuttableLink link = null;
        Thread actualAccept = null;
        Thread joinTarget = null;
        CountDownLatch joinTargetEntered = new CountDownLatch(1);
        CountDownLatch releaseJoinTarget = new CountDownLatch(1);
        int reservedLinkPort = -1;
        Throwable primary = null;
        try (ServerSocket targetListener = listener(loopback);
             ServerSocket foreignListener = listener(loopback);
             Socket ownClient = new Socket();
             Socket foreignClient = new Socket(loopback, foreignListener.getLocalPort());
             Socket foreignPeer = foreignListener.accept()) {
            ownClient.bind(new java.net.InetSocketAddress(loopback, 0));
            int diallerPort = ownClient.getLocalPort();
            reservedLinkPort = RealProcessServer.reservePort();
            link = CuttableLink.open(reservedLinkPort, loopback.getHostAddress(), targetListener.getLocalPort(),
                    Map.of("owned-peer", new int[]{diallerPort, diallerPort}));
            ownClient.connect(new java.net.InetSocketAddress(loopback, reservedLinkPort), 5_000);
            actualAccept = (Thread) acceptThread.get(link);
            assertThat(actualAccept).as("the actual relay listener has its own accept thread").isNotNull();
            try (Socket ownTarget = targetListener.accept()) {
                for (Socket socket : List.of(ownClient, ownTarget, foreignClient, foreignPeer)) {
                    socket.setSoTimeout(1_000);
                }
                delivered(ownClient, ownTarget, 41, "the actual relay carries its client byte");
                delivered(ownTarget, ownClient, 42, "the actual relay carries its target byte");
                delivered(foreignClient, foreignPeer, 51, "the separate foreign pair works before cleanup");
                delivered(foreignPeer, foreignClient, 52, "the foreign pair works in both directions");

                // Only the join target is substituted. The real listener, accept thread and bidirectional
                // relay sockets are retained. An owned real blocked thread makes pre-interruption exact,
                // independently of whether closing the real listener lets its accept thread exit quickly.
                joinTarget = Thread.ofPlatform().daemon().name("cuttable-link-join-control").start(() -> {
                    joinTargetEntered.countDown();
                    try {
                        releaseJoinTarget.await();
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("the owned join-target blocker was interrupted", failure);
                    }
                });
                assertThat(joinTargetEntered.await(5, TimeUnit.SECONDS))
                        .as("the substituted join target is actually alive and blocked").isTrue();
                acceptThread.set(link, joinTarget);
                CuttableLink closing = link;
                Thread.currentThread().interrupt();
                Throwable interruptedClose = catchThrowable(closing::close);
                assertThat(interruptedClose).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("interrupted while closing the cuttable link")
                        .hasCauseInstanceOf(InterruptedException.class);
                assertThat(Thread.currentThread().isInterrupted())
                        .as("the original close reports and preserves the caller interrupt").isTrue();

                assertAll("interrupted close completes owned socket cleanup while preserving foreign traffic",
                        () -> reachedEof(ownClient, "owned client-side relay half"),
                        () -> reachedEof(ownTarget, "owned target-side relay half"),
                        () -> delivered(foreignClient, foreignPeer, 61,
                                "the foreign pair remains usable after interrupted close"),
                        () -> delivered(foreignPeer, foreignClient, 62,
                                "foreign traffic still crosses in the other direction"));
                assertThat(Thread.currentThread().isInterrupted())
                        .as("socket inspection did not clear the control's interrupt").isTrue();
            }
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            // The control owns this interrupt. Clear it only here so all inspection above keeps it.
            Thread.interrupted();
            releaseJoinTarget.countDown();
            List<Throwable> cleanupFailures = new ArrayList<>();
            if (joinTarget != null) {
                try {
                    if (!joinTarget.join(Duration.ofSeconds(10))) {
                        throw new AssertionError("the owned substituted join target did not exit");
                    }
                } catch (Exception | Error failure) {
                    cleanupFailures.add(failure);
                }
            }
            if (link != null) {
                try {
                    if (actualAccept != null) acceptThread.set(link, actualAccept);
                    // Re-enter ordinary cleanup with the actual listener thread, including the old
                    // version whose first interrupted close left its owned relay sockets live.
                    link.close();
                } catch (Exception | Error failure) {
                    cleanupFailures.add(failure);
                }
            }
            if (actualAccept != null) {
                try {
                    if (!actualAccept.join(Duration.ofSeconds(10))) {
                        throw new AssertionError("the actual relay accept thread did not exit");
                    }
                } catch (Exception | Error failure) {
                    cleanupFailures.add(failure);
                }
            }
            if (reservedLinkPort >= 0) {
                try {
                    RealProcessServer.releasePort(reservedLinkPort);
                } catch (RuntimeException | Error failure) {
                    cleanupFailures.add(failure);
                }
            }
            if (!cleanupFailures.isEmpty()) {
                if (primary != null) {
                    for (Throwable failure : cleanupFailures) {
                        if (failure != primary) primary.addSuppressed(failure);
                    }
                } else {
                    AssertionError failure = new AssertionError("the owned relay control did not fully clean up");
                    cleanupFailures.forEach(failure::addSuppressed);
                    throw failure;
                }
            }
        }
    }

    @Test
    void anOwnedMemberRelayDoesNotDelegateItsTargetToTheJvmProxy() throws Exception {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        ProxySelector previous = ProxySelector.getDefault();
        AtomicInteger selections = new AtomicInteger();
        AtomicInteger intercepted = new AtomicInteger();
        AtomicInteger proxyFirstByte = new AtomicInteger(-1);
        int linkPort = RealProcessServer.reservePort();
        try (ServerSocket target = listener(loopback);
             ServerSocket proxy = listener(loopback);
             var worker = Executors.newSingleThreadExecutor()) {
            target.setSoTimeout(2_000);
            proxy.setSoTimeout(2_000);
            try (Socket control = new Socket(Proxy.NO_PROXY)) {
                control.connect(new InetSocketAddress(loopback, target.getLocalPort()), 2_000);
                control.setSoTimeout(1_000);
                try (Socket peer = target.accept()) {
                    peer.setSoTimeout(1_000);
                    delivered(control, peer, 31, "the real target is reachable before proxy selection changes");
                    delivered(peer, control, 32, "the independent direct target replies");
                }
            }
            var proxyAttempt = worker.submit(() -> {
                try (Socket socket = proxy.accept()) {
                    intercepted.incrementAndGet();
                    socket.setSoTimeout(1_000);
                    proxyFirstByte.set(socket.getInputStream().read());
                    return proxyFirstByte.get();
                } catch (SocketTimeoutException noConnection) {
                    return -1;
                }
            });
            ProxySelector.setDefault(new ProxySelector() {
                @Override public List<Proxy> select(URI uri) {
                    if ("socket".equals(uri.getScheme()) && uri.getPort() == target.getLocalPort()) {
                        selections.incrementAndGet();
                        return List.of(new Proxy(Proxy.Type.SOCKS,
                                new InetSocketAddress(loopback, proxy.getLocalPort())));
                    }
                    return previous == null ? List.of(Proxy.NO_PROXY) : previous.select(uri);
                }
                @Override public void connectFailed(URI uri, SocketAddress address, IOException failure) {
                    if (uri.getPort() != target.getLocalPort() && previous != null) {
                        previous.connectFailed(uri, address, failure);
                    }
                }
            });
            try (CuttableLink link = CuttableLink.open(linkPort, loopback.getHostAddress(),
                         target.getLocalPort(), Map.of());
                 Socket client = new Socket(Proxy.NO_PROXY)) {
                client.connect(new InetSocketAddress(loopback, linkPort), 2_000);
                client.setSoTimeout(1_000);
                client.getOutputStream().write(73);
                client.getOutputStream().flush();
                Socket accepted = null;
                try { accepted = target.accept(); }
                catch (SocketTimeoutException notConnected) {
                    // The independent direct control already proved this target is reachable.
                }
                try (Socket peer = accepted) {
                    assertThat(peer).as("the relay must reach its owned target directly; proxy selections=%s, "
                            + "intercepted=%s, proxy first byte=%s", selections.get(), intercepted.get(),
                            proxyFirstByte.get()).isNotNull();
                    peer.setSoTimeout(1_000);
                    assertThat(peer.getInputStream().read()).as("the real relay carries its client byte")
                            .isEqualTo(73);
                    delivered(peer, client, 74, "the real relay carries its target reply");
                }
            } finally {
                ProxySelector.setDefault(previous);
            }
            assertThat(proxyAttempt.get(3, TimeUnit.SECONDS)).as("the owned proxy was never contacted")
                    .isEqualTo(-1);
            assertThat(selections.get()).as("fixture member targets do not consult the JVM proxy").isZero();
            assertThat(intercepted.get()).as("fixture bytes did not reach a proxy").isZero();
        } finally {
            ProxySelector.setDefault(previous);
            RealProcessServer.releasePort(linkPort);
        }
        assertThat(ProxySelector.getDefault()).as("the caller's proxy selector is restored").isSameAs(previous);
    }

    private static ServerSocket listener(InetAddress address) throws IOException {
        ServerSocket server = new ServerSocket(0, 8, address);
        server.setSoTimeout(5_000);
        return server;
    }

    private static void delivered(Socket from, Socket to, int value, String what) {
        try {
            from.getOutputStream().write(value);
            from.getOutputStream().flush();
            assertThat(to.getInputStream().read()).as(what).isEqualTo(value);
        } catch (IOException failure) {
            throw new AssertionError(what + ": the real connected socket did not deliver its byte", failure);
        }
    }

    private static void reachedEof(Socket peer, String half) {
        try {
            assertThat(peer.getInputStream().read())
                    .as("interrupted close must finish cleanup of the %s", half).isEqualTo(-1);
        } catch (SocketTimeoutException failure) {
            // A timeout here is the named ownership assertion: setup and bidirectional byte transfer
            // already succeeded, but interrupted close left this actual connected relay half open.
            throw new AssertionError("interrupted close left the " + half + " open", failure);
        } catch (IOException failure) {
            throw new AssertionError("could not independently confirm cleanup of the " + half, failure);
        }
    }
}
