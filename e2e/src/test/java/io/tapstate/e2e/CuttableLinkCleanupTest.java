package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/** Actual relay sockets must close even when the listener join reports an interruption. */
class CuttableLinkCleanupTest {
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
