package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A stretch of network a case can cut.
 *
 * <p>It listens on a loopback port of its own and relays every byte to one member's real member port.
 * A member that reports this link's address rather than its own is reached through here by everybody,
 * so cutting the link is the only thing on a single machine that severs member traffic without a
 * packet filter or an address nobody has root to create.
 *
 * <p><strong>Which pairs a cut reaches is decided by who dialled whom</strong>, not by this class: a
 * connection lives in the link in front of whichever member was dialled, so cutting one link severs
 * what was dialled into that member and nothing it dialled outward. Measured on a cluster of three,
 * that is not enough to separate one member from the others -- see {@link PartitionableCluster}, which
 * records what the three of them reported afterwards.
 *
 * <p>{@link #carried()} is not decoration. A case that cuts a link and then asserts something stopped
 * has asserted nothing unless traffic was going through the link to begin with, and a link nobody
 * dialled looks exactly like a link that was cut.
 */
final class CuttableLink implements AutoCloseable {

    private static final String LOOPBACK = "127.0.0.1";

    private final int port;
    private final String targetHost;
    private final int targetPort;
    private final List<Socket> live = new CopyOnWriteArrayList<>();
    private final AtomicInteger carried = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile ServerSocket listener;
    private volatile Thread acceptor;

    private CuttableLink(int port, String targetHost, int targetPort) {
        this.port = port;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
    }

    /**
     * Opens a link on {@code port} carrying everything to {@code targetHost:targetPort}.
     *
     * <p>The port is reserved by the caller before any member starts, because a member is told what to
     * report on its command line and cannot be told afterwards.
     */
    static CuttableLink open(int port, String targetHost, int targetPort) {
        CuttableLink link = new CuttableLink(port, targetHost, targetPort);
        link.listen();
        return link;
    }

    /** The address a member reports so that everything reaching it comes through this link. */
    String address() {
        return LOOPBACK + ":" + port;
    }

    /** How many connections this link has carried since it opened -- across cuts, never reset. */
    int carried() {
        return carried.get();
    }

    /**
     * Severs it: no new connection is accepted, and every live one is closed from underneath both ends.
     *
     * <p>Closing the live ones is the half that matters. A cluster holds its connections open for as
     * long as they work, so refusing new ones alone would leave every existing pair talking and the
     * cut would read as a cluster that never noticed anything.
     */
    void cut() {
        closeListener();
        for (Socket socket : live) {
            closeQuietly(socket);
        }
        live.clear();
    }

    /** Opens it again, for the half of a case that asks what happens once the network comes back. */
    void restore() {
        if (listener == null && !closed.get()) {
            listen();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        cut();
    }

    private void listen() {
        try {
            ServerSocket server = new ServerSocket();
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), port));
            listener = server;
            Thread thread = new Thread(() -> accept(server), "cuttable-link-" + port);
            thread.setDaemon(true);
            thread.start();
            acceptor = thread;
        } catch (IOException e) {
            throw new AssertionError("could not open a cuttable link on " + LOOPBACK + ":" + port, e);
        }
    }

    private void accept(ServerSocket server) {
        while (!server.isClosed()) {
            Socket inbound;
            try {
                inbound = server.accept();
            } catch (IOException stopped) {
                // Cut or closed: the listener going away is how this thread is meant to end.
                return;
            }
            carried.incrementAndGet();
            try {
                Socket outbound = new Socket(targetHost, targetPort);
                live.add(inbound);
                live.add(outbound);
                pump(inbound, outbound);
                pump(outbound, inbound);
            } catch (IOException unreachable) {
                closeQuietly(inbound);
            }
        }
    }

    private void pump(Socket from, Socket to) {
        Thread thread = new Thread(() -> {
            byte[] buffer = new byte[8192];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException ended) {
                // Either end closing is ordinary: a cut closes both, and a member stopping closes one.
            } finally {
                closeQuietly(from);
                closeQuietly(to);
            }
        }, "cuttable-link-pump-" + port);
        thread.setDaemon(true);
        thread.start();
    }

    private void closeListener() {
        ServerSocket server = listener;
        listener = null;
        if (server != null) {
            try {
                server.close();
            } catch (IOException ignored) {
                // Closing a listener that is already gone is not a failure of the cut.
            }
        }
        Thread thread = acceptor;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // A socket the other end already closed is exactly what a cut is trying to produce.
        }
    }
}
