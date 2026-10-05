package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A stretch of network in front of one member that a case can cut per peer.
 *
 * <p>It listens on a loopback port of its own and relays every byte to one member's real member port.
 * A member that reports this link's address rather than its own is reached through here by everybody,
 * so this is the only thing on a single machine that severs member traffic without a packet filter or
 * an address nobody has root to create.
 *
 * <p><strong>A cut has to be chosen by peer, not by direction, and that was measured.</strong> A
 * connection lives in the link in front of whichever member was dialled, so severing a whole link
 * severs what was dialled into that member and nothing it dialled outward: cutting one member's link
 * on a cluster of three left the three holding three different answers -- the member whose link was
 * cut still reported all three, one reported two, and the third reported itself alone. Routing each
 * ordered pair through its own link does not fix it either, because the seed list is consulted only
 * for the first connection: a member's entry in the member list is the address it reports, and every
 * later dial goes there. Measured against a cluster of three, every member named the others by their
 * link addresses and never by the ports they bound.
 *
 * <p>So the dialler is identified instead, by the local port it dialled from. A member told which
 * ports it may dial from arrives here with a source port inside that range, and refusing one peer is
 * then exact and symmetric: refuse a in front of b and b in front of a, and that pair is severed
 * whichever of them dialled. Chosen this way, three members went to 1/2/2 within five seconds and
 * stayed there while both sides re-dialled the reported address hundreds of times.
 *
 * <p>{@link #carried()} and {@link #unattributed()} are not decoration. A case that cuts and then
 * asserts something stopped has asserted nothing unless traffic was going through here to begin with,
 * and a link nobody dialled looks exactly like a link that was cut. A connection this link could not
 * attribute is worse: it would pass straight through a cut aimed at its dialler, so a case reads that
 * count and refuses to believe a partition that has any.
 */
final class CuttableLink implements AutoCloseable {

    private static final String LOOPBACK = "127.0.0.1";
    private static final int MAX_UNKNOWN_RECEIPTS = 32;

    private final int port;
    private final String targetHost;
    private final int targetPort;
    private final Map<String, int[]> diallerPorts;
    private final List<String> refused = new CopyOnWriteArrayList<>();
    private final List<Held> live = new CopyOnWriteArrayList<>();
    private final Map<String, AtomicInteger> carriedFrom = new LinkedHashMap<>();
    private final AtomicInteger carried = new AtomicInteger();
    private final AtomicInteger turnedAway = new AtomicInteger();
    private final AtomicInteger unattributed = new AtomicInteger();
    private final Object receiptLock = new Object();
    private final List<UnknownAccept> unknownReceipts = new ArrayList<>();
    private final AtomicInteger lostUnknownReceipts = new AtomicInteger();
    private volatile boolean closed;
    private volatile ServerSocket listener;
    private volatile Thread acceptThread;

    /** One connection through here, remembered by who dialled it so a cut can pick it out. */
    private record Held(Socket inbound, Socket outbound, String dialler, UnknownAccept receipt) { }

    private static final class UnknownAccept {
        final String acceptedAt = Instant.now().toString();
        final String dialler, sourceAddress, linkAddress;
        final int sourcePort, linkPort;
        final List<String> refusedAtAccept;
        final AtomicLong sourceBytesRead = new AtomicLong(), targetBytesRead = new AtomicLong();
        final AtomicLong sourceBytesForwarded = new AtomicLong(), targetBytesForwarded = new AtomicLong();
        final AtomicReference<String> closedAt = new AtomicReference<>();
        final AtomicReference<String> closeReason = new AtomicReference<>();
        volatile boolean connected, partialWriteUnavailable;
        volatile AtomicInteger carriedFromCounter;
        volatile int carriedFromAtAccept;
        volatile String relayAddress;
        volatile int relayPort;

        UnknownAccept(Socket inbound, String dialler, List<String> refused) {
            this.dialler = dialler; this.sourceAddress = inbound.getInetAddress().getHostAddress();
            this.sourcePort = inbound.getPort(); this.linkAddress = inbound.getLocalAddress().getHostAddress();
            this.linkPort = inbound.getLocalPort(); this.refusedAtAccept = List.copyOf(refused);
        }

        void ended(String reason) {
            if (closeReason.compareAndSet(null, reason)) { closedAt.set(Instant.now().toString()); }
        }

        Map<String, Object> evidence() {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("acceptedAt", acceptedAt); row.put("dialler", dialler);
            row.put("sourceAddress", sourceAddress); row.put("sourcePort", sourcePort);
            row.put("linkAddress", linkAddress); row.put("linkPort", linkPort);
            row.put("refusedAtAccept", refusedAtAccept); row.put("cutActiveAtAccept", !refusedAtAccept.isEmpty());
            row.put("targetConnected", connected); row.put("relayLocalAddress", relayAddress); row.put("relayLocalPort", relayPort);
            row.put("sourceBytesRead", sourceBytesRead.get()); row.put("targetBytesRead", targetBytesRead.get());
            row.put("sourceBytesForwarded", sourceBytesForwarded.get()); row.put("targetBytesForwarded", targetBytesForwarded.get());
            row.put("partialWriteUnavailable", partialWriteUnavailable);
            row.put("carriedFromAtAccept", carriedFromAtAccept);
            row.put("carriedFromCurrent", carriedFromCounter == null ? null : carriedFromCounter.get());
            row.put("closedAt", closedAt.get()); row.put("closeReason", closeReason.get());
            return java.util.Collections.unmodifiableMap(row);
        }
    }

    private CuttableLink(int port, String targetHost, int targetPort, Map<String, int[]> diallerPorts) {
        this.port = port;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.diallerPorts = Map.copyOf(diallerPorts);
        diallerPorts.keySet().forEach(node -> carriedFrom.put(node, new AtomicInteger()));
    }

    /**
     * Opens a link on {@code port} carrying everything to {@code targetHost:targetPort}.
     *
     * <p>The port is reserved by the caller before any member starts, because a member is told what to
     * report on its command line and cannot be told afterwards. {@code diallerPorts} maps each member
     * to the local ports it was told to dial from, which is the only thing that identifies a dialler
     * here -- everything arrives from loopback.
     */
    static CuttableLink open(int port, String targetHost, int targetPort, Map<String, int[]> diallerPorts) {
        CuttableLink link = new CuttableLink(port, targetHost, targetPort, diallerPorts);
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

    /** How many of those one named member dialled. */
    int carriedFrom(String dialler) {
        AtomicInteger count = carriedFrom.get(dialler);
        return count == null ? 0 : count.get();
    }

    /** How many connections this link has turned away because their dialler is refused. */
    int turnedAway() {
        return turnedAway.get();
    }

    /**
     * How many connections arrived from a port belonging to no known member.
     *
     * <p>Any at all means a cut aimed at a dialler would have let that connection through, so a case
     * that reads a partition has to read this too rather than trust the membership it sees.
     */
    int unattributed() {
        return unattributed.get();
    }

    /** Passive, bounded receipts; the total unknown counter is never reset or truncated. */
    Map<String, Object> attributionEvidence() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("link", address()); out.put("targetHost", targetHost); out.put("targetPort", targetPort);
        Map<String, List<Integer>> ranges = new java.util.TreeMap<>();
        diallerPorts.forEach((node, range) -> ranges.put(node, List.of(range[0], range[1])));
        out.put("configuredRanges", ranges); out.put("currentlyRefused", List.copyOf(refused));
        out.put("carried", carried.get()); out.put("turnedAway", turnedAway.get()); out.put("unattributed", unattributed.get());
        synchronized (receiptLock) { out.put("unknownAccepts", unknownReceipts.stream().map(UnknownAccept::evidence).toList()); }
        out.put("lostUnknownReceipts", lostUnknownReceipts.get());
        out.put("receiptCoverage", lostUnknownReceipts.get() == 0 ? "RETAINED" : "PARTIAL_HISTORY_UNAVAILABLE");
        out.put("snapshotConsistency", "NON_ATOMIC_LIVE_TRANSPORT_COUNTERS");
        return java.util.Collections.unmodifiableMap(out);
    }

    private UnknownAccept retainUnknown(Socket inbound, String dialler) {
        if (diallerPorts.containsKey(dialler)) { return null; }
        synchronized (receiptLock) {
            if (unknownReceipts.size() >= MAX_UNKNOWN_RECEIPTS) { lostUnknownReceipts.incrementAndGet(); return null; }
            UnknownAccept receipt = new UnknownAccept(inbound, dialler, refused);
            unknownReceipts.add(receipt); return receipt;
        }
    }

    /**
     * Severs one peer: its live connections through here are closed, and its later dials are refused.
     *
     * <p>Closing the live ones is the half that matters. A cluster holds its connections open for as
     * long as they work, so refusing new ones alone would leave the existing pair talking and the cut
     * would read as a cluster that never noticed anything.
     */
    void refuse(String dialler) {
        refused.add(dialler);
        for (Held held : List.copyOf(live)) {
            if (held.dialler().equals(dialler)) {
                closeQuietly(held.inbound());
                closeQuietly(held.outbound());
                if (held.receipt() != null) { held.receipt().ended("PEER_REFUSED"); }
                live.remove(held);
            }
        }
    }

    /** Lets one peer back in, for the half of a case that asks what happens once the network returns. */
    void admit(String dialler) {
        refused.remove(dialler);
    }

    @Override
    public void close() {
        closed = true;
        ServerSocket server = listener;
        listener = null;
        if (server != null) {
            try {
                server.close();
            } catch (IOException ignored) {
                // Closing a listener that is already gone is not a failure.
            }
        }
        // A blocked accept can retain the socket after close returns. Wait for it to release the port.
        Thread accepting = acceptThread;
        if (accepting != null) {
            try {
                if (!accepting.join(Duration.ofSeconds(10))) {
                    throw new AssertionError("the cuttable link listener did not stop on " + address());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while closing the cuttable link on " + address(), e);
            }
        }
        for (Held held : live) {
            closeQuietly(held.inbound());
            closeQuietly(held.outbound());
            if (held.receipt() != null) { held.receipt().ended("FIXTURE_CLOSE"); }
        }
        live.clear();
    }

    private void listen() {
        try {
            ServerSocket server = new ServerSocket();
            server.setReuseAddress(true);
            RealProcessServer.releasePort(port);
            server.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), port));
            listener = server;
            Thread thread = new Thread(() -> accept(server), "cuttable-link-" + port);
            thread.setDaemon(true);
            acceptThread = thread;
            thread.start();
        } catch (IOException e) {
            throw new AssertionError("could not open a cuttable link on " + address(), e);
        }
    }

    private void accept(ServerSocket server) {
        while (!server.isClosed() && !closed) {
            Socket inbound;
            try {
                inbound = server.accept();
            } catch (IOException stopped) {
                // The listener going away is how this thread is meant to end.
                return;
            }
            String dialler = whoDialled(inbound.getPort());
            UnknownAccept receipt = retainUnknown(inbound, dialler);
            if (refused.contains(dialler)) {
                turnedAway.incrementAndGet();
                closeQuietly(inbound);
                if (receipt != null) { receipt.ended("REFUSED_AT_ACCEPT"); }
                continue;
            }
            carried.incrementAndGet();
            AtomicInteger fromCounter = carriedFrom.computeIfAbsent(dialler, unknown -> new AtomicInteger());
            int fromAtAccept = fromCounter.incrementAndGet();
            if (receipt != null) { receipt.carriedFromCounter = fromCounter; receipt.carriedFromAtAccept = fromAtAccept; }
            try {
                Socket outbound = new Socket(targetHost, targetPort);
                if (receipt != null) {
                    receipt.relayAddress = outbound.getLocalAddress().getHostAddress(); receipt.relayPort = outbound.getLocalPort();
                    receipt.connected = true;
                }
                live.add(new Held(inbound, outbound, dialler, receipt));
                pump(inbound, outbound, receipt, true);
                pump(outbound, inbound, receipt, false);
            } catch (IOException unreachable) {
                closeQuietly(inbound);
                if (receipt != null) { receipt.ended("TARGET_CONNECT_FAILED:" + unreachable.getClass().getSimpleName()); }
            }
        }
    }

    /** Which member dialled, read off the local port it was told to dial from. */
    private String whoDialled(int sourcePort) {
        for (Map.Entry<String, int[]> entry : diallerPorts.entrySet()) {
            if (sourcePort >= entry.getValue()[0] && sourcePort <= entry.getValue()[1]) {
                return entry.getKey();
            }
        }
        unattributed.incrementAndGet();
        return "unattributed:" + sourcePort;
    }

    private void pump(Socket from, Socket to, UnknownAccept receipt, boolean fromSource) {
        Thread thread = new Thread(() -> {
            byte[] buffer = new byte[8192];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    if (receipt != null) { (fromSource ? receipt.sourceBytesRead : receipt.targetBytesRead).addAndGet(read); }
                    try { out.write(buffer, 0, read); }
                    catch (IOException failed) { if (receipt != null) { receipt.partialWriteUnavailable = true; } throw failed; }
                    if (receipt != null) { (fromSource ? receipt.sourceBytesForwarded : receipt.targetBytesForwarded).addAndGet(read); }
                    out.flush();
                }
            } catch (IOException ended) {
                // Either end closing is ordinary: a cut closes both, and a member stopping closes one.
            } finally {
                closeQuietly(from);
                closeQuietly(to);
                if (receipt != null) { receipt.ended(fromSource ? "SOURCE_PUMP_CLOSED" : "TARGET_PUMP_CLOSED"); }
            }
        }, "cuttable-link-pump-" + port);
        thread.setDaemon(true);
        thread.start();
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // A socket the other end already closed is exactly what a cut is trying to produce.
        }
    }
}
