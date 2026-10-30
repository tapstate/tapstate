package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Optional buffered observations of existing fixture actions. */
final class ClusterFixtureTiming {
    enum Phase { PREPARE_BEGIN, PREPARED, SPAWN_BEGIN, SPAWN_RETURNED, HEALTH_BEGIN, HEALTH_RETURNED,
        READY_BEGIN, READY_RETURNED, QUIT_RETURNED, GRACE_BEGIN, GRACE_RETURNED, FORCE_RETURNED,
        EXIT_OBSERVED, EXIT_UNCONFIRMED, EXIT_CONFIRMED, INTERRUPTED }
    record Event(long ticket, Phase phase, long nanos, String parameters, String cluster) { }
    private static final ThreadLocal<Scope> ACTIVE = new ThreadLocal<>();
    private ClusterFixtureTiming() { }

    static Scope install() {
        Scope scope = new Scope(ACTIVE.get(), true);
        ACTIVE.set(scope);
        return scope;
    }
    static Scope activeScope() { return ACTIVE.get(); }
    static Scope suspend() {
        Scope scope = new Scope(ACTIVE.get(), false);
        ACTIVE.set(scope);
        return scope;
    }
    private static Scope current() {
        Scope scope = ACTIVE.get();
        if (scope == null || !scope.enabled) return null;
        if (scope.closed) { scope.valid = false; return null; }
        return scope;
    }
    static long beginPreparation() {
        Scope scope = current();
        if (scope == null) return 0;
        long start = System.nanoTime();
        try {
            synchronized (scope) {
                long id = ++scope.next;
                scope.record(id, Phase.PREPARE_BEGIN, start, "-");
                return id;
            }
        } catch (RuntimeException | Error failure) { scope.valid = false; return 0; }
        finally { scope.account(System.nanoTime() - start); }
    }
    static void bind(long id, int port) {
        Scope scope = current();
        if (scope == null) return;
        long start = System.nanoTime();
        try { synchronized (scope) { scope.ports.put(port, id); } }
        catch (RuntimeException | Error failure) { scope.valid = false; }
        finally { scope.account(System.nanoTime() - start); }
    }
    static void prepared(int port, String store, String operator, String listen,
            List<String> options, List<String> arguments) {
        Scope scope = current();
        if (scope == null) return;
        long start = System.nanoTime();
        try {
            synchronized (scope) {
                long id = scope.ports.getOrDefault(port, 0L);
                if (id == 0) { scope.valid = false; return; }
                if (scope.parameters.containsKey(id)) return;
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                List<String> values = new ArrayList<>(List.of(store, operator, listen));
                values.addAll(options);
                values.addAll(arguments);
                for (String value : values) {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    digest.update(new byte[] {(byte)(bytes.length >>> 24), (byte)(bytes.length >>> 16),
                            (byte)(bytes.length >>> 8), (byte)bytes.length});
                    digest.update(bytes);
                }
                String hash = java.util.HexFormat.of().formatHex(digest.digest());
                scope.parameters.put(id, hash);
                String cluster = arguments.stream().filter(value -> value.startsWith("--tapstate.cluster.id="))
                        .map(value -> value.substring("--tapstate.cluster.id=".length())).findFirst().orElse("");
                if (!cluster.isEmpty()) scope.clusters.put(id, java.util.HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(cluster.getBytes(StandardCharsets.UTF_8))));
                scope.record(id, Phase.PREPARED, start, hash);
            }
        } catch (Exception | Error failure) { scope.valid = false; }
        finally { scope.account(System.nanoTime() - start); }
    }
    static void spawnBegin(int port) { byPort(port, Phase.SPAWN_BEGIN); }
    static void spawned(int port, Process process) {
        Scope scope = current();
        if (scope == null) return;
        long start = System.nanoTime();
        try {
            synchronized (scope) {
                long id = scope.ports.getOrDefault(port, 0L);
                scope.processes.put(process, id);
                scope.record(id, Phase.SPAWN_RETURNED, start, scope.parameters.getOrDefault(id, "-"));
            }
        } catch (RuntimeException | Error failure) { scope.valid = false; }
        finally { scope.account(System.nanoTime() - start); }
    }
    private static void byPort(int port, Phase phase) {
        Scope scope = current();
        if (scope == null) return;
        long start = System.nanoTime();
        try { synchronized (scope) {
            long id = scope.ports.getOrDefault(port, 0L);
            scope.record(id, phase, start, scope.parameters.getOrDefault(id, "-"));
        } } catch (RuntimeException | Error failure) { scope.valid = false; }
        finally { scope.account(System.nanoTime() - start); }
    }
    static void phase(Process process, Phase phase) {
        Scope scope = current();
        if (scope == null) return;
        long start = System.nanoTime();
        try { synchronized (scope) {
            long id = scope.processes.getOrDefault(process, 0L);
            if (phase == Phase.GRACE_RETURNED && scope.graceReturned.contains(id)) return;
            if (phase == Phase.GRACE_RETURNED) scope.graceReturned.add(id);
            scope.record(id, phase, start, scope.parameters.getOrDefault(id, "-"));
        } } catch (RuntimeException | Error failure) { scope.valid = false; }
        finally { scope.account(System.nanoTime() - start); }
    }
    static void exitObserved(Process process) {
        if (current() == null) return;
        try { phase(process, process.isAlive() ? Phase.EXIT_UNCONFIRMED : Phase.EXIT_OBSERVED); }
        catch (RuntimeException | Error failure) { current().valid = false; }
    }
    static final class Scope implements AutoCloseable {
        private final Scope previous;
        private final boolean enabled;
        private final Thread owner = Thread.currentThread();
        private final List<Event> events = new ArrayList<>();
        private final Map<Integer, Long> ports = new HashMap<>();
        private final Map<Process, Long> processes = new IdentityHashMap<>();
        private final Map<Long, String> parameters = new HashMap<>();
        private final Map<Long, String> clusters = new HashMap<>();
        private final List<Long> graceReturned = new ArrayList<>();
        private long next;
        private volatile long overhead;
        private volatile boolean valid = true;
        private boolean closed;
        private boolean requestedClose;
        private Scope(Scope previous, boolean enabled) { this.previous = previous; this.enabled = enabled; }
        private synchronized void account(long nanos) { overhead += nanos; }
        private void record(long id, Phase phase, long nanos, String parameters) {
            if (id == 0 || events.size() >= 10000) { valid = false; return; }
            events.add(new Event(id, phase, nanos, parameters, clusters.getOrDefault(id, "-")));
        }
        synchronized List<Event> events() { return List.copyOf(events); }
        synchronized boolean valid() { return valid; }
        synchronized Path write(Path directory) throws java.io.IOException {
            if (!Files.isDirectory(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory))
                throw new java.io.IOException("owned timing directory is unavailable");
            long start = System.nanoTime();
            List<String> lines = new ArrayList<>(List.of("CFT\t1"));
            for (Event event : events) lines.add(event.ticket()+"\t"+event.phase()+"\t"+event.nanos()+"\t"+event.parameters()+"\t"+event.cluster());
            lines.add("END\t"+events.size()+"\t"+overhead+"\t"+(valid ? "recorded" : "unknown"));
            Path file = directory.resolve("cluster-fixture-timing-"+UUID.randomUUID()+".tsv");
            Files.write(file, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            long primaryFlush = System.nanoTime() - start;
            long metadataStart = System.nanoTime();
            String hash;
            try { hash = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(Files.readAllBytes(file))); }
            catch (java.security.NoSuchAlgorithmException failure) { throw new AssertionError(failure); }
            long metadataPreparation = System.nanoTime() - metadataStart;
            String receipt = "{\"schema\":1,\"trace_sha256\":\""+hash+"\",\"primary_flush_nanos\":"
                    +primaryFlush+",\"metadata_preparation_nanos\":"+metadataPreparation
                    +",\"metadata_write_unmeasured\":true}";
            Files.writeString(Path.of(file.toString()+".flush.json"), receipt, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            // Sidecar writing and class initialization remain in complete fork elapsed cost.
            return file;
        }
        @Override public synchronized void close() {
            if (Thread.currentThread() != owner || closed) { valid = false; return; }
            requestedClose = true;
            if (ACTIVE.get() != this) { valid = false; return; }
            closed = true;
            Scope restore = previous;
            while (restore != null && restore.owner == owner && restore.requestedClose) {
                restore.closed = true;
                restore = restore.previous;
            }
            if (restore == null) ACTIVE.remove(); else ACTIVE.set(restore);
        }
    }
}
