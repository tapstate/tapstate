package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SetupCapabilityService;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** Deployment-owner files for one installation; opening a missing stage never creates an installation. */
final class InstallationFiles {
    enum Handoff { LOCAL, INTENT, URI_WRITTEN, CREATE_STARTED, CONFIGURED, RESTART_REQUIRED, RESTARTED }
    record Stage(StorageSetupScope scope, Handoff handoff, String configurationHash, String managementUrl) {
        Stage {
            Objects.requireNonNull(scope); Objects.requireNonNull(handoff);
            if (handoff != Handoff.LOCAL && (configurationHash == null || !configurationHash.matches("[0-9a-f]{64}"))) throw refused();
            if (handoff == Handoff.LOCAL && configurationHash != null) throw refused();
            if (managementUrl != null && !managementUrl.matches("http://127\\.0\\.0\\.1:[1-9][0-9]{0,4}")) throw refused();
        }
        Stage withScope(StorageSetupScope value) { return new Stage(value, handoff, configurationHash, managementUrl); }
        Stage withHandoff(Handoff value, String hash) { return new Stage(scope, value, hash, managementUrl); }
        Stage withManagementUrl(String value) { return new Stage(scope, handoff, configurationHash, value); }
    }
    private static final Set<java.nio.file.attribute.PosixFilePermission> DIRECTORY_MODE = PosixFilePermissions.fromString("rwx------");
    private static final Set<java.nio.file.attribute.PosixFilePermission> FILE_MODE = PosixFilePermissions.fromString("rw-------");
    private static final ConcurrentHashMap<Path, FileState> LOCKS = new ConcurrentHashMap<>();
    private static final int MAX_BYTES = 128 * 1024;
    private final Path directory;
    private final FileState shared;

    InstallationFiles(Path directory) {
        Path requested = directory.toAbsolutePath().normalize();
        check(requested, true);
        try { this.directory = requested.toRealPath(); }
        catch (IOException invalid) { throw refused(); }
        check(this.directory, true);
        this.shared = LOCKS.computeIfAbsent(this.directory, FileState::new);
        check(path("owner.lock"), false);
    }

    /** Explicit installation command only; a pre-existing directory is never repurposed or reset. */
    static InstallationFiles initialize(Path requested) {
        Path directory = requested.toAbsolutePath().normalize();
        try {
            Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(DIRECTORY_MODE));
            Files.createFile(directory.resolve("owner.lock"), PosixFilePermissions.asFileAttribute(FILE_MODE));
            var files = new InstallationFiles(directory);
            var scope = new StorageSetupScope(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), 1);
            files.locked(() -> { files.saveStage(new Stage(scope, Handoff.LOCAL, null, null)); return null; });
            return files;
        } catch (IOException invalid) { throw refused(); }
    }

    Path directory() { return directory; }
    Path caFile() { return path("config-ca.pem"); }
    void requireOwner() { check(directory, true); check(path("owner.lock"), false); }

    <T> T locked(Supplier<T> action) {
        shared.mutex.lock();
        try {
            if (Thread.currentThread().isInterrupted()) throw refused();
            requireOwner();
            if (shared.mutex.getHoldCount() > 1) return action.get();
            try {
                var channel = shared.open(); shared.mutation = true;
                try (var lock = mutationLock(channel)) {
                    requireOwner(); shared.requireIdentity(); return action.get();
                }
            } catch (IOException | java.nio.channels.OverlappingFileLockException invalid) { throw refused(); }
            finally { shared.mutation = false; shared.closeIdle(); }
        } finally { shared.mutex.unlock(); }
    }

    private static java.nio.channels.FileLock mutationLock(FileChannel channel) throws IOException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
        while (true) {
            if (Thread.currentThread().isInterrupted()) throw refused();
            // Blocking FileChannel.lock is interruptible and closes this shared channel, dropping
            // runtime ownership as well. Nonblocking attempts preserve it when a caller cancels.
            var acquired = channel.tryLock(0, 1, false);
            if (acquired != null) return acquired;
            if (System.nanoTime() - deadline >= 0) throw refused();
            java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    /** A separate lock region admits one service process without blocking owner CLI file updates. */
    // The shared file state owns the channel; RuntimeOwner retains its lock and closeIdle releases failed opens.
    @SuppressWarnings("java:S2095")
    RuntimeOwner ownRuntime() {
        shared.mutex.lock();
        try {
            if (Thread.currentThread().isInterrupted()) throw refused();
            requireOwner();
            if (shared.runtime != null) throw refused();
            var lock = shared.open().tryLock(1, 1, false);
            if (lock == null) throw refused();
            shared.requireIdentity();
            shared.runtime = new RuntimeOwner(shared, lock); return shared.runtime;
        } catch (IOException | java.nio.channels.OverlappingFileLockException failure) { throw refused(); }
        finally { try { shared.closeIdle(); } finally { shared.mutex.unlock(); } }
    }
    static final class RuntimeOwner implements AutoCloseable {
        private final FileState shared;
        private final java.nio.channels.FileLock lock;
        private final FileChannel channel;
        private final Object fileKey;
        private RuntimeOwner(FileState shared, java.nio.channels.FileLock lock) {
            this.shared = shared; this.lock = lock; this.channel = shared.channel; this.fileKey = shared.fileKey;
        }
        void requireActive() {
            // Runtime/store monitors may call this while an installation writer holds the file
            // mutex. Ownership validation must never acquire that mutex in the reverse order.
            requireValid();
            try {
                Path path = shared.directory.resolve("owner.lock"); check(shared.directory, true); check(path, false);
                if (!Objects.equals(fileKey, Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey())) throw refused();
            } catch (IOException invalid) { throw refused(); }
            requireValid();
        }
        private void requireValid() {
            if (shared.runtime != this || shared.poisoned || !lock.isValid() || !channel.isOpen()) throw refused();
        }
        @Override public void close() {
            shared.mutex.lock();
            try {
                if (shared.runtime != this) return;
                lock.release(); shared.runtime = null; shared.closeIdle();
            } catch (IOException failure) { shared.poisoned = true; throw refused(); }
            finally { shared.mutex.unlock(); }
        }
    }

    /** POSIX locks are process-owned: closing another descriptor can drop every lock on this inode. */
    private static final class FileState {
        final ReentrantLock mutex = new ReentrantLock();
        final Path directory;
        FileChannel channel;
        Object fileKey;
        // Identity must be readable without reversing the file mutex and runtime lock order.
        @SuppressWarnings("java:S3077")
        volatile RuntimeOwner runtime;
        boolean mutation;
        volatile boolean poisoned;
        FileState(Path directory) { this.directory = directory; }
        FileChannel open() throws IOException {
            if (poisoned) throw refused();
            Path path = directory.resolve("owner.lock"); check(directory, true); check(path, false);
            Object actual = Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
            if (actual == null) throw refused();
            if (channel == null) {
                channel = FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS); fileKey = actual;
            }
            requireIdentity(); return channel;
        }
        void requireIdentity() throws IOException {
            if (poisoned || channel == null || !channel.isOpen()) throw refused();
            Path path = directory.resolve("owner.lock"); check(directory, true); check(path, false);
            if (!Objects.equals(fileKey, Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey())) throw refused();
        }
        void closeIdle() {
            if (runtime != null || mutation || channel == null) return;
            try { channel.close(); channel = null; fileKey = null; }
            catch (IOException failure) { poisoned = true; throw refused(); }
        }
    }

    Stage stage() {
        return locked(() -> {
            var value = object(read("stage.json"));
            exact(value, "formatVersion", "clusterId", "installationId", "operationId", "generation", "handoff", "configurationHash", "managementUrl");
            version(value);
            try { return new Stage(scope(value), Handoff.valueOf(text(value, "handoff")), nullable(value, "configurationHash"), nullable(value, "managementUrl")); }
            catch (IllegalArgumentException invalid) { throw refused(); }
        });
    }

    void saveStage(Stage stage) {
        var value = scopeTree(stage.scope());
        value.put("handoff", stage.handoff().name()); value.put("configurationHash", stage.configurationHash()); value.put("managementUrl", stage.managementUrl());
        write("stage.json", JsonWriter.write(value));
    }

    Optional<SetupCapabilityService.Issued> capability() {
        return locked(() -> {
            if (!exists("capability.json")) return Optional.empty();
            var value = object(read("capability.json"));
            exact(value, "formatVersion", "clusterId", "installationId", "operationId", "generation", "purpose", "expiresAt", "token");
            version(value);
            try { return Optional.of(new SetupCapabilityService.Issued(scope(value), StorageSetupStateStore.Purpose.valueOf(text(value, "purpose")),
                    Instant.parse(text(value, "expiresAt")), text(value, "token"))); }
            catch (IllegalArgumentException | java.time.DateTimeException invalid) { throw refused(); }
        });
    }

    void saveCapability(SetupCapabilityService.Issued capability) {
        var value = scopeTree(capability.scope());
        value.put("purpose", capability.purpose().name()); value.put("expiresAt", capability.expiresAt().toString()); value.put("token", capability.token());
        write("capability.json", JsonWriter.write(value));
    }

    void removeCapability() {
        locked(() -> {
            if (exists("capability.json")) {
                check(path("capability.json"), false);
                try { Files.delete(path("capability.json")); syncDirectory(); }
                catch (IOException invalid) { throw refused(); }
            }
            return null;
        });
    }

    Optional<String> configurationUri() {
        return locked(() -> exists("configuration.uri") ? Optional.of(read("configuration.uri")) : Optional.empty());
    }
    Optional<String> configurationCa() {
        return locked(() -> exists("config-ca.pem") ? Optional.of(read("config-ca.pem")) : Optional.empty());
    }
    void saveConnection(String uri, String ca) {
        locked(() -> {
            if (uri == null || uri.isBlank() || uri.indexOf('\n') >= 0 || uri.indexOf('\r') >= 0) throw refused();
            var priorUri = configurationUri(); var priorCa = configurationCa();
            if (priorUri.filter(current -> !current.equals(uri)).isPresent()
                    || priorCa.filter(current -> !current.equals(ca)).isPresent()
                    || priorUri.isPresent() && !Objects.equals(priorCa.orElse(null), ca)) throw refused();
            if (ca != null) write("config-ca.pem", ca);
            write("configuration.uri", uri); return null;
        });
    }

    private void write(String name, String value) {
        locked(() -> {
            if (value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_BYTES) throw refused();
            if (exists(name)) check(path(name), false);
            Path temporary = null;
            try {
                temporary = Files.createTempFile(directory, ".installation-", ".tmp", PosixFilePermissions.asFileAttribute(FILE_MODE));
                try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                    var bytes = ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    while (bytes.hasRemaining()) channel.write(bytes);
                    channel.force(true);
                }
                requireOwner(); if (exists(name)) check(path(name), false);
                Files.move(temporary, path(name), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                temporary = null; syncDirectory(); return null;
            } catch (IOException invalid) { throw refused(); }
            finally { if (temporary != null) try { Files.deleteIfExists(temporary); } catch (IOException ignored) { /* Original failure remains authoritative. */ } }
        });
    }

    private String read(String name) {
        check(path(name), false);
        try (var channel = FileChannel.open(path(name), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (channel.size() > MAX_BYTES) throw refused();
            var bytes = ByteBuffer.allocate(MAX_BYTES + 1);
            while (bytes.hasRemaining() && channel.read(bytes) >= 0) { }
            if (!bytes.hasRemaining()) throw refused();
            return new String(bytes.array(), 0, bytes.position(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException invalid) { throw refused(); }
    }
    private void syncDirectory() throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) { channel.force(true); }
    }
    private Path path(String name) { return directory.resolve(name); }
    private boolean exists(String name) { return Files.exists(path(name), LinkOption.NOFOLLOW_LINKS); }
    private static void check(Path path, boolean directory) {
        try {
            var attrs = Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            var owner = path.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(System.getProperty("user.name"));
            if (!attrs.owner().equals(owner) || !(directory ? attrs.isDirectory() : attrs.isRegularFile())
                    || !attrs.permissions().equals(directory ? DIRECTORY_MODE : FILE_MODE)
                    || !directory && ((Number)Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS)).intValue() != 1) throw refused();
        } catch (IOException | UnsupportedOperationException invalid) { throw refused(); }
    }
    private static Map<String,Object> scopeTree(StorageSetupScope scope) {
        var value = new LinkedHashMap<String,Object>(); value.put("formatVersion", 1);
        value.put("clusterId", scope.clusterId()); value.put("installationId", scope.installationId()); value.put("operationId", scope.operationId()); value.put("generation", scope.generation());
        return value;
    }
    private static StorageSetupScope scope(Map<String,Object> value) {
        if (!(value.get("generation") instanceof Long generation)) throw refused();
        try { return new StorageSetupScope(text(value, "clusterId"), text(value, "installationId"), text(value, "operationId"), generation); }
        catch (IllegalArgumentException invalid) { throw refused(); }
    }
    private static String text(Map<String,Object> value, String key) {
        if (!(value.get(key) instanceof String text) || text.isBlank()) throw refused(); return text;
    }
    private static String nullable(Map<String,Object> value, String key) { return value.get(key) == null ? null : text(value, key); }
    private static void version(Map<String,Object> value) { if (!Long.valueOf(1).equals(value.get("formatVersion"))) throw refused(); }
    private static void exact(Map<String,Object> value, String... keys) { if (!value.keySet().equals(Set.of(keys))) throw refused(); }
    private static Map<String,Object> object(String content) {
        Object value;
        try { value = JsonReader.parse(content); } catch (IllegalArgumentException invalid) { throw refused(); }
        if (!(value instanceof Map<?,?> map) || map.keySet().stream().anyMatch(key -> !(key instanceof String))) throw refused();
        @SuppressWarnings("unchecked") var result = (Map<String,Object>)map; return result;
    }
    static TapstateException refused() { return new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null); }
}
