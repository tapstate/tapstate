package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Owner-only, atomic persistence for the non-secret context configuration. */
final class ContextConfigStore {

    private static final Map<Path, Object> JVM_LOCKS = new ConcurrentHashMap<>();

    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS = Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE);
    private static final long MAX_CONFIG_BYTES = 1024L * 1024L;

    private final Path home;
    private final Path root;
    private final Path configFile;
    private final Map<Integer, ContextConfigMigration> migrations;

    private ContextConfigStore(Path home, List<ContextConfigMigration> migrations) {
        this.home = home.toAbsolutePath().normalize();
        this.root = this.home.resolve(".tapstate");
        this.configFile = root.resolve("config.yaml");
        Map<Integer, ContextConfigMigration> byVersion = new LinkedHashMap<>();
        for (ContextConfigMigration migration : List.copyOf(migrations)) {
            if (migration.sourceVersion() == ContextConfig.CURRENT_VERSION
                    || byVersion.putIfAbsent(migration.sourceVersion(), migration) != null) {
                throw new IllegalArgumentException("migration source versions must be unique and non-current");
            }
        }
        this.migrations = Map.copyOf(byVersion);
    }

    static ContextConfigStore underHome(Path home) {
        return underHome(home, List.of());
    }

    static ContextConfigStore underHome(Path home, List<ContextConfigMigration> migrations) {
        return new ContextConfigStore(home, migrations);
    }

    synchronized ContextConfig load() {
        if (!entryExists(root)) {
            return ContextConfig.empty();
        }
        verifyDirectory(root);
        if (!entryExists(configFile)) {
            return ContextConfig.empty();
        }
        verifyFile(configFile);
        String yaml;
        try {
            yaml = readNoFollow(configFile);
        } catch (IOException failure) {
            throw invalid("cannot read file: " + safeReason(failure), failure);
        }
        Map<String, Object> document;
        try {
            document = StrictYaml.parse(yaml);
        } catch (StrictYaml.ParseFailure failure) {
            throw invalid(failure.getMessage(), failure);
        }
        Object versionValue = document.get("version");
        if (!(versionValue instanceof Integer version)) {
            throw invalid("version must be an integer", null);
        }
        if (version != ContextConfig.CURRENT_VERSION) {
            ContextConfigMigration migration = migrations.get(version);
            if (migration == null) {
                throw new TapstateException(CliError.CONTEXT_CONFIG_VERSION,
                        Map.of("path", configFile, "version", version), null);
            }
            try {
                ContextConfig migrated = migration.migrate(Map.copyOf(document));
                if (migrated.version() != ContextConfig.CURRENT_VERSION) {
                    throw new IllegalArgumentException("migration did not produce the current version");
                }
                return migrated;
            } catch (RuntimeException failure) {
                if (failure instanceof TapstateException coded) {
                    throw coded;
                }
                throw invalid("schema migration failed", failure);
            }
        }
        try {
            return decodeCurrent(document);
        } catch (IllegalArgumentException | ClassCastException failure) {
            throw invalid(safeReason(failure), failure);
        }
    }

    synchronized void save(ContextConfig config) {
        if (config.version() != ContextConfig.CURRENT_VERSION) {
            throw new IllegalArgumentException("only the current context configuration can be saved");
        }
        ensureDirectory();
        withConfigLock(() -> saveStrict(config));
    }

    private void saveStrict(ContextConfig config) {
        if (entryExists(configFile)) {
            load();
        }
        byte[] bytes = encode(config).getBytes(StandardCharsets.UTF_8);
        Path temporary = root.resolve(".config.yaml.tmp-" + UUID.randomUUID());
        try {
            createAndSync(temporary, bytes);
            if (entryExists(configFile)) {
                verifyFile(configFile);
            }
            try {
                Files.move(temporary, configFile,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw permissions(configFile, "atomic replacement is not supported", unsupported);
            }
            verifyFile(configFile);
            syncDirectory(root);
        } catch (TapstateException coded) {
            deleteTemporary(temporary);
            throw coded;
        } catch (IOException failure) {
            deleteTemporary(temporary);
            throw permissions(configFile, safeReason(failure), failure);
        }
    }

    private void withConfigLock(LockedOperation operation) {
        Path lockPath = root.resolve(".config.lock");
        Object jvmLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new Object());
        synchronized (jvmLock) {
            FileAttribute<?>[] attributes = supportsPosix(root)
                    ? new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS)}
                    : new FileAttribute<?>[0];
            try (FileChannel channel = FileChannel.open(lockPath,
                    Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                    attributes)) {
                if (!supportsPosix(root)) {
                    installOwnerOnlyAcl(lockPath);
                }
                verifyFile(lockPath);
                try (FileLock ignored = channel.lock()) {
                    operation.run();
                }
            } catch (TapstateException coded) {
                throw coded;
            } catch (IOException | RuntimeException failure) {
                throw permissions(lockPath, safeReason(failure), failure);
            }
        }
    }

    private void ensureDirectory() {
        if (entryExists(root)) {
            verifyDirectory(root);
            return;
        }
        try {
            if (supportsPosix(home)) {
                Files.createDirectory(root, PosixFilePermissions.asFileAttribute(DIRECTORY_PERMISSIONS));
            } else {
                Files.createDirectory(root);
                installOwnerOnlyAcl(root);
            }
            verifyDirectory(root);
            syncDirectory(home);
        } catch (TapstateException coded) {
            throw coded;
        } catch (IOException failure) {
            if (entryExists(root)) {
                verifyDirectory(root);
                return;
            }
            throw permissions(root, safeReason(failure), failure);
        }
    }

    private void createAndSync(Path path, byte[] bytes) throws IOException {
        FileAttribute<?>[] attributes = supportsPosix(root)
                ? new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS)}
                : new FileAttribute<?>[0];
        try (FileChannel channel = FileChannel.open(path,
                Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), attributes)) {
            if (!supportsPosix(root)) {
                installOwnerOnlyAcl(path);
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        verifyFile(path);
    }

    private void verifyDirectory(Path directory) {
        BasicFileAttributes attributes = attributes(directory);
        if (attributes.isSymbolicLink() || !attributes.isDirectory()) {
            throw invalidAt(directory, "path must be a real directory", null);
        }
        verifyOwner(directory, home);
        verifyOwnerOnly(directory, DIRECTORY_PERMISSIONS);
    }

    private void verifyFile(Path file) {
        BasicFileAttributes attributes = attributes(file);
        if (attributes.isSymbolicLink() || !attributes.isRegularFile()) {
            throw invalidAt(file, "path must be a regular file and not a link", null);
        }
        if (attributes.size() > MAX_CONFIG_BYTES) {
            throw invalidAt(file, "file is too large", null);
        }
        verifySingleLink(file);
        verifyOwner(file, root);
        verifyOwnerOnly(file, FILE_PERMISSIONS);
    }

    private BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw invalidAt(path, "cannot inspect path: " + safeReason(failure), failure);
        }
    }

    private void verifySingleLink(Path file) {
        try {
            Object count = Files.getAttribute(file, "unix:nlink", LinkOption.NOFOLLOW_LINKS);
            if (count instanceof Number number && number.longValue() != 1L) {
                throw invalidAt(file, "file must have exactly one hard link", null);
            }
        } catch (UnsupportedOperationException | IllegalArgumentException ignored) {
            // The platform has no Unix link-count view; regular-file and ACL checks remain mandatory.
        } catch (IOException failure) {
            throw invalidAt(file, "cannot inspect hard links: " + safeReason(failure), failure);
        }
    }

    private void verifyOwner(Path path, Path ownerReference) {
        try {
            UserPrincipal actual = Files.getOwner(path, LinkOption.NOFOLLOW_LINKS);
            UserPrincipal expected = Files.getOwner(ownerReference, LinkOption.NOFOLLOW_LINKS);
            if (!actual.equals(expected)) {
                throw permissions(path, "owner differs from the user home", null);
            }
        } catch (IOException failure) {
            throw permissions(path, "cannot verify owner: " + safeReason(failure), failure);
        }
    }

    private void verifyOwnerOnly(Path path, Set<PosixFilePermission> expected) {
        PosixFileAttributeView posix = Files.getFileAttributeView(
                path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        try {
            if (posix != null) {
                Set<PosixFilePermission> actual = posix.readAttributes().permissions();
                if (!actual.equals(expected)) {
                    throw permissions(path, "POSIX mode is not owner-only", null);
                }
                return;
            }
            AclFileAttributeView acl = Files.getFileAttributeView(
                    path, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            if (acl == null) {
                throw permissions(path, "filesystem exposes neither POSIX permissions nor ACLs", null);
            }
            UserPrincipal owner = acl.getOwner();
            for (AclEntry entry : acl.getAcl()) {
                if (entry.type() == AclEntryType.ALLOW && !entry.principal().equals(owner)
                        && !entry.principal().getName().equalsIgnoreCase("SYSTEM")) {
                    throw permissions(path, "ACL grants access outside the owner and SYSTEM", null);
                }
            }
        } catch (IOException failure) {
            throw permissions(path, "cannot verify permissions: " + safeReason(failure), failure);
        }
    }

    private static void installOwnerOnlyAcl(Path path) throws IOException {
        AclFileAttributeView acl = Files.getFileAttributeView(
                path, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (acl == null) {
            throw new IOException("filesystem exposes no owner ACL");
        }
        AclEntry owner = AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(acl.getOwner())
                .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                .build();
        acl.setAcl(List.of(owner));
    }

    private static boolean supportsPosix(Path path) {
        return Files.getFileAttributeView(path, PosixFileAttributeView.class) != null;
    }

    private static boolean entryExists(Path path) {
        return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
    }

    private static String readNoFollow(Path path) throws IOException {
        Set<OpenOption> options = Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        try (var channel = Files.newByteChannel(path, options);
             var output = new ByteArrayOutputStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            while (channel.read(buffer) >= 0) {
                buffer.flip();
                output.write(buffer.array(), 0, buffer.remaining());
                buffer.clear();
                if (output.size() > MAX_CONFIG_BYTES) {
                    throw new IOException("file is too large");
                }
            }
            return output.toString(StandardCharsets.UTF_8);
        }
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void deleteTemporary(Path temporary) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
            // A same-directory temp is never read as configuration and is safe to leave after failure.
        }
    }

    @FunctionalInterface
    private interface LockedOperation {

        void run();
    }

    private ContextConfig decodeCurrent(Map<String, Object> document) {
        requireKeys(document, Set.of("version", "lastContext", "contexts", "workspaceBindings"),
                Set.of("version", "contexts", "workspaceBindings"), "root");
        String last = nullableString(document.get("lastContext"), "lastContext");
        Map<String, Object> rawContexts = stringMap(document.get("contexts"), "contexts");
        Map<String, ContextDefinition> contexts = new TreeMap<>();
        for (Map.Entry<String, Object> entry : rawContexts.entrySet()) {
            Map<String, Object> raw = stringMap(entry.getValue(), "contexts." + entry.getKey());
            requireKeys(raw, Set.of("id", "seeds", "tls", "authRef"),
                    Set.of("id", "seeds", "tls", "authRef"), "contexts." + entry.getKey());
            List<Object> rawSeeds = list(raw.get("seeds"), "seeds");
            List<URI> seeds = new ArrayList<>();
            for (Object seed : rawSeeds) {
                seeds.add(URI.create(string(seed, "seed")));
            }
            Map<String, Object> rawTls = stringMap(raw.get("tls"), "tls");
            requireKeys(rawTls, Set.of("verify"), Set.of("verify"), "tls");
            contexts.put(entry.getKey(), new ContextDefinition(
                    UUID.fromString(string(raw.get("id"), "id")),
                    seeds,
                    new ContextTls(bool(rawTls.get("verify"), "verify")),
                    UUID.fromString(string(raw.get("authRef"), "authRef"))));
        }
        Map<String, Object> rawBindings = stringMap(document.get("workspaceBindings"), "workspaceBindings");
        Map<String, String> bindings = new TreeMap<>();
        rawBindings.forEach((path, context) -> bindings.put(path, string(context, "workspace binding")));
        return new ContextConfig(ContextConfig.CURRENT_VERSION, last, contexts, bindings);
    }

    private static String encode(ContextConfig config) {
        StringBuilder yaml = new StringBuilder("version: 1\n");
        yaml.append("lastContext: ").append(config.lastContext() == null ? "null" : config.lastContext()).append('\n');
        if (config.contexts().isEmpty()) {
            yaml.append("contexts: {}\n");
        } else {
            yaml.append("contexts:\n");
            new TreeMap<>(config.contexts()).forEach((name, context) -> {
                yaml.append("  ").append(name).append(":\n");
                yaml.append("    id: ").append(context.id()).append('\n');
                yaml.append("    seeds:\n");
                context.seeds().forEach(seed -> yaml.append("      - ").append(seed).append('\n'));
                yaml.append("    tls:\n");
                yaml.append("      verify: ").append(context.tls().verify()).append('\n');
                yaml.append("    authRef: ").append(context.authRef()).append('\n');
            });
        }
        if (config.workspaceBindings().isEmpty()) {
            yaml.append("workspaceBindings: {}\n");
        } else {
            yaml.append("workspaceBindings:\n");
            new TreeMap<>(config.workspaceBindings()).forEach((path, context) -> yaml
                    .append("  \"").append(escape(path)).append("\": ").append(context).append('\n'));
        }
        return yaml.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private static Map<String, Object> stringMap(Object value, String field) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException(field + " must be a map");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, item) -> {
            if (!(key instanceof String stringKey)) {
                throw new IllegalArgumentException(field + " keys must be strings");
            }
            result.put(stringKey, item);
        });
        return result;
    }

    private static List<Object> list(Object value, String field) {
        if (!(value instanceof List<?> raw)) {
            throw new IllegalArgumentException(field + " must be a list");
        }
        return new ArrayList<>(raw);
    }

    private static String string(Object value, String field) {
        if (!(value instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException(field + " must be a non-empty string");
        }
        return text;
    }

    private static String nullableString(Object value, String field) {
        return value == null ? null : string(value, field);
    }

    private static boolean bool(Object value, String field) {
        if (!(value instanceof Boolean flag)) {
            throw new IllegalArgumentException(field + " must be a boolean");
        }
        return flag;
    }

    private static void requireKeys(
            Map<String, Object> map, Set<String> allowed, Set<String> required, String field) {
        if (!allowed.containsAll(map.keySet())) {
            Set<String> unknown = new java.util.TreeSet<>(map.keySet());
            unknown.removeAll(allowed);
            throw new IllegalArgumentException(field + " has unknown keys " + unknown);
        }
        if (!map.keySet().containsAll(required)) {
            Set<String> missing = new java.util.TreeSet<>(required);
            missing.removeAll(map.keySet());
            throw new IllegalArgumentException(field + " is missing keys " + missing);
        }
    }

    private TapstateException invalid(String reason, Throwable cause) {
        return invalidAt(configFile, reason, cause);
    }

    private static TapstateException invalidAt(Path path, String reason, Throwable cause) {
        return new TapstateException(CliError.CONTEXT_CONFIG_INVALID,
                Map.of("path", path, "reason", reason), cause);
    }

    private static TapstateException permissions(Path path, String reason, Throwable cause) {
        return new TapstateException(CliError.CONTEXT_CONFIG_PERMISSIONS,
                Map.of("path", path, "reason", reason), cause);
    }

    private static String safeReason(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }
}
