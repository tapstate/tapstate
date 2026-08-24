package io.tapstate.cli;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Owner-only, atomic persistence for opaque CLI auth sessions. */
final class AuthFileStore {

    private static final Map<Path, Object> JVM_LOCKS = new ConcurrentHashMap<>();

    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS = Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE);
    private static final Set<String> AUTH_KEYS = Set.of(
            "version", "authRef", "contextId", "issuer", "principal", "scopes", "sessionToken",
            "createdAt", "idleExpiresAt", "absoluteExpiresAt");
    private static final long MAX_AUTH_BYTES = 1024L * 1024L;

    private final Path home;
    private final Path root;
    private final Path authDir;
    private final DirectorySynchronizer directorySynchronizer;

    private AuthFileStore(Path home) {
        this(home, AuthFileStore::syncDirectory);
    }

    private AuthFileStore(Path home, DirectorySynchronizer directorySynchronizer) {
        this.home = home.toAbsolutePath().normalize();
        this.root = this.home.resolve(".tapstate");
        this.authDir = root.resolve("auth");
        this.directorySynchronizer = directorySynchronizer;
    }

    static AuthFileStore underHome(Path home) {
        return new AuthFileStore(home);
    }

    static AuthFileStore underHome(Path home, DirectorySynchronizer directorySynchronizer) {
        return new AuthFileStore(home, directorySynchronizer);
    }

    synchronized Optional<AuthSessionRecord> load(UUID authRef, UUID contextId) {
        if (!entryExists(authDir)) {
            return Optional.empty();
        }
        verifyDirectory(root);
        verifyDirectory(authDir);
        Path file = authFile(authRef);
        if (!entryExists(file)) {
            return Optional.empty();
        }
        verifyFile(file);
        String json;
        try {
            json = readNoFollow(file);
        } catch (IOException failure) {
            throw invalid(file, "cannot read file: " + safeReason(failure), failure);
        }
        try {
            rejectDuplicateKeys(json);
            AuthSessionRecord record = decode(file, json);
            if (!record.authRef().equals(authRef) || !record.contextId().equals(contextId)) {
                throw new IllegalArgumentException("authRef or contextId does not match the selected context");
            }
            return Optional.of(record);
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException ignored) {
            throw invalid(file, "schema validation failed", null);
        }
    }

    synchronized SaveResult save(AuthSessionRecord record, boolean interactiveTerminal) {
        if (record.version() != AuthSessionRecord.CURRENT_VERSION) {
            throw new IllegalArgumentException("only the current auth cache version can be saved");
        }
        try {
            ensureDirectory(root, home);
            ensureDirectory(authDir, root);
            withAuthLock(record.authRef(), () -> {
                saveStrict(record);
                return null;
            });
            return SaveResult.PERSISTED;
        } catch (TapstateException coded) {
            if (interactiveTerminal && coded.code() == CliError.AUTH_CACHE_PERMISSIONS
                    && !(coded.getCause() instanceof PersistenceOutcomeUncertain)) {
                return SaveResult.MEMORY_ONLY;
            }
            throw coded;
        }
    }

    /** Deletes only the exact session that the caller acted on, never a newer replacement. */
    synchronized DeleteResult delete(AuthSessionRecord expected) {
        if (!entryExists(authDir)) {
            return DeleteResult.ABSENT;
        }
        verifyDirectory(root);
        verifyDirectory(authDir);
        return withAuthLock(expected.authRef(), () -> deleteLocked(expected));
    }

    private DeleteResult deleteLocked(AuthSessionRecord expected) {
        Path file = authFile(expected.authRef());
        Optional<AuthSessionRecord> current = load(expected.authRef(), expected.contextId());
        if (current.isEmpty()) {
            return DeleteResult.ABSENT;
        }
        if (!current.orElseThrow().equals(expected)) {
            return DeleteResult.CHANGED;
        }
        try {
            Files.delete(file);
            directorySynchronizer.sync(authDir);
            return DeleteResult.DELETED;
        } catch (IOException failure) {
            throw permissions(file, safeReason(failure), failure);
        }
    }

    private void saveStrict(AuthSessionRecord record) {
        ensureDirectory(root, home);
        ensureDirectory(authDir, root);
        Path file = authFile(record.authRef());
        AuthSessionRecord previous = entryExists(file)
                ? load(record.authRef(), record.contextId()).orElse(null)
                : null;
        byte[] bytes = encode(record).getBytes(StandardCharsets.UTF_8);
        Path temporary = authDir.resolve(".auth.tmp-" + UUID.randomUUID());
        Path rollback = previous == null ? null : authDir.resolve(".auth.rollback-" + UUID.randomUUID());
        boolean replaced = false;
        try {
            if (rollback != null) {
                createAndSync(rollback, encode(previous).getBytes(StandardCharsets.UTF_8));
            }
            createAndSync(temporary, bytes);
            if (entryExists(file)) {
                verifyFile(file);
            }
            try {
                Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                replaced = true;
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw permissions(file, "atomic replacement is not supported", unsupported);
            }
            verifyFile(file);
            directorySynchronizer.sync(authDir);
            deleteTemporary(rollback);
        } catch (TapstateException coded) {
            deleteTemporary(temporary);
            if (replaced) {
                recoverPrevious(file, rollback);
            } else {
                deleteTemporary(rollback);
            }
            throw coded;
        } catch (IOException failure) {
            deleteTemporary(temporary);
            if (replaced) {
                recoverPrevious(file, rollback);
            } else {
                deleteTemporary(rollback);
            }
            throw permissions(file, safeReason(failure), failure);
        }
    }

    private Path authFile(UUID authRef) {
        Path file = authDir.resolve(authRef + ".json").normalize();
        if (!file.getParent().equals(authDir)) {
            throw invalid(file, "auth file path escapes the auth directory", null);
        }
        return file;
    }

    private Path lockFile(UUID authRef) {
        return authDir.resolve(".auth.lock-" + authRef).normalize();
    }

    private <T> T withAuthLock(UUID authRef, LockedOperation<T> operation) {
        Path lockPath = lockFile(authRef);
        Object jvmLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new Object());
        synchronized (jvmLock) {
            FileAttribute<?>[] attributes = supportsPosix(authDir)
                    ? new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS)}
                    : new FileAttribute<?>[0];
            try (FileChannel channel = FileChannel.open(lockPath,
                    Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                    attributes)) {
                if (!supportsPosix(authDir)) {
                    installOwnerOnlyAcl(lockPath);
                }
                verifyFile(lockPath);
                try (FileLock ignored = channel.lock()) {
                    return operation.run();
                }
            } catch (TapstateException coded) {
                throw coded;
            } catch (IOException | RuntimeException failure) {
                throw permissions(lockPath, safeReason(failure), failure);
            }
        }
    }

    private void ensureDirectory(Path directory, Path ownerReference) {
        if (entryExists(directory)) {
            verifyDirectory(directory);
            return;
        }
        try {
            if (supportsPosix(ownerReference)) {
                Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(DIRECTORY_PERMISSIONS));
            } else {
                Files.createDirectory(directory);
                installOwnerOnlyAcl(directory);
            }
            verifyDirectory(directory);
            directorySynchronizer.sync(ownerReference);
        } catch (TapstateException coded) {
            throw coded;
        } catch (IOException failure) {
            throw permissions(directory, safeReason(failure), failure);
        }
    }

    private void createAndSync(Path path, byte[] bytes) throws IOException {
        FileAttribute<?>[] attributes = supportsPosix(authDir)
                ? new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS)}
                : new FileAttribute<?>[0];
        try (FileChannel channel = FileChannel.open(path,
                Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), attributes)) {
            if (!supportsPosix(authDir)) {
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
            throw invalid(directory, "path must be a real directory", null);
        }
        verifyOwner(directory, home);
        verifyOwnerOnly(directory, DIRECTORY_PERMISSIONS);
    }

    private void verifyFile(Path file) {
        BasicFileAttributes attributes = attributes(file);
        if (attributes.isSymbolicLink() || !attributes.isRegularFile()) {
            throw invalid(file, "path must be a regular file and not a link", null);
        }
        if (attributes.size() > MAX_AUTH_BYTES) {
            throw invalid(file, "file is too large", null);
        }
        verifySingleLink(file);
        verifyOwner(file, home);
        verifyOwnerOnly(file, FILE_PERMISSIONS);
    }

    private BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw invalid(path, "cannot inspect path: " + safeReason(failure), failure);
        }
    }

    private void verifySingleLink(Path file) {
        try {
            Object count = Files.getAttribute(file, "unix:nlink", LinkOption.NOFOLLOW_LINKS);
            if (count instanceof Number number && number.longValue() != 1L) {
                throw invalid(file, "file must have exactly one hard link", null);
            }
        } catch (UnsupportedOperationException | IllegalArgumentException ignored) {
            // Link-count support is platform-specific; regular-file and owner-only checks still apply.
        } catch (IOException failure) {
            throw invalid(file, "cannot inspect hard links: " + safeReason(failure), failure);
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

    private static AuthSessionRecord decode(Path file, String json) {
        Object parsed = JsonReader.parse(json);
        if (!(parsed instanceof Map<?, ?> raw)) {
            throw invalid(file, "root must be an object", null);
        }
        Map<String, Object> document = stringMap(raw, "root");
        requireKeys(document, AUTH_KEYS, AUTH_KEYS, "root");
        int version = version(document.get("version"));
        if (version != AuthSessionRecord.CURRENT_VERSION) {
            throw new TapstateException(CliError.AUTH_CACHE_VERSION,
                    Map.of("path", file, "version", version), null);
        }
        return new AuthSessionRecord(
                version,
                UUID.fromString(string(document.get("authRef"), "authRef")),
                UUID.fromString(string(document.get("contextId"), "contextId")),
                string(document.get("issuer"), "issuer"),
                string(document.get("principal"), "principal"),
                stringList(document.get("scopes"), "scopes"),
                string(document.get("sessionToken"), "sessionToken"),
                Instant.parse(string(document.get("createdAt"), "createdAt")),
                Instant.parse(string(document.get("idleExpiresAt"), "idleExpiresAt")),
                Instant.parse(string(document.get("absoluteExpiresAt"), "absoluteExpiresAt")));
    }

    private static String encode(AuthSessionRecord record) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("version", record.version());
        document.put("authRef", record.authRef().toString());
        document.put("contextId", record.contextId().toString());
        document.put("issuer", record.issuer());
        document.put("principal", record.principal());
        document.put("scopes", record.scopes());
        document.put("sessionToken", record.sessionToken());
        document.put("createdAt", record.createdAt().toString());
        document.put("idleExpiresAt", record.idleExpiresAt().toString());
        document.put("absoluteExpiresAt", record.absoluteExpiresAt().toString());
        return JsonWriter.write(document);
    }

    private static Map<String, Object> stringMap(Map<?, ?> raw, String field) {
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (!(key instanceof String text)) {
                throw new IllegalArgumentException(field + " keys must be strings");
            }
            result.put(text, value);
        });
        return result;
    }

    private static List<String> stringList(Object value, String field) {
        if (!(value instanceof List<?> raw)) {
            throw new IllegalArgumentException(field + " must be a list");
        }
        List<String> result = new ArrayList<>();
        for (Object entry : raw) {
            result.add(string(entry, field + " item"));
        }
        return result;
    }

    private static int version(Object value) {
        if (value instanceof Integer version) {
            return version;
        }
        if (!(value instanceof Long version)) {
            throw new IllegalArgumentException("version must be an integer");
        }
        try {
            return Math.toIntExact(version);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("version must be an integer");
        }
    }

    private static String string(Object value, String field) {
        if (!(value instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException(field + " must be a non-empty string");
        }
        return text;
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

    private static void rejectDuplicateKeys(String json) {
        new DuplicateKeyScanner(json).scan();
    }

    private static boolean supportsPosix(Path path) {
        return Files.getFileAttributeView(path, PosixFileAttributeView.class) != null;
    }

    private static boolean entryExists(Path path) {
        return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
    }

    private static String readNoFollow(Path path) throws IOException {
        try (var channel = Files.newByteChannel(path, Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
             var output = new ByteArrayOutputStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            while (channel.read(buffer) >= 0) {
                buffer.flip();
                output.write(buffer.array(), 0, buffer.remaining());
                buffer.clear();
                if (output.size() > MAX_AUTH_BYTES) {
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

    private static void deleteTemporary(Path temporary) {
        if (temporary == null) {
            return;
        }
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
            // A same-directory temp is never selected by authRef and can be left after a failed write.
        }
    }

    private void recoverPrevious(Path file, Path rollback) {
        if (rollback == null) {
            discardReplacement(file);
            return;
        }
        try {
            Files.move(rollback, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            verifyFile(file);
            directorySynchronizer.sync(authDir);
        } catch (TapstateException | IOException failure) {
            throw persistenceUncertain(file);
        }
    }

    private void discardReplacement(Path file) {
        try {
            Files.deleteIfExists(file);
            directorySynchronizer.sync(authDir);
        } catch (IOException ignored) {
            throw persistenceUncertain(file);
        }
    }

    private static TapstateException persistenceUncertain(Path file) {
        return permissions(file,
                "auth cache persistence outcome cannot be confirmed; remove the cache file before retrying",
                new PersistenceOutcomeUncertain());
    }

    private static TapstateException invalid(Path path, String reason, Throwable cause) {
        return new TapstateException(CliError.AUTH_CACHE_INVALID,
                Map.of("path", path, "reason", reason), cause);
    }

    private static TapstateException permissions(Path path, String reason, Throwable cause) {
        return new TapstateException(CliError.AUTH_CACHE_PERMISSIONS,
                Map.of("path", path, "reason", reason), cause);
    }

    private static String safeReason(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }

    enum SaveResult {
        PERSISTED,
        MEMORY_ONLY
    }

    enum DeleteResult {
        DELETED,
        ABSENT,
        CHANGED
    }

    @FunctionalInterface
    private interface LockedOperation<T> {

        T run();
    }

    @FunctionalInterface
    interface DirectorySynchronizer {

        void sync(Path directory) throws IOException;
    }

    private static final class PersistenceOutcomeUncertain extends IOException {
    }

    private static final class DuplicateKeyScanner {

        private final String source;
        private int position;

        private DuplicateKeyScanner(String source) {
            this.source = source;
        }

        void scan() {
            skipWhitespace();
            scanValue();
            skipWhitespace();
            if (!atEnd()) {
                throw new IllegalArgumentException("trailing content after JSON value");
            }
        }

        private void scanValue() {
            skipWhitespace();
            if (atEnd()) {
                throw new IllegalArgumentException("unexpected end of input");
            }
            char c = source.charAt(position);
            switch (c) {
                case '{' -> scanObject();
                case '[' -> scanArray();
                case '"' -> readString();
                case 't' -> readLiteral("true");
                case 'f' -> readLiteral("false");
                case 'n' -> readLiteral("null");
                default -> readNumber();
            }
        }

        private void scanObject() {
            expect('{');
            Set<String> keys = new HashSet<>();
            skipWhitespace();
            if (peek('}')) {
                position++;
                return;
            }
            while (true) {
                skipWhitespace();
                String key = readString();
                if (!keys.add(key)) {
                    throw new IllegalArgumentException("duplicate key " + key);
                }
                skipWhitespace();
                expect(':');
                scanValue();
                skipWhitespace();
                if (peek('}')) {
                    position++;
                    return;
                }
                expect(',');
            }
        }

        private void scanArray() {
            expect('[');
            skipWhitespace();
            if (peek(']')) {
                position++;
                return;
            }
            while (true) {
                scanValue();
                skipWhitespace();
                if (peek(']')) {
                    position++;
                    return;
                }
                expect(',');
            }
        }

        private String readString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (true) {
                if (atEnd()) {
                    throw new IllegalArgumentException("unterminated string");
                }
                char c = source.charAt(position++);
                if (c == '"') {
                    return out.toString();
                }
                if (c == '\\') {
                    out.append(readEscape());
                } else {
                    out.append(c);
                }
            }
        }

        private char readEscape() {
            if (atEnd()) {
                throw new IllegalArgumentException("unterminated escape");
            }
            char c = source.charAt(position++);
            return switch (c) {
                case '"', '\\', '/' -> c;
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> readUnicodeEscape();
                default -> throw new IllegalArgumentException("invalid string escape");
            };
        }

        private char readUnicodeEscape() {
            if (position + 4 > source.length()) {
                throw new IllegalArgumentException("truncated unicode escape");
            }
            String hex = source.substring(position, position + 4);
            position += 4;
            try {
                return (char) Integer.parseInt(hex, 16);
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException("invalid unicode escape");
            }
        }

        private void readLiteral(String literal) {
            if (!source.startsWith(literal, position)) {
                throw new IllegalArgumentException("invalid literal");
            }
            position += literal.length();
        }

        private void readNumber() {
            int start = position;
            while (!atEnd()) {
                char c = source.charAt(position);
                if (c == ',' || c == '}' || c == ']' || Character.isWhitespace(c)) {
                    break;
                }
                position++;
            }
            if (start == position) {
                throw new IllegalArgumentException("expected JSON value");
            }
        }

        private void skipWhitespace() {
            while (!atEnd()) {
                char c = source.charAt(position);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    position++;
                } else {
                    return;
                }
            }
        }

        private void expect(char expected) {
            if (atEnd() || source.charAt(position) != expected) {
                throw new IllegalArgumentException("expected '" + expected + "'");
            }
            position++;
        }

        private boolean peek(char expected) {
            return !atEnd() && source.charAt(position) == expected;
        }

        private boolean atEnd() {
            return position >= source.length();
        }
    }
}
