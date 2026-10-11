package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;

/** Strict inverse of the bounded diagnostic ASCII manifests; qualification is evaluated elsewhere. */
final class BenchmarkReturnBindingsManifestReader {
    private static final SourceRole[] SOURCES = SourceRole.values();
    private static final Owner[] OWNERS = Owner.values();
    private static final JdkRole[] JDK = JdkRole.values();
    private static final int MAX_FIELDS = 14 + MAX_CLASSPATH + 4 * SOURCES.length + 4 * MAX_CLASSES + 16 * MAX_CLASSES;
    private static final int WRITER_FIELDS = 11, MAX_WRITER_FIELDS = MAX_FIELDS + WRITER_FIELDS;
    private static final int BUFFER = 8192;
    @FunctionalInterface interface Streams { InputStream open(Path path) throws IOException; }
    private static final Streams FILES = path -> Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
    @FunctionalInterface private interface Decode<T> { T read(Cursor cursor); }
    @FunctionalInterface private interface Encode<T> { byte[] write(T value); }

    private BenchmarkReturnBindingsManifestReader() { }

    static HelperManifest readHelper(Path path, Blob prior) throws IOException { return readHelper(path, prior, FILES); }
    static JdkManifest readJdk(Path path, Blob prior) throws IOException { return readJdk(path, prior, FILES); }
    static HelperManifest readHelper(Path path, Blob prior, Streams streams) throws IOException {
        return read(path, prior, streams, BenchmarkReturnBindingsManifestReader::helper, BenchmarkReturnWorkBudgetBindingsIo::helperBytes);
    }
    static JdkManifest readJdk(Path path, Blob prior, Streams streams) throws IOException {
        return read(path, prior, streams, BenchmarkReturnBindingsManifestReader::jdk, BenchmarkReturnWorkBudgetBindingsIo::jdkBytes);
    }
    private static <T> T read(Path path, Blob prior, Streams streams, Decode<T> decoder, Encode<T> encoder) throws IOException {
        var scope = BenchmarkReturnWorkBudget.current();
        var ticket = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.DECODE, BenchmarkReturnWorkBudget.Work.call());
        try {
            byte[] raw = bytes(path, prior, streams, scope); var cursor = new Cursor(raw);
            T value = decoder.read(cursor); require(cursor.at == raw.length, "extra or trailing field bytes");
            byte[] encoded = encoder.write(value);
            require(BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.COPY,
                    () -> Arrays.equals(raw, encoded)), "canonical re-encoding differs");
            BenchmarkReturnWorkBudget.done(ticket, new BenchmarkReturnWorkBudget.Work(0, 0, 0, 0,
                    raw.length, 1, Math.addExact(raw.length, cursor.fields), cursor.fields));
            return value;
        } catch (IOException | RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failure(scope, failure); BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure;
        }
    }

    private static HelperManifest helper(Cursor c) {
        String header = c.field(32, false);
        require(header.equals("HELPER_BINDINGS_V1") || header.equals("HELPER_BINDINGS_V2"), "helper header or field order differs");
        int schema = c.count(PRODUCER_WRITER_SCHEMA_VERSION);
        require(header.equals("HELPER_BINDINGS_V1") && schema == SCHEMA_VERSION
                || header.equals("HELPER_BINDINGS_V2") && schema == PRODUCER_WRITER_SCHEMA_VERSION,
                "helper header or field order differs");
        String run = c.field(128, false), input = c.digest(), build = c.field(128, false), definition = c.field(128, false);
        String root = c.digest(), producer = c.digest(); Blob xml = c.blob(Long.MAX_VALUE - 1);
        int count = c.count(MAX_CLASSPATH); var classpath = new ArrayList<String>(count); var paths = new HashSet<String>();
        for (int i = 0; i < count; i++) {
            String path = c.field(4096, false); absolute(path); require(paths.add(path), "classpath origin repeated"); classpath.add(path);
        }
        count = c.count(SOURCES.length); var sources = new ArrayList<Source>(count); var sourceRoles = new HashSet<SourceRole>();
        for (int i = 0; i < count; i++) {
            SourceRole role = c.source(); require(sourceRoles.add(role), "source role repeated");
            String path = c.field(512, false); require(path.equals(role.sourcePath()), "source relative path differs");
            sources.add(new Source(role, path, c.blob(MAX_MEMBER_BYTES)));
        }
        count = c.count(MAX_CLASSES); var classes = new ArrayList<CompiledClass>(count); var members = new HashMap<String, CompiledClass>();
        for (int i = 0; i < count; i++) {
            SourceRole role = c.source(); String binary = c.field(256, false); binary(role, binary);
            Blob blob = c.blob(MAX_MEMBER_BYTES); require(!members.containsKey(binary), "compiled member repeated");
            var compiled = new CompiledClass(role, binary, blob); members.put(binary, compiled); classes.add(compiled);
        }
        ProducerWriterBinding producerWriter = null;
        if (schema == PRODUCER_WRITER_SCHEMA_VERSION) {
            require(c.count(1) == 1, "producer writer tuple is missing");
            Owner owner = c.owner(); String sourceRoot = c.field(4096, false); absolute(sourceRoot);
            var source = new Source(c.source(), c.field(512, false), c.blob(MAX_MEMBER_BYTES));
            var compiled = new CompiledClass(c.source(), c.field(256, false), c.blob(MAX_MEMBER_BYTES));
            producerWriter = new ProducerWriterBinding(owner, sourceRoot, source, compiled);
            require(members.containsKey(SourceRole.PDK_WRITER.binaryName()), "root writer compiled fact is missing");
        }
        count = c.count(2 * MAX_CLASSES); var origins = new ArrayList<Origin>(count); var originKeys = new HashSet<String>();
        for (int i = 0; i < count; i++) {
            Owner owner = c.owner(); SourceRole role = c.source(); String binary = c.field(256, false); binary(role, binary);
            String container = c.field(4096, false); absolute(container); String entry = c.field(1024, false);
            Blob blob = c.blob(MAX_MEMBER_BYTES); String app = c.field(64, true); if (!app.isEmpty()) { digest(app); }
            var compiled = compiledForOwner(members, owner, binary, producerWriter);
            require(compiled != null && compiled.role() == role && compiled.blob().equals(blob) && role.owners().contains(owner), "origin source owner or compiled bytes differ");
            require(originKeys.add(owner.name() + ":" + binary), "owned origin repeated");
            require(owner == Owner.ROOT ? app.isEmpty() : !app.isEmpty(), "origin application digest kind differs");
            origins.add(new Origin(owner, role, binary, container, entry, blob, app.isEmpty() ? null : app));
        }
        return new HelperManifest(schema, run, input, build, definition, root, producer, xml, classpath, sources, classes, origins, producerWriter);
    }

    private static JdkManifest jdk(Cursor c) {
        require(c.field(32, false).equals("JDK_BINDINGS_V1"), "JDK header or field order differs");
        int schema = c.count(1); require(schema == SCHEMA_VERSION, "JDK schema differs");
        String run = c.field(128, false), input = c.digest(), home = c.field(4096, false), version = c.field(128, false); absolute(home);
        int count = c.count(JDK.length); var files = new ArrayList<JdkFile>(count); var roles = new HashSet<JdkRole>();
        for (int i = 0; i < count; i++) {
            JdkRole role = c.jdk(); require(roles.add(role), "JDK role repeated"); String path = c.field(4096, false); absolute(path);
            require(path.equals(Path.of(home).resolve(role.relativePath()).toString()), "JDK role path differs");
            files.add(new JdkFile(role, path, c.blob(Long.MAX_VALUE - 1)));
        }
        return new JdkManifest(schema, run, input, home, version, files);
    }

    private static final class Cursor {
        final byte[] raw; int at, fields;
        Cursor(byte[] raw) { this.raw = raw; }
        String field(int maximum, boolean empty) {
            require(++fields <= MAX_WRITER_FIELDS && at < raw.length, "field roster missing or exceeds cap");
            int first = at, length = 0, digits = 0;
            while (at < raw.length && raw[at] >= '0' && raw[at] <= '9') {
                require(++digits <= 4, "field length prefix exceeds bound");
                require(digits == 1 || raw[first] != '0', "noncanonical leading field length");
                length = Math.addExact(Math.multiplyExact(length, 10), raw[at++] - '0');
                require(length <= maximum && length <= 4096, "field length exceeds prior cap");
            }
            require(digits > 0 && at < raw.length && raw[at++] == ':', "field length delimiter differs");
            require(empty || length > 0, "required field is empty");
            require(length < raw.length - at, "field is truncated before allocation");
            int end = Math.addExact(at, length);
            for (int i = at; i < end; i++) { require(raw[i] >= 32 && raw[i] <= 126, "field is not printable ASCII"); }
            require(raw[end] == '\n', "field newline delimiter differs");
            String value = new String(raw, at, length, StandardCharsets.US_ASCII); at = end + 1; return value;
        }
        long number(long maximum) {
            String value = field(19, false); require(value.length() == 1 || value.charAt(0) != '0', "noncanonical leading numeric value");
            long number = 0;
            for (int i = 0; i < value.length(); i++) {
                char digit = value.charAt(i); require(digit >= '0' && digit <= '9', "numeric type differs");
                try { number = Math.addExact(Math.multiplyExact(number, 10), digit - '0'); }
                catch (ArithmeticException overflow) { throw new AssertionError("manifest numeric value overflowed", overflow); }
            }
            require(number <= maximum, "numeric value exceeds prior cap"); return number;
        }
        int count(int maximum) { return Math.toIntExact(number(maximum)); }
        String digest() { String value = field(64, false); BenchmarkReturnBindingsManifestReader.digest(value); return value; }
        Blob blob(long maximum) { String sha = digest(); long bytes = number(maximum); require(bytes > 0, "byte fact is not positive"); return new Blob(sha, bytes); }
        SourceRole source() { String text = field(64, false); for (SourceRole role : SOURCES) { if (role.name().equals(text)) { return role; } } throw new AssertionError("manifest source enum differs"); }
        Owner owner() { String text = field(32, false); for (Owner owner : OWNERS) { if (owner.name().equals(text)) { return owner; } } throw new AssertionError("manifest owner enum differs"); }
        JdkRole jdk() { String text = field(32, false); for (JdkRole role : JDK) { if (role.name().equals(text)) { return role; } } throw new AssertionError("manifest JDK enum differs"); }
    }

    private static byte[] bytes(Path path, Blob prior, Streams streams, BenchmarkReturnWorkBudget.Scope scope) throws IOException {
        require(path != null && path.isAbsolute() && path.equals(path.normalize()) && prior != null && streams != null
                && prior.bytes() <= MAX_MANIFEST_BYTES, "file identity or prior manifest cap differs");
        require(!external(scope, () -> Files.isSymbolicLink(path)), "linked manifest refused");
        require(path.equals(external(scope, path::toRealPath)) && external(scope, () -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)), "manifest path is not canonical regular file");
        require(external(scope, () -> Files.size(path)) == prior.bytes(), "manifest size differs from prior bytes");
        InputStream input = external(scope, () -> streams.open(path)); Throwable primary = null;
        try {
            int capacity = Math.toIntExact(prior.bytes()); byte[] raw = new byte[capacity];
            BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY, new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, capacity, 1, 1, 0));
            MessageDigest digest = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> {
                try { return MessageDigest.getInstance("SHA-256"); }
                catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
            });
            int at = 0;
            while (at < raw.length) {
                int offset = at, limit = Math.min(BUFFER, raw.length - at);
                int read = external(scope, () -> input.read(raw, offset, limit));
                require(read > 0 && read <= limit, "manifest read is truncated or makes no progress");
                at = Math.addExact(at, read); consume(scope, read);
                BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        () -> { digest.update(raw, offset, read); return null; });
                BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, BenchmarkReturnWorkBudget.Work.bytes(read));
            }
            int extra = external(scope, input::read); if (extra >= 0) { consume(scope, 1); }
            require(extra < 0, "manifest exceeds its prior byte count");
            byte[] hashed = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, digest::digest);
            String sha = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> HexFormat.of().formatHex(hashed));
            require(sha.equals(prior.sha256()) && external(scope, () -> Files.size(path)) == prior.bytes(), "actual manifest digest or size differs");
            require(path.equals(external(scope, path::toRealPath)), "manifest canonical path changed");
            return raw;
        } catch (IOException | RuntimeException | Error failure) { primary = failure; BenchmarkReturnWorkBudget.failure(scope, failure); throw failure; }
        finally {
            try { external(scope, () -> { input.close(); return null; }); }
            catch (IOException | RuntimeException | Error close) {
                if (primary == null) { throw close; }
                if (primary != close) { primary.addSuppressed(close); }
            }
        }
    }
    private static <T> T external(BenchmarkReturnWorkBudget.Scope scope, BenchmarkReturnWorkBudget.CheckedAction<T, IOException> action) throws IOException {
        return BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, action);
    }
    private static void consume(BenchmarkReturnWorkBudget.Scope scope, long bytes) {
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, BenchmarkReturnWorkBudget.Work.bytes(bytes));
    }

    /** Inclusive source/JDK API bounds per reader entry, not observed service time or VM instructions. */
    static BenchmarkReturnWorkBudget.Work decodingWorkUpper(long byteCap, long fieldCap) {
        limits(byteCap, fieldCap); long enums = Math.addExact(SOURCES.length, Math.addExact(OWNERS.length, JDK.length));
        long requests = Math.addExact(1024, Math.addExact(Math.multiplyExact(32, byteCap),
                Math.multiplyExact(Math.addExact(128, Math.multiplyExact(8, enums)), fieldCap)));
        return new BenchmarkReturnWorkBudget.Work(Math.addExact(128, Math.multiplyExact(32, fieldCap)), requests, 0, 0,
                Math.addExact(Math.multiplyExact(4, byteCap), Math.multiplyExact(256, fieldCap)), 8,
                Math.addExact(Math.multiplyExact(8, byteCap), Math.multiplyExact(64, fieldCap)), Math.multiplyExact(32, fieldCap));
    }
    /** Includes existing encoder field production, enum/blob/accessor/list calls and both emitted-character loops. */
    static BenchmarkReturnWorkBudget.Work encodingWorkUpper(long byteCap, long fieldCap) {
        limits(byteCap, fieldCap);
        return new BenchmarkReturnWorkBudget.Work(Math.addExact(128, Math.multiplyExact(16, fieldCap)),
                Math.addExact(512, Math.addExact(Math.multiplyExact(16, byteCap), Math.multiplyExact(128, fieldCap))), 0, 0,
                Math.addExact(Math.multiplyExact(4, byteCap), Math.multiplyExact(128, fieldCap)), 4,
                Math.addExact(Math.multiplyExact(4, byteCap), Math.multiplyExact(16, fieldCap)), Math.multiplyExact(8, fieldCap));
    }
    static int maximumFields() { return MAX_FIELDS; }
    static int maximumFields(int schema) {
        require(schema == SCHEMA_VERSION || schema == PRODUCER_WRITER_SCHEMA_VERSION, "helper schema differs");
        return schema == SCHEMA_VERSION ? MAX_FIELDS : MAX_WRITER_FIELDS;
    }
    private static void limits(long bytes, long fields) {
        require(bytes > 0 && bytes <= MAX_MANIFEST_BYTES && fields > 0 && fields <= MAX_WRITER_FIELDS, "independent source work caps differ");
    }
    private static void absolute(String value) { Path path = Path.of(value); require(path.isAbsolute() && path.equals(path.normalize()), "typed path is not absolute and normalized"); }
    private static void binary(SourceRole role, String value) {
        String prefix = role.binaryName(); require(prefix != null && (value.equals(prefix)
                || value.startsWith(prefix + "$") && value.substring(prefix.length() + 1).matches("[A-Za-z0-9_$]+")), "binary source owner differs");
    }
    private static void digest(String value) {
        require(value.length() == 64, "digest length differs");
        for (int i = 0; i < value.length(); i++) { char c = value.charAt(i); require(c >= '0' && c <= '9' || c >= 'a' && c <= 'f', "digest type differs"); }
    }
    private static void require(boolean value, String reason) { if (!value) { throw new AssertionError("manifest reader " + reason); } }
}
