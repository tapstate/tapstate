package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;

/** Bounded byte authentication; runtime ownership and emitted-member completeness remain separate. */
final class BenchmarkReturnWorkBudgetBindingsIo {
    private static final int BUFFER_BYTES = 8192;
    private static final long ROOT_ARCHIVE_BYTES = BenchmarkReturnWorkBudget.MAX_CONNECTOR_INPUT_BYTES;

    @FunctionalInterface interface Streams { InputStream open(Path path) throws IOException; }
    private static final Streams FILES = path -> Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);

    record Result(List<FileFact> files, List<Source> sources, List<CompiledClass> classes,
                  List<Origin> origins, List<JdkFile> jdkFiles, Map<String, Object> evidence) {
        Result {
            files = List.copyOf(files); sources = List.copyOf(sources); classes = List.copyOf(classes);
            origins = List.copyOf(origins); jdkFiles = List.copyOf(jdkFiles);
            evidence = Collections.unmodifiableMap(new LinkedHashMap<>(evidence));
        }
    }

    private final BenchmarkReturnWorkBudget.Scope scope;
    private final Streams streams;
    private long requests, consumedBytes, hashBytes, zipEntries;
    private record Read(String sha256, long bytes) {
        Blob blob() { require(sha256 != null && bytes > 0, "declared material is empty"); return new Blob(sha256, bytes); }
    }

    private BenchmarkReturnWorkBudgetBindingsIo(Streams streams) {
        require(streams != null, "stream source missing"); this.streams = streams; scope = BenchmarkReturnWorkBudget.current();
    }

    static Result authenticate(Path sourceRoot, Path application, List<FileFact> files,
                               HelperManifest helper, JdkManifest jdk) throws IOException {
        return authenticate(sourceRoot, application, files, helper, jdk, FILES);
    }
    static Result authenticate(Path sourceRoot, Path application, List<FileFact> files,
                               HelperManifest helper, JdkManifest jdk, Streams streams) throws IOException {
        var reader = new BenchmarkReturnWorkBudgetBindingsIo(streams);
        try { return reader.read(sourceRoot, application, files, helper, jdk); }
        catch (IOException | RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failure(reader.scope, failure); throw failure; }
    }

    private Result read(Path sourceRoot, Path application, List<FileFact> declared, HelperManifest helper, JdkManifest jdk) throws IOException {
        require(helper != null && jdk != null && declared != null && declared.size() <= BenchmarkReturnWorkBudgetProfile.FileRole.values().length,
                "declared inputs missing or exceed roster cap");
        require(helper.schemaVersion() == SCHEMA_VERSION && jdk.schemaVersion() == SCHEMA_VERSION
                && helper.runId().equals(jdk.runId()) && helper.inputSha256().equals(jdk.inputSha256()), "manifest identity or schema differs");
        Path root = directory(sourceRoot);
        var fileMap = new EnumMap<BenchmarkReturnWorkBudgetProfile.FileRole, FileFact>(BenchmarkReturnWorkBudgetProfile.FileRole.class);
        for (var file : declared) { require(fileMap.put(file.role(), file) == null, "file role repeated"); }
        var app = fileMap.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR);
        var helperFile = fileMap.get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST);
        var jdkFile = fileMap.get(BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST);
        require(app != null && helperFile != null && jdkFile != null && application != null
                && application.equals(Path.of(app.path())), "application or manifest file role missing");

        byte[] helperRaw = helperBytes(helper), jdkRaw = jdkBytes(jdk);
        require(helperFile.blob().equals(hash(helperRaw)) && jdkFile.blob().equals(hash(jdkRaw)), "manifest object does not match registered bytes");
        var actualFiles = new ArrayList<FileFact>();
        for (var file : declared) {
            long cap = file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST
                    || file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST ? MAX_MANIFEST_BYTES : file.blob().bytes();
            Blob actual = file(Path.of(file.path()), file.blob(), cap);
            actualFiles.add(new FileFact(file.role(), file.path(), actual));
        }
        var actualSources = new ArrayList<Source>(); var sourceRoles = new HashSet<SourceRole>();
        for (var source : helper.sources()) {
            require(sourceRoles.add(source.role()) && source.path().equals(source.role().sourcePath()), "source role or exact relative path differs");
            entry(source.path());
            Path path = root.resolve(source.path()).normalize(); require(path.startsWith(root), "source escapes registered root");
            actualSources.add(new Source(source.role(), source.path(), file(path, source.blob(), MAX_MEMBER_BYTES)));
        }
        var compiled = new HashMap<String, CompiledClass>();
        for (var item : helper.classes()) {
            require(compiled.put(item.binaryName(), item) == null, "compiled member repeated");
            String owner = item.role().binaryName();
            require(owner != null && (item.binaryName().equals(owner) || item.binaryName().startsWith(owner + "$"))
                    && item.blob().bytes() <= MAX_MEMBER_BYTES, "compiled source owner or prior byte cap differs");
        }
        var originKeys = new HashSet<String>(); var origins = new ArrayList<Origin>();
        var modules = new LinkedHashMap<String, List<Origin>>();
        for (var origin : helper.origins()) {
            var item = compiled.get(origin.binaryName());
            require(item != null && item.role() == origin.role() && item.blob().equals(origin.classBytes())
                    && origin.role().owners().contains(origin.owner()), "declared class source owner differs");
            require(originKeys.add(origin.owner().name() + ":" + origin.binaryName()), "declared owned origin repeated");
            String member = origin.binaryName().replace('.', '/') + ".class";
            if (origin.owner() == Owner.ROOT) {
                require(helper.failsafeClasspath().contains(origin.container()) && origin.entry().equals(member)
                        && origin.applicationSha256() == null, "root origin or class entry differs");
                entry(origin.entry());
                Path container = canonical(Path.of(origin.container()));
                Blob actual;
                if (request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.isDirectory(container, LinkOption.NOFOLLOW_LINKS))) {
                    actual = file(container.resolve(member), origin.classBytes(), MAX_MEMBER_BYTES);
                } else {
                    requireRegular(container, ROOT_ARCHIVE_BYTES);
                    actual = rootArchive(container, member, origin.classBytes());
                }
                origins.add(new Origin(origin.owner(), origin.role(), origin.binaryName(), origin.container(), origin.entry(), actual, null));
            } else {
                require(origin.container().equals(app.path()) && app.blob().sha256().equals(origin.applicationSha256()), "producer application identity differs");
                int delimiter = origin.entry().indexOf("!/");
                require(delimiter > 0 && delimiter == origin.entry().lastIndexOf("!/")
                        && origin.entry().substring(delimiter + 2).equals(member), "producer class entry differs");
                String module = origin.entry().substring(0, delimiter); entry(module); entry(member);
                require(module.startsWith("BOOT-INF/lib/" + origin.role().module() + "-") && module.endsWith(".jar"), "foreign producer module entry");
                modules.computeIfAbsent(module, ignored -> new ArrayList<>()).add(origin);
            }
        }
        if (!modules.isEmpty()) { origins.addAll(producerArchive(application, app.blob().bytes(), modules)); }
        for (var item : compiled.values()) {
            for (var owner : item.role().owners()) {
                require(originKeys.contains(owner.name() + ":" + item.binaryName()), "compiled member has no declared owned origin");
            }
        }
        var jdkFiles = new ArrayList<JdkFile>(); var jdkRoles = new HashSet<JdkRole>();
        Path home = directory(Path.of(jdk.javaHome()));
        for (var file : jdk.files()) {
            require(jdkRoles.add(file.role()) && file.path().equals(home.resolve(file.role().relativePath()).toString()), "JDK role or exact path differs");
            jdkFiles.add(new JdkFile(file.role(), file.path(), file(Path.of(file.path()), file.blob(), file.blob().bytes())));
        }
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("schemaVersion", 1); evidence.put("state", "DECLARED_BYTE_FACTS_AUTHENTICATED");
        evidence.put("manifestEncoding", "CANONICAL_ASCII_LENGTH_PREFIXED_V1"); evidence.put("requests", requests);
        evidence.put("consumedBytes", consumedBytes); evidence.put("hashedBytes", hashBytes); evidence.put("zipEntries", zipEntries);
        evidence.put("scope", "DECLARED_SOURCE_CLASS_APPLICATION_ENTRY_JDK_AND_MANIFEST_BYTES_ONLY");
        evidence.put("runtimeOriginAuthenticationQualified", false); evidence.put("emittedMemberCompletenessQualified", false);
        evidence.put("runtimeInstructionBytesQualified", false); evidence.put("structuralWorkBudgetQualified", false);
        evidence.put("wholeMethodCostQualified", false); evidence.put("causalOverheadQualified", false);
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> evidence.put(flag, false));
        return new Result(actualFiles, actualSources, helper.classes(), origins, jdkFiles, evidence);
    }

    private Blob file(Path path, Blob prior, long cap) throws IOException {
        require(prior.bytes() <= cap, "declared file exceeds prior byte cap");
        Path actual = canonical(path); requireRegular(actual, cap);
        require(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.size(actual)) == prior.bytes(), "actual file size differs from prior bytes");
        InputStream input = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> streams.open(actual));
        return closing(input, () -> {
            Blob read = digest(input, prior.bytes(), true).blob();
            require(read.equals(prior), "actual byte digest or length differs");
            canonical(actual); require(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.size(actual)) == prior.bytes(), "actual file changed while read");
            return read;
        });
    }

    private Blob rootArchive(Path container, String member, Blob prior) throws IOException {
        ZipFile zip = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> new ZipFile(container.toFile()));
        return closing(zip, () -> {
            uniqueEntries(zip, ROOT_ARCHIVE_BYTES);
            ZipEntry item = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> zip.getEntry(member));
            require(item != null && !item.isDirectory() && item.getSize() >= 0 && item.getSize() <= MAX_MEMBER_BYTES, "root archive class missing or exceeds prior cap");
            require(item.getSize() == prior.bytes(), "root archive class size differs");
            InputStream input = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> zip.getInputStream(item));
            return closing(input, () -> { Blob bytes = digest(input, prior.bytes(), true).blob(); require(bytes.equals(prior), "root archive class digest differs"); return bytes; });
        });
    }

    private List<Origin> producerArchive(Path application, long cap, Map<String, List<Origin>> modules) throws IOException {
        ZipFile zip = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> new ZipFile(application.toFile()));
        return closing(zip, () -> {
            uniqueEntries(zip, cap);
            var origins = new ArrayList<Origin>();
            for (var module : modules.entrySet()) {
                ZipEntry item = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> zip.getEntry(module.getKey()));
                require(item != null && !item.isDirectory() && item.getSize() >= 0 && item.getSize() <= cap, "nested module missing or exceeds outer prior byte cap");
                InputStream raw = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> zip.getInputStream(item));
                origins.addAll(closing(raw, () -> {
                    var bounded = new Limited(raw, cap);
                    ZipInputStream nested = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> new ZipInputStream(bounded));
                    return closing(nested, () -> nested(nested, cap, module.getValue()));
                }));
            }
            return origins;
        });
    }

    private List<Origin> nested(ZipInputStream input, long cap, List<Origin> declared) throws IOException {
        var wanted = new HashMap<String, Origin>(); var seen = new HashSet<String>(); var authenticated = new ArrayList<Origin>();
        for (var origin : declared) { wanted.put(origin.binaryName().replace('.', '/') + ".class", origin); }
        long entries = 0, inflated = 0;
        while (true) {
            ZipEntry item = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, input::getNextEntry);
            if (item == null) { break; }
            entries = Math.incrementExact(entries); require(entries <= cap, "nested entry count exceeds outer input bound");
            zipEntries = Math.incrementExact(zipEntries);
            String name = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, item::getName); entry(name);
            require(seen.add(name), "nested entry repeated");
            var expected = wanted.get(name);
            long remaining = Math.subtractExact(cap, inflated);
            long memberCap = name.endsWith(".class") ? Math.min(remaining, MAX_MEMBER_BYTES) : remaining;
            Read actual = digest(input, memberCap, expected != null);
            inflated = Math.addExact(inflated, actual.bytes());
            if (expected != null) {
                Blob bytes = actual.blob();
                require(!item.isDirectory() && bytes.equals(expected.classBytes()), "nested class digest or length differs");
                authenticated.add(new Origin(expected.owner(), expected.role(), expected.binaryName(), expected.container(), expected.entry(), bytes, expected.applicationSha256()));
            }
            request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> { input.closeEntry(); return null; });
        }
        require(authenticated.size() == wanted.size(), "declared nested class missing"); return authenticated;
    }

    private void uniqueEntries(ZipFile zip, long countCap) throws IOException {
        var entries = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, zip::entries); var names = new HashSet<String>();
        long count = 0;
        while (request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, entries::hasMoreElements)) {
            ZipEntry item = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, entries::nextElement);
            count = Math.incrementExact(count); require(count <= countCap, "archive entry count exceeds input bound");
            zipEntries = Math.incrementExact(zipEntries);
            String name = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, item::getName); entry(name);
            require(names.add(name), "archive entry repeated");
        }
    }

    private Read digest(InputStream input, long cap, boolean hashing) throws IOException {
        require(cap >= 0 && cap < Long.MAX_VALUE, "stream prior byte cap is invalid");
        MessageDigest digest = hashing ? sha() : null;
        byte[] buffer = new byte[BUFFER_BYTES]; copy(BUFFER_BYTES, 1); long bytes = 0;
        while (true) {
            int limit = Math.toIntExact(Math.min(buffer.length, Math.addExact(Math.subtractExact(cap, bytes), 1)));
            int read = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> input.read(buffer, 0, limit));
            if (read < 0) { break; }
            require(read > 0, "stream read made no progress");
            bytes = Math.addExact(bytes, read); consumed(read); require(bytes <= cap, "stream exceeds prior byte cap");
            if (digest != null) { request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> { digest.update(buffer, 0, read); return null; }); hashed(read); }
        }
        if (digest == null) { return new Read(null, bytes); }
        byte[] sum = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, digest::digest);
        String hex = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> HexFormat.of().formatHex(sum));
        return new Read(hex, bytes);
    }
    private Blob hash(byte[] value) throws IOException {
        MessageDigest digest = sha();
        byte[] bytes = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> digest.digest(value)); hashed(value.length);
        String hex = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> HexFormat.of().formatHex(bytes));
        return new Blob(hex, value.length);
    }
    private MessageDigest sha() throws IOException {
        return request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> {
            try { return MessageDigest.getInstance("SHA-256"); }
            catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        });
    }
    private Path canonical(Path path) throws IOException {
        require(path != null && path.isAbsolute() && path.equals(path.normalize()), "path is not absolute and normalized");
        require(!request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.isSymbolicLink(path)), "linked file refused");
        Path real = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, path::toRealPath);
        require(path.equals(real), "linked or noncanonical path refused"); return path;
    }
    private Path directory(Path path) throws IOException {
        Path real = canonical(path);
        require(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.isDirectory(real, LinkOption.NOFOLLOW_LINKS)), "directory missing"); return real;
    }
    private void requireRegular(Path path, long cap) throws IOException {
        require(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)), "regular file missing");
        long size = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.size(path));
        require(size > 0 && size <= cap, "file exceeds prior byte cap");
    }
    private <T> T request(BenchmarkReturnWorkBudget.Activity activity, BenchmarkReturnWorkBudget.CheckedAction<T, IOException> action) throws IOException {
        requests = Math.incrementExact(requests); return BenchmarkReturnWorkBudget.external(scope, activity, action);
    }
    private <T> T closing(AutoCloseable resource, BenchmarkReturnWorkBudget.CheckedAction<T, IOException> body) throws IOException {
        Throwable primary = null;
        try { return body.run(); }
        catch (IOException | RuntimeException | Error failure) { primary = failure; BenchmarkReturnWorkBudget.failure(scope, failure); throw failure; }
        finally {
            try { request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> {
                try { resource.close(); }
                catch (IOException failure) { throw failure; }
                catch (Exception impossible) { throw new AssertionError(impossible); }
                return null;
            }); }
            catch (IOException | RuntimeException | Error failure) {
                if (primary == null) { throw failure; }
                if (failure != primary) { primary.addSuppressed(failure); }
            }
        }
    }
    private void consumed(long count) {
        consumedBytes = Math.addExact(consumedBytes, count);
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, BenchmarkReturnWorkBudget.Work.bytes(count));
    }
    private void hashed(long count) {
        hashBytes = Math.addExact(hashBytes, count);
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, new BenchmarkReturnWorkBudget.Work(0, 0, 0, 0, count, 1, count, 0));
    }
    private void copy(long bytes, long elements) {
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY, new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, bytes, 1, elements, 0));
    }
    private final class Limited extends InputStream {
        private final InputStream input; private final long cap; private long count;
        Limited(InputStream input, long cap) { this.input = input; this.cap = cap; }
        @Override public int read() throws IOException {
            int value = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, input::read);
            if (value >= 0) { count = Math.incrementExact(count); consumed(1); require(count <= cap, "module stream exceeds outer prior byte cap"); }
            return value;
        }
        @Override public int read(byte[] buffer, int off, int len) throws IOException {
            if (len == 0) { return 0; }
            int wanted = Math.toIntExact(Math.min(len, Math.addExact(Math.subtractExact(cap, count), 1)));
            int read = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> input.read(buffer, off, wanted));
            if (read >= 0) { require(read > 0, "module stream made no progress"); count = Math.addExact(count, read); consumed(read); require(count <= cap, "module stream exceeds outer prior byte cap"); }
            return read;
        }
        @Override public void close() throws IOException { request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> { input.close(); return null; }); }
    }

    static byte[] helperBytes(HelperManifest helper) {
        require(helper != null && helper.schemaVersion() == SCHEMA_VERSION, "helper schema differs"); var fields = new ArrayList<String>();
        fields.add("HELPER_BINDINGS_V1"); fields.add(Integer.toString(helper.schemaVersion())); fields.add(helper.runId()); fields.add(helper.inputSha256());
        fields.add(helper.buildId()); fields.add(helper.rootDefinitionId()); fields.add(helper.rootFormulaSha256()); fields.add(helper.producerFormulaSha256()); blobFields(fields, helper.failsafeXml());
        fields.add(Integer.toString(helper.failsafeClasspath().size())); fields.addAll(helper.failsafeClasspath());
        fields.add(Integer.toString(helper.sources().size()));
        for (var source : helper.sources()) { fields.add(source.role().name()); fields.add(source.path()); blobFields(fields, source.blob()); }
        fields.add(Integer.toString(helper.classes().size()));
        for (var item : helper.classes()) { fields.add(item.role().name()); fields.add(item.binaryName()); blobFields(fields, item.blob()); }
        fields.add(Integer.toString(helper.origins().size()));
        for (var origin : helper.origins()) {
            fields.add(origin.owner().name()); fields.add(origin.role().name()); fields.add(origin.binaryName()); fields.add(origin.container()); fields.add(origin.entry());
            blobFields(fields, origin.classBytes()); fields.add(origin.applicationSha256() == null ? "" : origin.applicationSha256());
        }
        return encoded(fields);
    }
    static byte[] jdkBytes(JdkManifest jdk) {
        require(jdk != null && jdk.schemaVersion() == SCHEMA_VERSION, "JDK schema differs"); var fields = new ArrayList<String>();
        fields.add("JDK_BINDINGS_V1"); fields.add(Integer.toString(jdk.schemaVersion())); fields.add(jdk.runId()); fields.add(jdk.inputSha256()); fields.add(jdk.javaHome()); fields.add(jdk.javaVersion());
        fields.add(Integer.toString(jdk.files().size()));
        for (var file : jdk.files()) { fields.add(file.role().name()); fields.add(file.path()); blobFields(fields, file.blob()); }
        return encoded(fields);
    }
    private static void blobFields(List<String> fields, Blob blob) { fields.add(blob.sha256()); fields.add(Long.toString(blob.bytes())); }
    private static byte[] encoded(List<String> fields) {
        long size = 0;
        for (String value : fields) {
            require(value.length() <= 4096 && value.chars().allMatch(c -> c >= 32 && c <= 126), "manifest field exceeds ASCII material cap");
            size = Math.addExact(size, Math.addExact(value.length(), Math.addExact(Integer.toString(value.length()).length(), 2)));
            require(size <= MAX_MANIFEST_BYTES, "manifest encoding exceeds prior byte cap");
        }
        var scope = BenchmarkReturnWorkBudget.current();
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY,
                new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, size, 1, Math.addExact(size, fields.size()), 0));
        byte[] encoded = new byte[Math.toIntExact(size)]; int at = 0;
        for (String value : fields) {
            String length = Integer.toString(value.length());
            for (int i = 0; i < length.length(); i++) { encoded[at++] = (byte) length.charAt(i); }
            encoded[at++] = ':';
            for (int i = 0; i < value.length(); i++) { encoded[at++] = (byte) value.charAt(i); }
            encoded[at++] = '\n';
        }
        require(at == encoded.length, "canonical manifest length differs"); return encoded;
    }
    private static void entry(String value) {
        require(value != null && !value.isEmpty() && value.length() <= 1024 && value.indexOf('\\') < 0 && value.indexOf('\0') < 0,
                "archive entry exceeds path cap");
        String name = value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        Path path = Path.of(name);
        require(!name.isEmpty() && !path.isAbsolute() && path.equals(path.normalize()) && !path.startsWith(".."), "foreign archive entry path");
    }
    private static void require(boolean value, String reason) { if (!value) { throw new AssertionError("return work binding IO " + reason); } }
}
