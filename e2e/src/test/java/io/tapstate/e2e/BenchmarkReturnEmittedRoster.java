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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;

/** Observes compiler files belonging to named source units, independently of declared member lists. */
final class BenchmarkReturnEmittedRoster {
    static final int DIRECTORY_VISITS = 40000, SELECTED_MEMBERS = 512, MEMBER_BYTES = 1048576;
    private static final Map<String, String> OUTPUTS = Map.of("e2e", "e2e/target/test-classes",
            "adapter-pdk", "adapters/adapter-pdk/target/classes", "core-common", "core/core-common/target/classes");
    @FunctionalInterface interface Streams { InputStream open(Path path) throws IOException; }
    @FunctionalInterface interface Walks { Stream<Path> open(Path path) throws IOException; }
    record Member(SourceRole role, String binaryName, String sourceFile, Blob blob, String compilerContainer, String entry) { }
    private record Identity(String binaryName, String sourceFile, int major) { }
    static final class Observation {
        private final List<Source> sources;
        private final List<Member> members;
        private final Map<String, Object> evidence;
        private Observation(List<Source> sources, List<Member> members, Map<String, Object> evidence) {
            this.sources = List.copyOf(sources); this.members = List.copyOf(members);
            this.evidence = Collections.unmodifiableMap(new LinkedHashMap<>(evidence));
        }
        List<Source> sources() { return sources; }
        List<Member> members() { return members; }
        List<CompiledClass> classes() { return members.stream().map(m -> new CompiledClass(m.role(), m.binaryName(), m.blob())).toList(); }
        Map<String, Object> evidence() { return evidence; }
    }
    static final class Refusal extends AssertionError {
        private final Map<String, Object> evidence;
        Refusal(Throwable original, Map<String, Object> evidence) {
            super("emitted source roster refused", original); this.evidence = Collections.unmodifiableMap(new LinkedHashMap<>(evidence));
        }
        Map<String, Object> retainedEvidence() { return evidence; }
    }
    private final BenchmarkReturnWorkBudget.Scope scope;
    private final Streams streams;
    private final Walks walks;
    private final ArrayList<Source> sources = new ArrayList<>();
    private final ArrayList<Member> members = new ArrayList<>();
    private long visits, requests, consumed;
    private BenchmarkReturnEmittedRoster(Streams streams, Walks walks) { this.streams = streams; this.walks = walks; scope = BenchmarkReturnWorkBudget.current(); }
    private BenchmarkReturnEmittedRoster() { this(path -> Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS), Files::walk); }

    static Observation observe(Path sourceRoot, HelperManifest helper) throws IOException { return new BenchmarkReturnEmittedRoster().run(sourceRoot, helper); }
    static Observation observe(Path sourceRoot, HelperManifest helper, Streams streams, Walks walks) throws IOException {
        return new BenchmarkReturnEmittedRoster(streams, walks).run(sourceRoot, helper);
    }
    private Observation run(Path root, HelperManifest helper) throws IOException {
        var ticket = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, BenchmarkReturnWorkBudget.Work.call());
        try {
            require(streams != null && walks != null && helper != null && helper.schemaVersion() == 1, "input or schema missing");
            canonical(root); require(request(() -> Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)), "source root is not a directory");
            var declaredSources = javaSources(helper.sources());
            var bySource = new HashMap<String, SourceRole>();
            for (var role : javaRoles()) {
                var declared = declaredSources.get(role); require(declared.path().equals(role.sourcePath()), "registered source path differs");
                var source = new Source(role, role.sourcePath(), read(root.resolve(role.sourcePath()), declared.blob()).blob);
                sources.add(source);
                require(bySource.put(role.module() + ":" + sourceFile(role), role) == null, "source file owner is ambiguous");
            }
            var binaries = new HashSet<String>();
            for (String module : List.of("e2e", "adapter-pdk", "core-common")) {
                Path output = root.resolve(OUTPUTS.get(module)); canonical(output);
                require(request(() -> Files.isDirectory(output, LinkOption.NOFOLLOW_LINKS)), "owned compiler output is missing");
                Stream<Path> paths = request(() -> walks.open(output));
                closing(paths, () -> {
                    var iterator = request(paths::iterator);
                    while (request(iterator::hasNext)) {
                        Path file = request(iterator::next); visits = Math.incrementExact(visits);
                        require(visits <= DIRECTORY_VISITS, "directory visits exceed prior cap");
                        require(file != null && file.startsWith(output), "compiler traversal escaped owned output"); canonical(file);
                        if (request(() -> Files.isDirectory(file, LinkOption.NOFOLLOW_LINKS))) continue;
                        require(request(() -> Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)), "compiler traversal contains a nonregular entry");
                        if (!file.toString().endsWith(".class")) continue;
                        var raw = read(file, null); var decode = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.DECODE, BenchmarkReturnWorkBudget.Work.call());
                        try {
                            Identity identity = identity(raw.bytes);
                            String entry = output.relativize(file).toString().replace(java.io.File.separatorChar, '/');
                            require(entry.equals(identity.binaryName().replace('.', '/') + ".class"), "compiler class path differs from this_class");
                            SourceRole fromSource = bySource.get(module + ":" + identity.sourceFile()); SourceRole fromBinary = null;
                            for (var role : javaRoles()) if (role.module().equals(module)
                                    && (identity.binaryName().equals(role.binaryName()) || identity.binaryName().startsWith(role.binaryName() + "$"))) {
                                require(fromBinary == null, "binary owner is ambiguous"); fromBinary = role;
                            }
                            if (fromSource != null || fromBinary != null) {
                                require(fromSource != null && fromSource == fromBinary, "SourceFile and binary source owner differ");
                                require(members.size() < SELECTED_MEMBERS && binaries.add(identity.binaryName()), "selected member roster exceeds cap or repeats");
                                members.add(new Member(fromSource, identity.binaryName(), identity.sourceFile(), raw.blob, output.toString(), entry));
                            }
                            BenchmarkReturnWorkBudget.done(decode, new BenchmarkReturnWorkBudget.Work(0, 0, 0, 1, raw.bytes.length, 1, raw.bytes.length, 0));
                        } catch (RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failed(decode, failure); throw failure; }
                    }
                    return null;
                });
            }
            require(requests <= serviceRequestUpper(), "service requests exceed fixed source bound");
            var observation = new Observation(sources, members, evidence("COMPILER_SOURCE_ROSTER_OBSERVED"));
            verify(helper, observation); BenchmarkReturnWorkBudget.done(ticket); return observation;
        } catch (IOException | RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failure(scope, failure); BenchmarkReturnWorkBudget.failed(ticket, failure);
            throw new Refusal(failure, evidence("UNKNOWN"));
        }
    }
    static void verify(HelperManifest helper, Observation observed) {
        require(helper != null && helper.schemaVersion() == 1 && observed != null, "registered or observed source roster missing or schema differs");
        require(javaSources(helper.sources()).equals(javaSources(observed.sources())), "actual Java source role or digest differs");
        var declared = new HashMap<String, CompiledClass>();
        for (var item : helper.classes()) require(declared.put(item.binaryName(), item) == null, "declared compiled member repeats");
        require(declared.size() <= SELECTED_MEMBERS, "declared member cap differs");
        var actual = new HashMap<String, CompiledClass>();
        for (var member : observed.members()) require(actual.put(member.binaryName(), new CompiledClass(member.role(), member.binaryName(), member.blob())) == null, "observed member repeats");
        require(actual.equals(declared), "complete compiler-emitted role binary or digest set differs");
        for (var role : javaRoles()) require(actual.containsKey(role.binaryName()), "named source outer member missing");
    }

    private record Raw(byte[] bytes, Blob blob) { }
    private Raw read(Path file, Blob expected) throws IOException {
        canonical(file); require(request(() -> Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)), "member is not a regular file");
        long size = request(() -> Files.size(file)); require(size > 0 && size <= MEMBER_BYTES, "source or class exceeds prior member cap");
        if (expected != null) require(size == expected.bytes(), "source byte size differs");
        InputStream input = request(() -> streams.open(file));
        return closing(input, () -> {
            byte[] bytes = new byte[Math.toIntExact(size)];
            BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.bytes(size));
            MessageDigest hash = hashService(() -> {
                try { return MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
            });
            int at = 0;
            while (at < bytes.length) {
                int offset = at, wanted = Math.min(8192, bytes.length - at); int read = request(() -> input.read(bytes, offset, wanted));
                require(read > 0 && read <= wanted, "member read is truncated or makes no progress"); at = Math.addExact(at, read); consume(read);
                hashService(() -> { hash.update(bytes, offset, read); return null; });
                BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, BenchmarkReturnWorkBudget.Work.bytes(read));
            }
            int extra = request(input::read); if (extra >= 0) consume(1); require(extra < 0, "member changed beyond its checked size");
            byte[] sum = hashService(hash::digest); String sha = hashService(() -> HexFormat.of().formatHex(sum)); Blob blob = new Blob(sha, size);
            require(expected == null || expected.equals(blob), "actual source digest differs");
            require(request(() -> Files.size(file)) == size, "member changed while read"); canonical(file); return new Raw(bytes, blob);
        });
    }
    private Path canonical(Path path) throws IOException {
        require(path != null && path.toString().length() <= 4096 && path.isAbsolute() && path.equals(path.normalize()), "member path is not canonical or bounded");
        require(!request(() -> Files.isSymbolicLink(path)) && path.equals(request(path::toRealPath)), "linked compiler or source entry refused"); return path;
    }
    private <T> T request(BenchmarkReturnWorkBudget.CheckedAction<T, IOException> action) throws IOException {
        requests = Math.incrementExact(requests); return BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, action);
    }
    private <T> T hashService(BenchmarkReturnWorkBudget.CheckedAction<T, IOException> action) throws IOException {
        requests = Math.incrementExact(requests); return BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, action);
    }
    private <T> T closing(AutoCloseable resource, BenchmarkReturnWorkBudget.CheckedAction<T, IOException> action) throws IOException {
        Throwable primary = null;
        try { return action.run(); }
        catch (IOException | RuntimeException | Error failure) { primary = failure; BenchmarkReturnWorkBudget.failure(scope, failure); throw failure; }
        finally {
            try { request(() -> { try { resource.close(); } catch (IOException | RuntimeException | Error failure) { throw failure; }
                catch (Exception failure) { throw new AssertionError(failure); } return null; }); }
            catch (IOException | RuntimeException | Error cleanup) { if (primary == null) throw cleanup; if (primary != cleanup) primary.addSuppressed(cleanup); }
        }
    }
    private void consume(long bytes) { consumed = Math.addExact(consumed, bytes); BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, BenchmarkReturnWorkBudget.Work.bytes(bytes)); }

    private static Set<SourceRole> javaRoles() { var roles = java.util.EnumSet.allOf(SourceRole.class); roles.remove(SourceRole.JNI_SOURCE); return roles; }
    private static Map<SourceRole, Source> javaSources(List<Source> list) {
        var result = new EnumMap<SourceRole, Source>(SourceRole.class);
        for (var source : list) if (source.role() != SourceRole.JNI_SOURCE) {
            require(source.path().equals(source.role().sourcePath()) && source.blob().bytes() <= MEMBER_BYTES
                    && result.put(source.role(), source) == null, "source role path digest or cap differs");
        }
        require(result.keySet().equals(javaRoles()), "complete named Java source roster missing"); return result;
    }
    private static String sourceFile(SourceRole role) { return Path.of(role.sourcePath()).getFileName().toString(); }

    private static Identity identity(byte[] raw) {
        var c = new Cursor(raw); require(c.u4() == 0xcafebabeL, "class magic differs"); c.u2(); int major = c.u2(); require(major == 65, "class major version differs");
        int count = c.u2(); require(count > 0 && count <= raw.length / 3 + 1, "constant pool exceeds prior byte shape");
        int[] tags = new int[count], offsets = new int[count], lengths = new int[count], refs = new int[count];
        for (int i = 1; i < count; i++) {
            int tag = c.u1(); tags[i] = tag;
            switch (tag) {
                case 1 -> { int length = c.u2(); offsets[i] = c.at; lengths[i] = length; c.skip(length); }
                case 7, 8, 16, 19, 20 -> refs[i] = c.u2();
                case 3, 4, 9, 10, 11, 12, 17, 18 -> c.skip(4);
                case 5, 6 -> { c.skip(8); require(++i < count, "wide constant slot missing"); }
                case 15 -> c.skip(3);
                default -> throw new AssertionError("class constant tag differs");
            }
        }
        c.u2(); int owner = c.u2(); c.u2(); require(owner > 0 && owner < count && tags[owner] == 7, "this_class is not a class constant");
        String binary = utf(raw, tags, offsets, lengths, refs[owner], 256).replace('/', '.');
        c.skip(Math.multiplyExact(2, c.u2()));
        for (int group = 0; group < 2; group++) {
            int members = c.u2(); for (int i = 0; i < members; i++) { c.skip(6); attributes(c, tags, offsets, lengths, null); }
        }
        String[] source = new String[1]; attributes(c, tags, offsets, lengths, source); require(c.at == raw.length, "class has trailing bytes");
        return new Identity(binary, source[0], major);
    }
    private static void attributes(Cursor c, int[] tags, int[] offsets, int[] lengths, String[] source) {
        int count = c.u2();
        for (int i = 0; i < count; i++) {
            int name = c.u2(); require(name > 0 && name < tags.length && tags[name] == 1, "attribute name is not UTF8");
            long length = c.u4(); require(length <= c.raw.length - c.at, "attribute length exceeds remaining member");
            if (source != null && asciiEquals(c.raw, offsets[name], lengths[name], "SourceFile")) {
                require(source[0] == null && length == 2, "SourceFile is repeated or malformed"); source[0] = utf(c.raw, tags, offsets, lengths, c.u2(), 256);
            } else c.skip(Math.toIntExact(length));
        }
    }
    private static String utf(byte[] raw, int[] tags, int[] offsets, int[] lengths, int index, int cap) {
        require(index > 0 && index < tags.length && tags[index] == 1 && lengths[index] > 0 && lengths[index] <= cap, "class identity UTF8 differs or exceeds cap");
        for (int i = 0; i < lengths[index]; i++) require(raw[offsets[index] + i] >= 32 && raw[offsets[index] + i] <= 126, "class identity is not ASCII");
        return new String(raw, offsets[index], lengths[index], StandardCharsets.US_ASCII);
    }
    private static boolean asciiEquals(byte[] raw, int offset, int length, String expected) {
        if (length != expected.length()) return false; for (int i = 0; i < length; i++) if (raw[offset + i] != expected.charAt(i)) return false; return true;
    }
    private static final class Cursor {
        final byte[] raw; int at;
        Cursor(byte[] raw) { this.raw = raw; }
        int u1() { require(at < raw.length, "class field is truncated"); return raw[at++] & 255; }
        int u2() { return (u1() << 8) | u1(); }
        long u4() { return ((long) u2() << 16) | u2(); }
        void skip(int count) { require(count >= 0 && count <= raw.length - at, "class body is truncated"); at = Math.addExact(at, count); }
    }

    private Map<String, Object> evidence(String state) {
        var map = new LinkedHashMap<String, Object>(); map.put("schemaVersion", 1); map.put("state", state); map.put("directoryVisits", visits);
        map.put("serviceRequests", requests); map.put("serviceRequestUpper", serviceRequestUpper()); map.put("consumedBytes", consumed);
        map.put("sources", sources.stream().map(s -> Map.of("role", s.role().name(), "path", s.path(),
                "sha256", s.blob().sha256(), "bytes", s.blob().bytes())).toList());
        map.put("selectedMembers", members.stream().map(m -> Map.of("role", m.role().name(), "binaryName", m.binaryName(), "sourceFile", m.sourceFile(),
                "sha256", m.blob().sha256(), "bytes", m.blob().bytes(), "compilerContainer", m.compilerContainer(), "entry", m.entry())).toList());
        map.put("scope", "THREE_OWNED_COMPILER_OUTPUTS_SOURCEFILE_BINARY_AND_BYTE_FACTS_ONLY");
        map.put("boundBasis", "FIXED_DIRECTORY_VISIT_SOURCE_ROLE_MEMBER_BYTE_AND_SELECTED_MEMBER_CAPS");
        map.put("opaqueServiceInternals", "IO_PROVIDER_AND_DIGEST_INTERNAL_CPU_OR_ELAPSED_EXCLUDED");
        for (String flag : List.of("loadedRuntimeOriginsQualified", "emittedMemberCompletenessQualified", "runtimeInstructionBytesQualified", "sourceCoverageQualified",
                "structuralWorkBudgetQualified", "wholeMethodCostQualified", "samplingCostQualified", "causalOverheadQualified",
                "performanceAcceptanceEligible", "formalPerformance")) map.put(flag, false);
        return map;
    }
    static BenchmarkReturnWorkBudget.Work localUpper() {
        // Byte and u2-shaped loops include cursor checks, constant arrays, source-owner scans and evidence copies.
        long paths = Math.multiplyExact(4096L, DIRECTORY_VISITS), bytes = MEMBER_BYTES, roles = javaRoles().size();
        long classWork = Math.multiplyExact(DIRECTORY_VISITS, Math.addExact(Math.multiplyExact(64, bytes), 65536L * 64));
        return new BenchmarkReturnWorkBudget.Work(Math.addExact(128, Math.multiplyExact(32, DIRECTORY_VISITS + roles)),
                Math.addExact(classWork, Math.multiplyExact(32, paths)), Math.addExact(SELECTED_MEMBERS, roles), DIRECTORY_VISITS,
                Math.addExact(Math.multiplyExact(32, paths), Math.multiplyExact(DIRECTORY_VISITS, Math.multiplyExact(32, bytes))),
                Math.multiplyExact(32, DIRECTORY_VISITS + roles), Math.addExact(classWork, paths), Math.addExact(classWork, paths));
    }
    static long serviceRequestUpper() {
        long files = Math.addExact(DIRECTORY_VISITS, javaRoles().size());
        return Math.addExact(128, Math.multiplyExact(files, Math.addExact(32, Math.multiplyExact(2, MEMBER_BYTES + 1L))));
    }
    private static void require(boolean condition, String reason) { if (!condition) throw new AssertionError("emitted roster " + reason); }
}
