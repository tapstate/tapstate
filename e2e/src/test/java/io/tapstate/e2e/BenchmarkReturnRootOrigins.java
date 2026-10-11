package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.Runtime;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;

/** Observes exact named root classes; resource bytes are not transformed VM instruction bodies. */
final class BenchmarkReturnRootOrigins {
    private static final int BUFFER_BYTES = 8192;
    private static final int NAME_CHARACTERS = 256, PATH_CHARACTERS = 4096, ENTRY_CHARACTERS = 1024;
    // A file path and member can percent-expand each character to three ASCII characters.
    private static final int URI_CHARACTERS = 3 * (PATH_CHARACTERS + ENTRY_CHARACTERS) + 16;

    @FunctionalInterface interface Resolver { Class<?> resolve(String name) throws ClassNotFoundException; }
    @FunctionalInterface interface Streams { InputStream open(URLConnection connection) throws IOException; }

    record RootOrigins(List<ObservedOrigin> observedOrigins, Map<String, Object> evidence) {
        RootOrigins { observedOrigins = List.copyOf(observedOrigins); evidence = Map.copyOf(evidence); }
    }

    private final BenchmarkReturnWorkBudget.Scope scope;
    private final Resolver resolver;
    private final Streams streams;
    private long serviceRequests, consumedBytes, hashedBytes;

    private BenchmarkReturnRootOrigins(BenchmarkReturnWorkBudget.Scope scope, Resolver resolver, Streams streams) {
        require(resolver != null && streams != null, "resolution or stream source is missing");
        this.scope = scope; this.resolver = resolver; this.streams = streams;
    }

    static RootOrigins observeRoot(HelperManifest helper, List<String> actualClasspath, Runtime expectedRoot,
                                   BenchmarkReturnWorkBudget.Scope scope) throws IOException, ClassNotFoundException {
        return observeRoot(helper, actualClasspath, expectedRoot, scope,
                name -> Class.forName(name, false, BenchmarkReturnWorkBudgetRun.class.getClassLoader()),
                URLConnection::getInputStream);
    }

    /** Test seams resolve real package classes and streams; they cannot supply an origin or runtime. */
    static RootOrigins observeRoot(HelperManifest helper, List<String> actualClasspath, Runtime expectedRoot,
                                   BenchmarkReturnWorkBudget.Scope scope, Resolver resolver, Streams streams)
            throws IOException, ClassNotFoundException {
        return new BenchmarkReturnRootOrigins(scope, resolver, streams).observe(helper, actualClasspath, expectedRoot);
    }

    /** Inclusive local allowance per entered class, including a conservative repeat of setup and report work. */
    static BenchmarkReturnWorkBudget.Work localUpper() {
        long classes = MAX_CLASSES, origins = Math.multiplyExact(2L, classes);
        long roster = Math.addExact(Math.addExact(classes, origins), MAX_CLASSPATH);
        long rowCharacters = Math.addExact(Math.addExact(NAME_CHARACTERS + ENTRY_CHARACTERS + 256L + 128 + 64,
                2L * PATH_CHARACTERS), 2L * URI_CHARACTERS);
        long setupCharacters = Math.addExact(Math.multiplyExact(MAX_CLASSPATH, PATH_CHARACTERS),
                Math.multiplyExact(origins, NAME_CHARACTERS + ENTRY_CHARACTERS + PATH_CHARACTERS + 64L));
        long characters = Math.addExact(rowCharacters, setupCharacters);
        // Roster slot: <=16 accessor/map/set/iterator/record APIs, <=8 stores. Text visit:
        // <=32 guard/URI/path/string/checked-arithmetic APIs and <=8 primitive visits/stores.
        // Fixed 128 API slots cover immutable row/report maps, hex/records, loop and failure setup.
        // External Class/URL/file/hash APIs are separately ticketed; their interiors are opaque.
        return new BenchmarkReturnWorkBudget.Work(Math.addExact(32, Math.multiplyExact(16, roster)),
                Math.addExact(128, Math.addExact(Math.multiplyExact(32, characters), Math.multiplyExact(16, roster))),
                1, 0, Math.addExact(BUFFER_BYTES, Math.addExact(Math.multiplyExact(4, characters), Math.multiplyExact(128, roster))),
                8, Math.addExact(Math.multiplyExact(8, characters), Math.multiplyExact(16, roster)),
                Math.addExact(Math.multiplyExact(8, characters), Math.multiplyExact(8, roster)));
    }

    /** Provider boundaries, not provider instructions: runtime=12+4 path requests; each jar route<=36. */
    static long serviceRequestUpper(long classes, long priorClassBytes) {
        require(classes > 0 && classes <= MAX_CLASSES && priorClassBytes >= classes
                && priorClassBytes <= Math.multiplyExact(classes, MAX_MEMBER_BYTES), "prior service basis exceeds fixed class caps");
        // A positive read consumes at least one byte; reads<=B+1 and digest updates<=B.
        // The 36 includes EOF, resolve/metadata/URL/path/open/hash/hex and both owned closes.
        return Math.addExact(16, Math.addExact(Math.multiplyExact(36, classes), Math.multiplyExact(2, priorClassBytes)));
    }

    private RootOrigins observe(HelperManifest helper, List<String> actualClasspath, Runtime expectedRoot)
            throws IOException, ClassNotFoundException {
        // The first class context also owns finite roster setup, runtime checks and final evidence.
        var first = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                BenchmarkReturnWorkBudget.Work.call());
        try {
            require(supportedHelper(helper) && expectedRoot != null
                    && expectedRoot.owner() == Owner.ROOT && helper.runId().equals(expectedRoot.runId()), "root registration differs");
            require(actualClasspath != null && !actualClasspath.isEmpty() && actualClasspath.size() <= MAX_CLASSPATH
                    && actualClasspath.equals(helper.failsafeClasspath()), "actual classpath differs or exceeds its cap");
            var classpath = new HashSet<String>();
            for (String path : actualClasspath) { bounded(path, PATH_CHARACTERS); require(classpath.add(path), "classpath origin repeated"); }
            require(!helper.classes().isEmpty() && helper.classes().size() <= MAX_CLASSES
                    && helper.origins().size() <= 2 * MAX_CLASSES, "declared class roster is empty or exceeds its cap");
            var declared = new HashMap<String, Origin>();
            for (var origin : helper.origins()) if (origin.owner() == Owner.ROOT) {
                bounded(origin.binaryName(), NAME_CHARACTERS); bounded(origin.container(), PATH_CHARACTERS);
                bounded(origin.entry(), ENTRY_CHARACTERS);
                require(declared.put(origin.binaryName(), origin) == null, "root origin repeated");
            }
            var names = new HashSet<String>(); long priorClassBytes = 0;
            for (var compiled : helper.classes()) {
                bounded(compiled.binaryName(), NAME_CHARACTERS);
                require(compiled.role().owners().contains(Owner.ROOT) && packageName(compiled.binaryName())
                        && names.add(compiled.binaryName()), "class is repeated or outside the fixed root packages");
                require(compiled.blob().bytes() <= MAX_MEMBER_BYTES, "class exceeds its prior material cap");
                priorClassBytes = Math.addExact(priorClassBytes, compiled.blob().bytes());
                var origin = declared.get(compiled.binaryName());
                require(origin != null && origin.role() == compiled.role() && origin.classBytes().equals(compiled.blob())
                        && origin.applicationSha256() == null && classpath.contains(origin.container())
                        && origin.entry().equals(compiled.binaryName().replace('.', '/') + ".class"), "declared root member differs");
            }
            require(declared.keySet().equals(names), "root origin roster is incomplete or has extra names");
            Runtime before = runtime(expectedRoot.runId());
            require(before.equals(expectedRoot), "actual root runtime differs before resolution");
            var observed = new ArrayList<ObservedOrigin>(); var rows = new ArrayList<Map<String, Object>>();
            for (int index = 0; index < helper.classes().size(); index++) {
                var compiled = helper.classes().get(index);
                var ticket = index == 0 ? first : BenchmarkReturnWorkBudget.begin(scope,
                        BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, BenchmarkReturnWorkBudget.Work.call());
                try {
                    observeClass(compiled, declared.get(compiled.binaryName()), before, observed, rows);
                    if (index != 0) BenchmarkReturnWorkBudget.done(ticket, BenchmarkReturnWorkBudget.Work.records(1));
                } catch (IOException | ClassNotFoundException | RuntimeException | Error failure) {
                    BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure;
                }
            }
            Runtime after = runtime(expectedRoot.runId());
            require(before.equals(after) && after.equals(expectedRoot), "actual root runtime changed during resolution");
            var evidence = new LinkedHashMap<String, Object>();
            evidence.put("state", "ROOT_NAMED_CLASS_ORIGINS_RECORDED"); evidence.put("runtime", runtimeEvidence(before));
            evidence.put("classCount", observed.size()); evidence.put("serviceRequests", serviceRequests);
            long requestUpper = serviceRequestUpper(observed.size(), priorClassBytes);
            require(serviceRequests <= requestUpper, "actual provider requests exceed the source service bound");
            evidence.put("serviceRequestUpper", requestUpper); evidence.put("serviceRequestBasis", "PRIOR_CLASS_BYTES_AND_ENTERED_NAMES");
            evidence.put("serviceRequestScope", "TICKETED_PROVIDER_BOUNDARIES_NOT_PROVIDER_INSTRUCTIONS");
            evidence.put("consumedBytes", consumedBytes); evidence.put("hashedBytes", hashedBytes);
            evidence.put("classes", List.copyOf(rows)); evidence.put("resolution", "EXACT_REGISTERED_NAMES_INITIALIZE_FALSE");
            evidence.put("loaderIdentityScope", "ACTUAL_DEFINING_LOADER_IDENTITY_HASH_WITHIN_ROOT_JVM");
            evidence.put("byteScope", "OWN_CLASS_RESOURCE_AGREES_WITH_ACTUAL_PROTECTION_DOMAIN_AND_REGISTERED_BYTES");
            evidence.put("gaps", List.of("PRODUCER_LOADED_ORIGINS_NOT_OBSERVED_HERE", "EMITTED_MEMBER_COMPLETENESS_NOT_PROVEN_HERE",
                    "VM_GENERATED_OR_TRANSFORMED_INSTRUCTION_BODY_NOT_PROVEN"));
            evidence.put("sourceCoverageQualified", false); evidence.put("emittedMemberCompletenessQualified", false);
            evidence.put("runtimeInstructionBytesQualified", false); evidence.put("structuralWorkBudgetQualified", false);
            evidence.put("wholeMethodCostQualified", false); evidence.put("performanceAcceptanceEligible", false);
            evidence.put("causalOverheadQualified", false); evidence.put("formalPerformance", false);
            BenchmarkReturnWorkBudget.done(first, BenchmarkReturnWorkBudget.Work.records(1));
            return new RootOrigins(observed, evidence);
        } catch (IOException | ClassNotFoundException | RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failed(first, failure); throw failure;
        }
    }

    private void observeClass(CompiledClass compiled, Origin declared, Runtime actual,
                              List<ObservedOrigin> observed, List<Map<String, Object>> rows)
            throws IOException, ClassNotFoundException {
        Class<?> type = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> resolver.resolve(compiled.binaryName()));
        require(type != null && compiled.binaryName().equals(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, type::getName)),
                "resolved class name differs");
        ClassLoader loader = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, type::getClassLoader);
        require(loader != null, "root measurement class has no defining loader");
        String loaderClass = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> loader.getClass().getName());
        bounded(loaderClass, NAME_CHARACTERS);
        int loaderIdentity = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> System.identityHashCode(loader));
        var domain = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, type::getProtectionDomain);
        require(domain != null, "class protection domain is absent");
        var source = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, domain::getCodeSource);
        require(source != null, "class code source is absent");
        URL sourceUrl = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, source::getLocation);
        Path container = fileUrl(sourceUrl);
        require(container.toString().equals(declared.container()), "actual code source differs from registered root origin");
        String entry = compiled.binaryName().replace('.', '/') + ".class";
        URL resource = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> type.getResource("/" + entry));
        require(resource != null, "own class resource is absent");
        boundedUrl(resource);
        boolean directory = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ,
                () -> Files.isDirectory(container, LinkOption.NOFOLLOW_LINKS));
        String protocol = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, resource::getProtocol);
        require(directory ? protocol.equals("file") : protocol.equals("jar"), "resource protocol does not match the code source");
        if (directory) {
            Path member = fileUrl(resource);
            require(member.equals(container.resolve(entry)) && member.startsWith(container), "shadow class resource differs from code source");
            require(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.size(member)) == compiled.blob().bytes(),
                    "actual resource size differs from prior bytes");
        }
        URLConnection connection = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, resource::openConnection);
        request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> { connection.setUseCaches(false); return null; });
        Blob bytes;
        if (!directory) {
            require(connection instanceof JarURLConnection, "class resource is not a local jar connection");
            var jar = (JarURLConnection) connection;
            URL archive = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, jar::getJarFileURL);
            require(fileUrl(archive).equals(container), "shadow jar resource differs from code source");
            require(entry.equals(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, jar::getEntryName)), "jar class entry differs");
            var ownedJar = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, jar::getJarFile);
            Throwable primary = null;
            try {
                var member = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, jar::getJarEntry);
                require(member != null && !member.isDirectory() && member.getSize() == compiled.blob().bytes(),
                        "actual jar resource size differs from prior bytes");
                bytes = openHash(connection, compiled.blob().bytes());
            } catch (IOException | RuntimeException | Error failure) { primary = failure; throw failure; }
            finally {
                try { request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> { ownedJar.close(); return null; }); }
                catch (IOException | RuntimeException | Error failure) {
                    if (primary == null) throw failure;
                    if (failure != primary) primary.addSuppressed(failure);
                }
            }
        } else {
            bytes = openHash(connection, compiled.blob().bytes());
        }
        require(bytes.equals(compiled.blob()), "actual class resource digest or length differs");
        // Container, entry, bytes and runtime below are all obtained from the resolved class and reads.
        var origin = new Origin(Owner.ROOT, compiled.role(), request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, type::getName),
                container.toString(), entry, bytes, null);
        observed.add(new ObservedOrigin(origin, actual));
        rows.add(Map.of("binaryName", origin.binaryName(), "container", origin.container(), "entry", origin.entry(),
                "sha256", bytes.sha256(), "bytes", bytes.bytes(), "loaderClass", loaderClass, "loaderIdentityHashCode", loaderIdentity));
    }

    private Blob openHash(URLConnection connection, long priorBytes) throws IOException {
        InputStream input = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> streams.open(connection));
        require(input != null, "class stream is absent");
        return hashClosing(input, priorBytes);
    }

    private Blob hashClosing(InputStream input, long priorBytes) throws IOException {
        Throwable primary = null;
        try {
            MessageDigest digest;
            try { digest = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> MessageDigest.getInstance("SHA-256")); }
            catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
            byte[] buffer = new byte[(int) Math.min(BUFFER_BYTES, priorBytes)];
            BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.bytes(buffer.length));
            long bytes = 0;
            while (true) {
                int limit = Math.toIntExact(Math.min(buffer.length, Math.addExact(Math.subtractExact(priorBytes, bytes), 1)));
                int read = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> input.read(buffer, 0, limit));
                if (read < 0) break;
                require(read > 0 && read <= limit, "class stream is truncated or makes no progress");
                bytes = Math.addExact(bytes, read); consumedBytes = Math.addExact(consumedBytes, read);
                BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ,
                        BenchmarkReturnWorkBudget.Work.bytes(read));
                require(bytes <= priorBytes, "class stream exceeds its prior material cap");
                request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> { digest.update(buffer, 0, read); return null; });
                hashedBytes = Math.addExact(hashedBytes, read);
                BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        BenchmarkReturnWorkBudget.Work.bytes(read));
            }
            require(bytes == priorBytes, "class stream length differs from prior bytes");
            byte[] sum = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, digest::digest);
            String hex = request(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, () -> HexFormat.of().formatHex(sum));
            return new Blob(hex, bytes);
        } catch (IOException | RuntimeException | Error failure) { primary = failure; throw failure; }
        finally {
            try { request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> { input.close(); return null; }); }
            catch (IOException | RuntimeException | Error failure) {
                if (primary == null) throw failure;
                if (failure != primary) primary.addSuppressed(failure);
            }
        }
    }

    private Runtime runtime(String runId) throws IOException {
        var process = request(BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, ProcessHandle::current);
        long pid = request(BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, process::pid);
        var bean = request(BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, ManagementFactory::getRuntimeMXBean);
        long start = request(BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, bean::getStartTime);
        String home = request(BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> System.getProperty("java.home"));
        String version = request(BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> System.getProperty("java.version"));
        bounded(home, PATH_CHARACTERS); bounded(version, 128);
        Path canonical = canonical(Path.of(home));
        return new Runtime(Owner.ROOT, runId, pid, start, canonical.toString(), version);
    }

    private Path fileUrl(URL url) throws IOException {
        boundedUrl(url);
        require("file".equals(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, url::getProtocol)), "nonlocal class URL refused");
        URI uri;
        try { uri = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, url::toURI); }
        catch (URISyntaxException malformed) { throw new AssertionError("root origin URL is malformed", malformed); }
        require(uri.getRawAuthority() == null && uri.getRawQuery() == null && uri.getRawFragment() == null,
                "class URL has an authority, query or fragment");
        return canonical(Path.of(uri));
    }

    private void boundedUrl(URL url) throws IOException {
        require(url != null, "class URL is absent");
        bounded(request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, url::toExternalForm), URI_CHARACTERS);
    }

    private Path canonical(Path path) throws IOException {
        bounded(path.toString(), PATH_CHARACTERS);
        require(path.isAbsolute() && path.equals(path.normalize()), "class path is not absolute and normalized");
        require(!request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.isSymbolicLink(path)), "linked class origin refused");
        Path real = request(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, path::toRealPath);
        require(path.equals(real), "noncanonical class origin refused"); return real;
    }

    private <T, E extends Exception> T request(BenchmarkReturnWorkBudget.Activity activity,
            BenchmarkReturnWorkBudget.CheckedAction<T, E> action) throws E {
        serviceRequests = Math.incrementExact(serviceRequests);
        return BenchmarkReturnWorkBudget.external(scope, activity, action);
    }

    private static Map<String, Object> runtimeEvidence(Runtime runtime) {
        return Map.of("owner", runtime.owner().name(), "runId", runtime.runId(), "pid", runtime.pid(),
                "jvmStartTimeMillis", runtime.jvmStartTimeMillis(), "javaHome", runtime.javaHome(), "javaVersion", runtime.javaVersion());
    }
    private static boolean packageName(String name) {
        return name.startsWith("io.tapstate.e2e.") || name.startsWith("io.tapstate.adapters.pdk.") || name.startsWith("io.tapstate.core.common.") || name.startsWith("io.tapstate.testsupport.");
    }
    private static void bounded(String value, int cap) {
        require(value != null && !value.isBlank() && value.length() <= cap
                && value.chars().allMatch(c -> c >= 32 && c <= 126), "root origin text exceeds its prior bound");
    }
    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError("root class origins " + reason);
    }
}
