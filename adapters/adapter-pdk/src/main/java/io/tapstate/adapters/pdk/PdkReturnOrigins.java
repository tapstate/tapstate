package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/** Cold, exact-name resource origins; observations do not establish transformed instruction bytes. */
final class PdkReturnOrigins {
    static final int NATIVE_BYTES = 8192, HEADER_BYTES = 8192, ORIGIN_BYTES = 63 * 1024;
    static final int CLASS_BYTES = 1024 * 1024, CLASS_COUNT = 32, BUFFER_BYTES = 8192;
    static final int MAX_PACKET_BYTES = NATIVE_BYTES + HEADER_BYTES + CLASS_COUNT * ORIGIN_BYTES;
    static final int HEADER_NODES = 128, ORIGIN_NODES = 96, DEPTH = 4, NUMBER_TEXT = 20;
    static final int HOME_CHARS = 512, VERSION_CHARS = 128, NAME_CHARS = 256;
    static final int PATH_CHARS = 4096, ENTRY_CHARS = 1024, URI_CHARS = 18448, FAILURE_CHARS = 128;
    static final String RESOLUTION = "EXACT_NAMES_INITIALIZE_FALSE";
    static final String BYTE_SCOPE = "OWN_RESOURCE_CODE_SOURCE_AND_CONTAINER_NOT_VM_INSTRUCTION_BODY";
    static final List<String> FLAGS = List.of("clockQualified", "siTimeQualified", "utcAccuracyQualified",
            "samplingAgeQualified", "callBoundaryQualified", "loadingQualified", "classIntrinsicQualified",
            "completeOsFunctionBodyQualified", "samplingCostQualified", "costAcceptanceEligible",
            "performanceAcceptanceEligible", "formalPerformance", "sourceCoverageQualified",
            "emittedMemberCompletenessQualified", "runtimeInstructionBytesQualified", "structuralWorkBudgetQualified",
            "wholeMethodCostQualified", "causalOverheadQualified");
    static final List<String> HEADER_FIELDS = List.of("schemaVersion", "state", "pid", "jvmStartTimeMillis",
            "javaHome", "javaVersion", "beforeRuntime", "afterRuntime", "expectedClassCount", "completedClassCount",
            "serviceRequests", "classBytesConsumed", "classBytesHashed", "reason", "failureType",
            "suppressedFailureTypes", "resolution", "byteScope");
    static final List<String> ORIGIN_FIELDS = List.of("binaryName", "actualName", "state", "loaderClass",
            "loaderIdentityHashCode", "codeSource", "resource", "container", "outer", "module", "entry",
            "sha256", "bytes", "reason", "failureType");
    private static final String PDK = "io.tapstate.adapters.pdk.";
    private static final List<String> NAMES = List.of(PDK + "PdkBenchmarkClock", PDK + "PdkBenchmarkClock$Clock",
            PDK + "PdkBenchmarkClock$JniAccess", PDK + "PdkBenchmarkClock$NativeAccess",
            PDK + "PdkWriteReturnProbe", PDK + "PdkWriteReturnProbe$Writer", PDK + "PdkWriteReturnProbe$Ticket",
            PDK + "PdkWriteReturnProbe$CostCall", PDK + "PdkWriteReturnProbe$CostStages", PDK + "PdkWriteReturnProbe$Frame",
            PDK + "PdkWriteReturnProbe$ErrorEntry", PDK + "PdkWriteReturnProbe$BoundedBytes", PDK + "PdkWriteReturnProbeMBean",
            PDK + "PdkWriteReturnWorkBudget", PDK + "PdkWriteReturnWorkBudget$Activity", PDK + "PdkWriteReturnWorkBudget$Actor",
            PDK + "PdkWriteReturnWorkBudget$Span", PDK + "PdkWriteReturnWorkBudget$Config", PDK + "PdkMongoWriteScope",
            PDK + "PdkMongoWriteScope$Call", PDK + "PdkMongoWriteScope$ThreadOwners", PDK + "PdkMongoWriteScope$Transactions",
            PDK + "PdkMongoWriteScope$Observation", PDK + "PdkMongoWriteScope$Access", PDK + "PdkMongoWriteScope$UnsupportedShape",
            PDK + "PdkSinkWriter", "io.tapstate.core.common.JsonWriter", "io.tapstate.core.common.JsonReader",
            "io.tapstate.core.common.JsonReader$Parser", PDK + "PdkReturnOrigins", PDK + "PdkReturnOrigins$Resolver",
            PDK + "PdkReturnOrigins$Streams");

    @FunctionalInterface interface Resolver { Class<?> resolve(String name) throws ClassNotFoundException; }
    @FunctionalInterface interface Streams { InputStream open(URLConnection connection) throws IOException; }

    private final PdkWriteReturnWorkBudget budget;
    private final Resolver resolver;
    private final Streams streams;
    private long requests, consumed, hashed;

    private PdkReturnOrigins(PdkWriteReturnWorkBudget budget, Resolver resolver, Streams streams) {
        this.budget = budget; this.resolver = resolver; this.streams = streams;
    }

    static List<String> classNames() { return NAMES; }
    static long serviceRequestUpper() { return add(16, mul(CLASS_COUNT, add(128, mul(2, CLASS_BYTES + 1L)))); }

    static String[] packet(String nativeRaw, PdkWriteReturnWorkBudget budget) {
        return packet(nativeRaw, budget, name -> Class.forName(name, false, PdkReturnOrigins.class.getClassLoader()),
                URLConnection::getInputStream, null);
    }

    /** The observer receives the exact original refusal, after owned stream cleanup. */
    static String[] packet(String nativeRaw, PdkWriteReturnWorkBudget budget, Resolver resolver,
                           Streams streams, Consumer<Throwable> refusalObserver) {
        if (nativeRaw == null || nativeRaw.isEmpty() || nativeRaw.length() > NATIVE_BYTES
                || !nativeRaw.chars().allMatch(c -> c >= 32 && c <= 126)) {
            throw new AssertionError("native origin packet input exceeds the original bound");
        }
        if (budget == null || resolver == null || streams == null) {
            throw new AssertionError("cold origin packet requires an enabled owner and provider boundaries");
        }
        return new PdkReturnOrigins(budget, resolver, streams).observe(nativeRaw, refusalObserver);
    }

    /** Independent source recipe per entered class; setup and final packet work repeat conservatively. */
    static long[] perClassUpper() {
        long nodes = add(HEADER_NODES, mul(CLASS_COUNT, ORIGIN_NODES));
        long entries = add(HEADER_FIELDS.size(), FLAGS.size(), 8, mul(CLASS_COUNT, add(ORIGIN_FIELDS.size(), FLAGS.size())));
        long text = add(HEADER_BYTES, mul(CLASS_COUNT, ORIGIN_BYTES));
        long localText = add(2L * URI_CHARS, 2L * PATH_CHARS, 2L * ENTRY_CHARS,
                2L * NAME_CHARS, HOME_CHARS, VERSION_CHARS, chars(NAMES));
        // JsonWriter: five map APIs, seven entry APIs, three character APIs, two scalar APIs.
        long json = add(2, mul(5, CLASS_COUNT + 3L), mul(7, entries), mul(3, add(entries, nodes)),
                mul(3, text), mul(2, nodes), 4, mul(3, 8));
        // One-byte reads require at most B+1 reads and B digest updates. Provider interiors are opaque.
        long provider = add(128, mul(2, CLASS_BYTES + 1L));
        // The final tree ASCII walk uses length and two charAt calls per character. The local
        // allowance also covers the first row's pre-encoding and guard before its final encoding.
        long requestUpper = add(provider, mul(32, localText), json, mul(3, MAX_PACKET_BYTES), mul(16, nodes));
        return new long[]{add(1, mul(16, nodes), mul(8, text)), requestUpper, 1, 0,
                add(mul(4, add(CLASS_BYTES + 1L, MAX_PACKET_BYTES)), mul(128, nodes), mul(CLASS_COUNT, BUFFER_BYTES)),
                add(16, mul(8, CLASS_COUNT), mul(2, nodes)), mul(8, add(CLASS_BYTES + 1L, MAX_PACKET_BYTES, nodes)),
                mul(8, add(text, nodes))};
    }

    static long headerEncodingUpper() {
        return add(mul(2, 3L * (HOME_CHARS + VERSION_CHARS)), mul(128, 13),
                mul(2, add(chars(HEADER_FIELDS), chars(FLAGS), 2 * 42L)), mul(NUMBER_TEXT, 12),
                mul(5, FLAGS.size()), mul(4, HEADER_FIELDS.size() + FLAGS.size() + 8L), 32);
    }

    static long originEncodingUpper() {
        // Canonical ASCII URIs cannot contain the two characters escaped by JsonWriter.
        long escaped = add(2L * NAME_CHARS, 2L * PATH_CHARS, 2L * ENTRY_CHARS, 64, 2L * FAILURE_CHARS, 32);
        return add(2L * URI_CHARS, mul(2, escaped), mul(2, add(chars(ORIGIN_FIELDS), chars(FLAGS))),
                2L * NUMBER_TEXT, mul(5, FLAGS.size()), mul(4, ORIGIN_FIELDS.size() + FLAGS.size()), 4);
    }

    private String[] observe(String nativeRaw, Consumer<Throwable> refusalObserver) {
        var first = budget.enter(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS);
        var rows = new ArrayList<Map<String, Object>>(CLASS_COUNT);
        for (String name : NAMES) rows.add(emptyRow(name));
        Map<String, Object> before = new LinkedHashMap<>(), after = new LinkedHashMap<>();
        Throwable primary = null; int completed = 0;
        try {
            try {
                runtime(before);
                for (int i = 0; i < CLASS_COUNT; i++) {
                    var span = i == 0 ? first : budget.enter(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS);
                    try {
                        observeClass(NAMES.get(i), rows.get(i));
                        encode(rows.get(i), false);
                        completed++; budget.material(2, 1);
                        if (i != 0) span.success();
                    } catch (Exception | LinkageError failure) {
                        rows.get(i).put("state", "UNKNOWN"); rows.get(i).put("reason", "ORIGIN_READ_REFUSED");
                        rows.get(i).put("failureType", failureType(failure)); primary = failure; break;
                    } finally { if (i != 0) span.close(); }
                }
            } catch (Exception | LinkageError failure) { primary = failure; }
            try {
                runtime(after);
                check(before.equals(after), "producer runtime changed during the cold read");
            } catch (Exception | LinkageError failure) {
                if (primary == null) primary = failure; else suppress(primary, failure);
            }
            if (primary != null) {
                budget.gap("PRODUCER_ORIGINS_REFUSED");
                if (refusalObserver != null) {
                    try { refusalObserver.accept(primary); }
                    catch (RuntimeException | Error observerFailure) { suppress(primary, observerFailure); }
                }
            }
            var header = header(before, after, completed, primary);
            if (requests > serviceRequestUpper()) throw new AssertionError("cold origin requests exceeded the fixed source basis");
            String[] result = new String[CLASS_COUNT + 2]; result[0] = nativeRaw;
            result[1] = encodeRetainedPart(header, true, primary); long size = add(nativeRaw.length(), result[1].length());
            for (int i = 0; i < CLASS_COUNT; i++) { result[i + 2] = encodeRetainedPart(rows.get(i), false, primary); size = add(size, result[i + 2].length()); }
            if (size > MAX_PACKET_BYTES) throw new AssertionError("cold origin packet exceeded its independent bound");
            budget.material(4, size); budget.material(6, add(HEADER_NODES, mul(CLASS_COUNT, ORIGIN_NODES)));
            if (primary == null) first.success();
            return result;
        } finally { first.close(); }
    }

    private void observeClass(String expected, Map<String, Object> row) throws Exception {
        Class<?> type = request(() -> resolver.resolve(expected));
        check(type != null, "named class is unavailable");
        String name = request(type::getName); ascii(name, NAME_CHARS); row.put("actualName", name);
        check(expected.equals(name), "resolved class name differs");
        ClassLoader loader = request(type::getClassLoader); check(loader != null, "named class has no defining loader");
        String loaderClass = request(() -> loader.getClass().getName()); ascii(loaderClass, NAME_CHARS);
        row.put("loaderClass", loaderClass); row.put("loaderIdentityHashCode", request(() -> System.identityHashCode(loader)));
        var domain = request(type::getProtectionDomain); check(domain != null, "class protection domain is unavailable");
        var source = request(domain::getCodeSource); check(source != null, "class code source is unavailable");
        URL sourceUrl = request(source::getLocation); URI code = uri(sourceUrl); row.put("codeSource", code.toASCIIString());
        URL resource = request(() -> type.getResource("/" + expected.replace('.', '/') + ".class"));
        URI resourceUri = uri(resource); row.put("resource", resourceUri.toASCIIString());
        String entry = expected.replace('.', '/') + ".class"; ascii(entry, ENTRY_CHARS);
        Map<String, Object> route = route(code, true);
        row.put("container", route.get("container")); row.put("outer", route.get("outer")); row.put("module", route.get("module"));
        Map<String, Object> own = route(resourceUri, false);
        if ("DIRECTORY".equals(route.get("kind"))) {
            check("FILE".equals(own.get("kind")), "directory class resource is not a local member file");
            Path member = canonical(Path.of((String) own.get("container")));
            Path directory = Path.of((String) route.get("container"));
            check(member.startsWith(directory), "class resource shadows its directory code source");
            String observedEntry = directory.relativize(member).toString(); ascii(observedEntry, ENTRY_CHARS); row.put("entry", observedEntry);
            check(member.equals(directory.resolve(entry)), "class resource shadows its directory code source");
        } else {
            row.put("entry", own.get("entry"));
            check(route.get("container").equals(own.get("container")) && java.util.Objects.equals(route.get("module"), own.get("module")),
                    "class resource has another container");
            check(entry.equals(own.get("entry")), "class resource has another member");
        }
        URLConnection connection = request(resource::openConnection);
        request(() -> { connection.setUseCaches(false); return null; });
        InputStream input = request(() -> streams.open(connection)); check(input != null, "class stream is unavailable");
        hash(input, row);
        row.put("state", "RECORDED"); row.put("reason", "NONE"); row.put("failureType", "NONE");
    }

    /** Parses local locators before opening any URL connection; it does not own a supplied archive. */
    private Map<String, Object> route(URI uri, boolean codeSource) throws Exception {
        check(uri.getRawAuthority() == null && uri.getRawQuery() == null && uri.getRawFragment() == null,
                "class locator has an authority, query or fragment");
        var result = new LinkedHashMap<String, Object>();
        if ("file".equals(uri.getScheme())) {
            Path path = canonical(Path.of(uri)); boolean directory = request(() -> Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS));
            if (codeSource) check(directory || request(() -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)), "class container is not a local file");
            result.put("kind", directory ? "DIRECTORY" : "FILE"); result.put("container", path.toString());
            result.put("outer", directory ? null : path.toString()); result.put("module", null); result.put("entry", null);
            return result;
        }
        check("jar".equals(uri.getScheme()), "nonlocal class locator refused");
        String value = uri.getRawSchemeSpecificPart(); int memberAt = value.lastIndexOf("!/");
        check(memberAt >= 0 && value.indexOf("!/") == memberAt, "jar locator has an invalid member delimiter");
        String rawArchive = value.substring(0, memberAt), member = decode(value.substring(memberAt + 2));
        ascii(member, ENTRY_CHARS); check(codeSource ? member.isEmpty() : !member.isEmpty(), "jar member kind differs");
        Path outer; String module = null;
        if (rawArchive.startsWith("nested:")) {
            String nested = rawArchive.substring(7); int nestedAt = nested.indexOf("/!");
            check(nestedAt > 0 && nested.lastIndexOf("/!") == nestedAt, "nested container delimiter differs");
            outer = localFile("file:" + nested.substring(0, nestedAt)); module = decode(nested.substring(nestedAt + 2));
            ascii(module, ENTRY_CHARS); Path relative = Path.of(module);
            check(!relative.isAbsolute() && relative.equals(relative.normalize()) && !relative.startsWith("..")
                    && module.startsWith("BOOT-INF/lib/") && module.endsWith(".jar"), "nested module escapes the local application");
        } else outer = localFile(rawArchive);
        check(request(() -> Files.isRegularFile(outer, LinkOption.NOFOLLOW_LINKS)), "jar container is not a local regular file");
        result.put("kind", module == null ? "JAR" : "BOOT_NESTED_JAR"); result.put("container", outer.toString());
        result.put("outer", outer.toString()); result.put("module", module); result.put("entry", member); return result;
    }

    private Path localFile(String raw) throws Exception {
        URI value = new URI(raw); check("file".equals(value.getScheme()) && value.getRawAuthority() == null
                && value.getRawQuery() == null && value.getRawFragment() == null, "nonlocal archive container refused");
        return canonical(Path.of(value));
    }

    private static String decode(String raw) throws Exception {
        URI value = new URI("file:/" + raw); check(value.getRawAuthority() == null && value.getRawQuery() == null
                && value.getRawFragment() == null, "encoded member is not a local relative path");
        return value.getPath().substring(1);
    }

    private URI uri(URL value) throws Exception {
        check(value != null, "class URL is unavailable");
        String raw = request(value::toExternalForm); ascii(raw, URI_CHARS);
        URI uri = request(value::toURI); String canonical = uri.toASCIIString(); ascii(canonical, URI_CHARS);
        check(canonical.indexOf('"') < 0 && canonical.indexOf('\\') < 0, "class URI is not canonical printable ASCII");
        return uri;
    }

    private Path canonical(Path path) throws Exception {
        ascii(path.toString(), PATH_CHARS); check(path.isAbsolute() && path.equals(path.normalize()), "class path is not absolute and normalized");
        check(!request(() -> Files.isSymbolicLink(path)), "linked class container refused");
        Path real = request(path::toRealPath); check(path.equals(real), "class path resolves to another container");
        ascii(real.toString(), PATH_CHARS); return real;
    }

    private void runtime(Map<String, Object> result) throws Exception {
        for (String key : List.of("pid", "jvmStartTimeMillis", "javaHome", "javaVersion")) result.put(key, null);
        var process = request(ProcessHandle::current); long pid = request(process::pid); result.put("pid", pid);
        var bean = request(ManagementFactory::getRuntimeMXBean); long start = request(bean::getStartTime); result.put("jvmStartTimeMillis", start);
        String home = request(() -> System.getProperty("java.home")); ascii(home, HOME_CHARS); result.put("javaHome", home);
        String version = request(() -> System.getProperty("java.version")); ascii(version, VERSION_CHARS); result.put("javaVersion", version);
        Path path = canonical(Path.of(home));
        check(pid > 0 && start > 0, "producer runtime identity is unavailable");
        result.put("javaHome", path.toString());
    }

    private void hash(InputStream input, Map<String, Object> row) throws Exception {
        Throwable primary = null;
        try {
            var digest = request(() -> MessageDigest.getInstance("SHA-256")); byte[] buffer = new byte[BUFFER_BYTES]; long bytes = 0;
            row.put("bytes", 0L);
            while (true) {
                int limit = Math.toIntExact(Math.min(buffer.length, add(Math.subtractExact(CLASS_BYTES, bytes), 1)));
                int read = request(() -> input.read(buffer, 0, limit)); if (read < 0) break;
                check(read > 0 && read <= limit, "class stream made no progress or exceeded its requested slice");
                bytes = add(bytes, read); consumed = add(consumed, read); row.put("bytes", bytes); budget.material(4, read);
                check(bytes <= CLASS_BYTES, "class resource exceeds its prior material cap");
                request(() -> { digest.update(buffer, 0, read); return null; }); hashed = add(hashed, read); budget.material(6, read);
            }
            check(bytes > 0, "class resource is empty"); byte[] sum = request(digest::digest);
            row.put("sha256", request(() -> HexFormat.of().formatHex(sum)));
        } catch (Exception | Error failure) { primary = failure; throw failure; }
        finally {
            try { request(() -> { input.close(); return null; }); }
            catch (Exception | Error failure) { if (primary == null) throw failure; suppress(primary, failure); }
        }
    }

    private <T> T request(Callable<T> action) throws Exception {
        requests = add(requests, 1); budget.material(1, 1); return action.call();
    }

    private Map<String, Object> header(Map<String, Object> before, Map<String, Object> after, int completed, Throwable failure) {
        var result = new LinkedHashMap<String, Object>(); result.put("schemaVersion", 1);
        result.put("state", failure == null ? "RECORDED_UNQUALIFIED" : "UNKNOWN");
        for (String key : List.of("pid", "jvmStartTimeMillis", "javaHome", "javaVersion")) result.put(key, before.get(key));
        result.put("beforeRuntime", before); result.put("afterRuntime", after); result.put("expectedClassCount", CLASS_COUNT);
        result.put("completedClassCount", completed); result.put("serviceRequests", requests); result.put("classBytesConsumed", consumed);
        result.put("classBytesHashed", hashed); result.put("reason", failure == null ? "NONE" : "ORIGIN_READ_REFUSED");
        result.put("failureType", failure == null ? "NONE" : failureType(failure)); var suppressed = new ArrayList<String>();
        if (failure != null) {
            Throwable[] failures = failure.getSuppressed();
            if (failures.length > 8) { result.put("reason", "SUPPRESSED_ROSTER_OVERFLOW"); budget.gap("ORIGINS_SUPPRESSED_ROSTER_OVERFLOW"); }
            for (int i = 0; i < Math.min(failures.length, 8); i++) suppressed.add(failureType(failures[i]));
        }
        result.put("suppressedFailureTypes", suppressed); result.put("resolution", RESOLUTION); result.put("byteScope", BYTE_SCOPE);
        FLAGS.forEach(flag -> result.put(flag, false)); return result;
    }

    private static Map<String, Object> emptyRow(String expected) {
        var row = new LinkedHashMap<String, Object>();
        for (String field : ORIGIN_FIELDS) row.put(field, null);
        row.put("binaryName", expected); row.put("state", "UNOBSERVED"); row.put("reason", "NOT_OBSERVED"); row.put("failureType", "NONE");
        FLAGS.forEach(flag -> row.put(flag, false)); return row;
    }

    /** A retained source-tree failure is an invariant; an earlier refusal remains the exact cause. */
    static String encodeRetainedPart(Map<String, Object> tree, boolean header, Throwable original) {
        try { return encode(tree, header); }
        catch (IOException encodingFailure) {
            Throwable primary = original == null ? encodingFailure : original;
            if (original != null) suppress(original, encodingFailure);
            if (primary instanceof Error error) throw error;
            if (primary instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("bounded cold origin evidence could not be encoded", primary);
        }
    }

    private static String encode(Map<String, Object> tree, boolean header) throws IOException {
        long upper = header ? headerEncodingUpper() : originEncodingUpper(); int limit = header ? HEADER_BYTES : ORIGIN_BYTES;
        if (upper > limit) throw new AssertionError("source origin encoding plan exceeds its transport bound");
        long[] nodes = {0}; tree(tree, 0, nodes, header ? HEADER_NODES : ORIGIN_NODES);
        String raw = JsonWriter.write(tree);
        if (raw.length() > upper) throw new AssertionError("origin encoding exceeded the independent source plan");
        return raw;
    }

    private static void tree(Object value, int depth, long[] nodes, int cap) throws IOException {
        check(depth <= DEPTH && ++nodes[0] <= cap, "origin JSON shape exceeds its prior source bound");
        if (value == null || value instanceof Boolean) return;
        if (value instanceof Integer || value instanceof Long) { check(value.toString().length() <= NUMBER_TEXT, "origin numeric text exceeds its source bound"); return; }
        if (value instanceof String text) { ascii(text, URI_CHARS); return; }
        if (value instanceof Map<?, ?> map) {
            for (var item : map.entrySet()) { check(item.getKey() instanceof String, "origin JSON key is not text"); tree(item.getKey(), depth + 1, nodes, cap); tree(item.getValue(), depth + 1, nodes, cap); }
            return;
        }
        if (value instanceof List<?> list) { check(list.size() <= 8, "origin JSON list exceeds its prior source bound"); for (Object item : list) tree(item, depth + 1, nodes, cap); return; }
        throw new AssertionError("origin JSON value is outside its fixed source schema");
    }

    private static String failureType(Throwable failure) {
        String name = failure.getClass().getName();
        return name.length() <= FAILURE_CHARS && name.chars().allMatch(c -> c >= 32 && c <= 126 && c != '"' && c != '\\') ? name : "UNAVAILABLE";
    }
    private static void suppress(Throwable primary, Throwable cleanup) { if (primary != cleanup) primary.addSuppressed(cleanup); }
    private static void ascii(String value, int cap) throws IOException {
        check(value != null && value.length() <= cap, "origin text exceeds its prior source bound");
        for (int i = 0; i < value.length(); i++) check(value.charAt(i) >= 32 && value.charAt(i) <= 126, "origin text is not printable ASCII");
    }
    private static void check(boolean valid, String message) throws IOException { if (!valid) throw new IOException(message); }
    private static long chars(List<String> values) { long count = 0; for (String value : values) count = add(count, value.length()); return count; }
    private static long add(long... values) { long result = 0; for (long value : values) result = Math.addExact(result, value); return result; }
    private static long mul(long left, long right) { return Math.multiplyExact(left, right); }
}
