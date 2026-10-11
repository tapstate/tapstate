package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Parses obtained producer facts without substituting caller identities or qualifying measurements. */
final class BenchmarkReturnProducerOrigins {
    static final int PARTS = 34, ORIGINS = 32, SMALL_BYTES = 8192, ORIGIN_BYTES = 64512, TOTAL_BYTES = 2080768;
    static final long SERVICE_REQUEST_UPPER = Math.addExact(16, Math.multiplyExact(32L, Math.addExact(128, Math.multiplyExact(2L, 1048576L + 1))));
    static final List<String> NAMES = List.of(
            "PdkBenchmarkClock", "PdkBenchmarkClock$Clock", "PdkBenchmarkClock$JniAccess", "PdkBenchmarkClock$NativeAccess",
            "PdkWriteReturnProbe", "PdkWriteReturnProbe$Writer", "PdkWriteReturnProbe$Ticket", "PdkWriteReturnProbe$CostCall",
            "PdkWriteReturnProbe$CostStages", "PdkWriteReturnProbe$Frame", "PdkWriteReturnProbe$ErrorEntry", "PdkWriteReturnProbe$BoundedBytes",
            "PdkWriteReturnProbeMBean", "PdkWriteReturnWorkBudget", "PdkWriteReturnWorkBudget$Activity", "PdkWriteReturnWorkBudget$Actor",
            "PdkWriteReturnWorkBudget$Span", "PdkWriteReturnWorkBudget$Config", "PdkMongoWriteScope", "PdkMongoWriteScope$Call",
            "PdkMongoWriteScope$ThreadOwners", "PdkMongoWriteScope$Transactions", "PdkMongoWriteScope$Observation", "PdkMongoWriteScope$Access",
            "PdkMongoWriteScope$UnsupportedShape", "PdkSinkWriter", "io.tapstate.core.common.JsonWriter", "io.tapstate.core.common.JsonReader",
            "io.tapstate.core.common.JsonReader$Parser", "PdkReturnOrigins", "PdkReturnOrigins$Resolver", "PdkReturnOrigins$Streams")
            .stream().map(name -> name.startsWith("io.") ? name : "io.tapstate.adapters.pdk." + name).toList();
    static final List<String> FLAGS;
    private static final Set<String> HEADER_KEYS, ORIGIN_KEYS;
    private static final Set<String> RUNTIME_KEYS = Set.of("pid", "jvmStartTimeMillis", "javaHome", "javaVersion");
    static {
        var flags = new ArrayList<>(BenchmarkNativeClockEvidence.FLAGS);
        flags.addAll(List.of("sourceCoverageQualified", "emittedMemberCompletenessQualified", "runtimeInstructionBytesQualified",
                "structuralWorkBudgetQualified", "wholeMethodCostQualified", "causalOverheadQualified")); FLAGS = List.copyOf(flags);
        var header = new LinkedHashSet<>(List.of("schemaVersion", "state", "pid", "jvmStartTimeMillis", "javaHome", "javaVersion",
                "beforeRuntime", "afterRuntime", "expectedClassCount", "completedClassCount", "serviceRequests", "classBytesConsumed",
                "classBytesHashed", "reason", "failureType", "suppressedFailureTypes", "resolution", "byteScope"));
        header.addAll(flags); HEADER_KEYS = Set.copyOf(header);
        var origin = new LinkedHashSet<>(List.of("binaryName", "actualName", "state", "loaderClass", "loaderIdentityHashCode",
                "codeSource", "resource", "container", "outer", "module", "entry", "sha256", "bytes", "reason", "failureType"));
        origin.addAll(flags); ORIGIN_KEYS = Set.copyOf(origin);
    }
    enum Kind { DIRECTORY, JAR, BOOT_NESTED, UNAVAILABLE }
    record RuntimeFacts(Long pid, Long jvmStartTimeMillis, String javaHome, String javaVersion) { }
    record Header(String state, RuntimeFacts top, RuntimeFacts before, RuntimeFacts after, int completedClassCount,
                  long serviceRequests, long classBytesConsumed, long classBytesHashed, Map<String, Object> raw) { }
    record RecordPart(String binaryName, String actualName, String state, String loaderClass, Long loaderIdentityHashCode,
                      String codeSource, String resource, String container, String outer, String module, String entry,
                      String sha256, Long bytes, String reason, String failureType, Kind kind, Map<String, Object> raw) { }
    record Parsed(String nativeRaw, Header header, List<RecordPart> typedParts, Map<String, Object> evidence) {
        Parsed { typedParts = List.copyOf(typedParts); evidence = immutable(evidence); }
    }
    static final class Refusal extends AssertionError {
        private final String nativeRaw;
        private final Map<String, Object> evidence;
        Refusal(Throwable cause, String nativeRaw, Map<String, Object> evidence) {
            super("producer origins packet refused", cause); this.nativeRaw = nativeRaw; this.evidence = immutable(evidence);
        }
        String nativeRaw() { return nativeRaw; }
        Map<String, Object> retainedEvidence() { return evidence; }
    }
    private BenchmarkReturnProducerOrigins() { }

    static Parsed parse(Object packet, BenchmarkCausalClock.Identity expectedOwned, String expectedAppPath) {
        var scope = BenchmarkReturnWorkBudget.current();
        var ticket = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.DECODE, BenchmarkReturnWorkBudget.Work.call());
        var raw = new ArrayList<String>(); var parts = new ArrayList<RecordPart>(); Header header = null; String nativeRaw = null;
        try {
            require(packet instanceof String[] && ((String[]) packet).length == PARTS, "packet part roster differs");
            String[] supplied = (String[]) packet; long bytes = 0;
            for (int i = 0; i < supplied.length; i++) {
                ascii(supplied[i], i < 2 ? SMALL_BYTES : ORIGIN_BYTES);
                if (i == 0) nativeRaw = supplied[i];
                bytes = Math.addExact(bytes, supplied[i].length()); require(bytes <= TOTAL_BYTES, "packet total material exceeds bound");
            }
            String[] copy = supplied.clone();
            // The RPC array is mutable; recheck the captured references before any decoding.
            bytes = 0;
            for (int i = 0; i < copy.length; i++) {
                ascii(copy[i], i < 2 ? SMALL_BYTES : ORIGIN_BYTES); raw.add(copy[i]); bytes = Math.addExact(bytes, copy[i].length());
                require(bytes <= TOTAL_BYTES, "captured packet total material exceeds bound");
            }
            nativeRaw = copy[0]; require(expectedOwned != null, "owned runtime is missing"); path(expectedAppPath, 4096);
            header = header(object(copy[1], SMALL_BYTES, 128, 4096, HEADER_KEYS), expectedOwned);
            long recordedBytes = 0; int recorded = 0;
            for (int i = 0; i < ORIGINS; i++) {
                var part = part(object(copy[i + 2], ORIGIN_BYTES, 96, 18448, ORIGIN_KEYS), NAMES.get(i), expectedAppPath);
                parts.add(part);
                if (part.state().equals("RECORDED")) { recorded++; recordedBytes = Math.addExact(recordedBytes, part.bytes()); }
            }
            require(header.completedClassCount() == recorded, "header completed count differs from obtained parts");
            if (header.state().equals("RECORDED_UNQUALIFIED")) {
                require(recorded == ORIGINS && header.classBytesConsumed() == recordedBytes && header.classBytesHashed() == recordedBytes,
                        "complete header has missing origins or inconsistent byte totals");
            } else {
                require(header.classBytesConsumed() >= recordedBytes && header.classBytesHashed() >= recordedBytes,
                        "partial header contradicts recorded byte totals");
            }
            var evidence = evidence(header, parts, raw, "PRODUCER_ORIGIN_FACTS_RECORDED");
            if (header.state().equals("UNKNOWN")) evidence.put("state", "UNKNOWN");
            BenchmarkReturnWorkBudget.done(ticket, new BenchmarkReturnWorkBudget.Work(0, 0, 0, ORIGINS, bytes, 1, bytes, parts.size()));
            return new Parsed(nativeRaw, header, parts, evidence);
        } catch (RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failure(scope, failure); BenchmarkReturnWorkBudget.failed(ticket, failure);
            var evidence = evidence(header, parts, raw, "UNKNOWN"); evidence.put("failureType", failure.getClass().getName());
            throw new Refusal(failure, nativeRaw, evidence);
        }
    }

    private static Header header(Map<String, Object> root, BenchmarkCausalClock.Identity expected) {
        require(number(root.get("schemaVersion"), false) == 1 && number(root.get("expectedClassCount"), false) == ORIGINS, "header schema or expected class count differs");
        flags(root); String state = text(root.get("state"), 32, false);
        require(state.equals("RECORDED_UNQUALIFIED") || state.equals("UNKNOWN"), "header state differs");
        boolean unknown = state.equals("UNKNOWN");
        RuntimeFacts before = runtime(root.get("beforeRuntime"), unknown), after = runtime(root.get("afterRuntime"), unknown);
        var top = new RuntimeFacts(number(root.get("pid"), true), number(root.get("jvmStartTimeMillis"), true),
                text(root.get("javaHome"), 512, true), text(root.get("javaVersion"), 128, true));
        require(top.equals(before), "top runtime does not duplicate the obtained before runtime");
        if (!unknown && top.javaHome() != null) path(top.javaHome(), 512);
        String reason = text(root.get("reason"), 128, false), failure = text(root.get("failureType"), 128, false);
        require(root.get("suppressedFailureTypes") instanceof List<?> suppressed && suppressed.size() <= 8, "suppressed failure roster differs");
        for (Object item : (List<?>) root.get("suppressedFailureTypes")) text(item, 128, false);
        require("EXACT_NAMES_INITIALIZE_FALSE".equals(root.get("resolution"))
                && "OWN_RESOURCE_CODE_SOURCE_AND_CONTAINER_NOT_VM_INSTRUCTION_BODY".equals(root.get("byteScope")), "header measurement scope differs");
        long complete = number(root.get("completedClassCount"), false), consumed = number(root.get("classBytesConsumed"), false), hashed = number(root.get("classBytesHashed"), false);
        require(complete <= ORIGINS && consumed <= ORIGINS * (1048576L + 1) && hashed <= consumed, "header counter scalar exceeds source bound");
        if (state.equals("RECORDED_UNQUALIFIED")) {
            require(matches(before, expected) && before.equals(after) && reason.equals("NONE") && failure.equals("NONE")
                    && ((List<?>) root.get("suppressedFailureTypes")).isEmpty(), "successful header has another runtime or failure");
        } else {
            require(Set.of("ORIGIN_READ_REFUSED", "UNAVAILABLE", "SUPPRESSED_ROSTER_OVERFLOW").contains(reason)
                    && !failure.equals("NONE"), "unknown header has no refusal cause");
        }
        long services = number(root.get("serviceRequests"), false); require(services <= SERVICE_REQUEST_UPPER, "header service requests exceed fixed source bound");
        return new Header(state, top, before, after, Math.toIntExact(complete), services, consumed, hashed, root);
    }
    private static RuntimeFacts runtime(Object value, boolean unknown) {
        require(value instanceof Map<?, ?>, "runtime snapshot is not an object"); Map<?, ?> map = (Map<?, ?>) value;
        if (map.isEmpty()) return new RuntimeFacts(null, null, null, null);
        require(map.keySet().equals(RUNTIME_KEYS), "runtime snapshot fields differ");
        Long pid = number(map.get("pid"), unknown), start = number(map.get("jvmStartTimeMillis"), unknown);
        require((pid == null || pid > 0) && (start == null || start > 0), "obtained runtime identity is not positive");
        String home = text(map.get("javaHome"), 512, unknown); if (!unknown) path(home, 512);
        return new RuntimeFacts(pid, start, home, text(map.get("javaVersion"), 128, unknown));
    }
    private static boolean matches(RuntimeFacts actual, BenchmarkCausalClock.Identity expected) {
        return actual.pid() != null && actual.pid() == expected.pid() && actual.jvmStartTimeMillis() == expected.jvmStartTimeMillis();
    }

    private static RecordPart part(Map<String, Object> map, String expected, String app) {
        flags(map); require(expected.equals(map.get("binaryName")), "ordered static origin roster differs");
        String state = text(map.get("state"), 32, false); require(Set.of("RECORDED", "UNKNOWN", "UNOBSERVED").contains(state), "origin state differs");
        String actual = text(map.get("actualName"), 256, true), loader = text(map.get("loaderClass"), 256, true);
        Long loaderId = signed(map.get("loaderIdentityHashCode"));
        String code = uriText(map.get("codeSource")), resource = uriText(map.get("resource"));
        String container = text(map.get("container"), 4096, true), outer = text(map.get("outer"), 4096, true);
        String module = text(map.get("module"), 1024, true), entry = text(map.get("entry"), 1024, true), sha = text(map.get("sha256"), 64, true);
        Long bytes = number(map.get("bytes"), true); if (sha != null) hash(sha);
        require(bytes == null || bytes <= 1048576L + (state.equals("UNKNOWN") ? 1 : 0), "class byte scalar exceeds source bound");
        String reason = text(map.get("reason"), 128, false), failure = text(map.get("failureType"), 128, false); Kind kind = Kind.UNAVAILABLE;
        if (state.equals("UNOBSERVED")) {
            require(actual == null && loader == null && loaderId == null && code == null && resource == null && container == null
                    && outer == null && module == null && entry == null && sha == null && bytes == null
                    && reason.equals("NOT_OBSERVED") && failure.equals("NONE"), "unobserved origin fabricates obtained fields");
        } else if (state.equals("RECORDED")) {
            require(expected.equals(actual) && loader != null && loaderId != null && code != null && resource != null
                    && container != null && entry != null && sha != null && bytes != null && bytes > 0
                    && reason.equals("NONE") && failure.equals("NONE"), "recorded origin is incomplete or failed");
            kind = route(expected, code, resource, container, outer, module, entry, app);
        } else {
            require(Set.of("ORIGIN_READ_REFUSED", "UNAVAILABLE").contains(reason) && !failure.equals("NONE"), "unknown origin has no refusal cause");
        }
        return new RecordPart(expected, actual, state, loader, loaderId, code, resource, container, outer, module, entry, sha, bytes, reason, failure, kind, map);
    }
    private static Kind route(String binary, String code, String resource, String container, String outer, String module, String entry, String app) {
        path(container, 4096); require(entry.equals(binary.replace('.', '/') + ".class"), "actual class entry differs");
        if (module != null) {
            require(app.equals(outer) && outer.equals(container), "Boot outer application binding differs");
            relative(module); String artifact = binary.startsWith("io.tapstate.core.common.") ? "core-common" : "adapter-pdk";
            require(module.startsWith("BOOT-INF/lib/" + artifact + "-") && module.endsWith(".jar"), "Boot module class owner differs");
            String prefix = "jar:nested:" + encodedPath(outer) + "/!" + encodedPath(module) + "!/";
            require(code.equals(prefix) && resource.equals(prefix + encodedPath(entry)), "actual Boot nested URI layout differs"); return Kind.BOOT_NESTED;
        }
        if (outer != null) {
            require(outer.equals(container) && outer.equals(app), "local jar outer application binding differs");
            require(filePath(code).equals(outer), "local jar code source differs");
            require(resource.startsWith("jar:") && resource.endsWith("!/" + encodedPath(entry)), "local jar class URI differs");
            String inner = resource.substring(4, resource.length() - 2 - encodedPath(entry).length());
            require(filePath(inner).equals(outer), "local jar resource outer differs"); return Kind.JAR;
        }
        require(filePath(code).equals(container) && filePath(resource).equals(Path.of(container).resolve(entry).toString()), "directory code source or class resource differs");
        return Kind.DIRECTORY;
    }
    private static String filePath(String value) {
        URI uri = uri(value); require("file".equals(uri.getScheme()) && !uri.isOpaque() && uri.getRawAuthority() == null
                && uri.getRawQuery() == null && uri.getRawFragment() == null, "nonlocal file URI refused");
        String valuePath = Path.of(uri).normalize().toString(); path(valuePath, 4096); return valuePath;
    }
    private static String encodedPath(String value) { try { return new URI(null, null, value, null).toASCIIString(); } catch (URISyntaxException failure) { throw new AssertionError(failure); } }
    private static URI uri(String value) {
        try { URI uri = new URI(value); require(uri.toASCIIString().equals(value) && uri.normalize().toString().equals(value), "URI is not canonical ASCII"); return uri; }
        catch (URISyntaxException failure) { throw new AssertionError("producer URI is malformed", failure); }
    }
    private static void relative(String value) { Path path = Path.of(value); require(!path.isAbsolute() && path.normalize().toString().equals(value) && !path.startsWith(".."), "module path is not an exact relative route"); }
    private static String uriText(Object value) { String text = text(value, 18448, true); if (text != null) require(text.indexOf('"') < 0 && text.indexOf('\\') < 0, "URI contains forbidden literal characters"); return text; }

    private static Map<String, Object> object(String raw, int bytes, int nodes, int leaf, Set<String> keys) {
        BenchmarkReturnWorkBudgetDefinitions.requireJsonShape(raw, bytes, 4, nodes, leaf);
        scalars(raw);
        Object decoded = JsonReader.parse(raw);
        require(decoded instanceof Map<?, ?> && ((Map<?, ?>) decoded).keySet().equals(keys), "JSON object fields differ");
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(nodes, 4, leaf, bytes, 64, 24);
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(decoded, plan);
        require(JsonWriter.write(frozen.value()).equals(raw), "JSON is noncanonical or has duplicate fields");
        @SuppressWarnings("unchecked") Map<String, Object> result = (Map<String, Object>) frozen.value(); return result;
    }
    private static void flags(Map<String, Object> map) { for (String flag : FLAGS) require(Boolean.FALSE.equals(map.get(flag)), "producer metadata claims a qualification"); }
    private static void scalars(String raw) {
        boolean quoted = false, escaped = false; int scalar = 0;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (quoted) { if (escaped) escaped = false; else if (c == '\\') escaped = true; else if (c == '"') quoted = false; continue; }
            if (c == '"') { quoted = true; scalar = 0; }
            else if (c == '{' || c == '}' || c == '[' || c == ']' || c == ':' || c == ',' || Character.isWhitespace(c)) scalar = 0;
            else require(++scalar <= 20, "JSON scalar exceeds producer numeric text bound");
        }
    }
    private static Long number(Object value, boolean nullable) { if (value == null && nullable) return null; require(value instanceof Long && (Long) value >= 0, "scalar is not an exact nonnegative long"); return (Long) value; }
    private static Long signed(Object value) { if (value == null) return null; require(value instanceof Long && (Long) value >= Integer.MIN_VALUE && (Long) value <= Integer.MAX_VALUE, "loader identity is not a signed integer"); return (Long) value; }
    private static String text(Object value, int bound, boolean nullable) { if (value == null && nullable) return null; require(value instanceof String, "text type differs"); ascii((String) value, bound); return (String) value; }
    private static void ascii(String value, int bound) { require(value != null && !value.isEmpty() && value.length() <= bound, "text is missing or exceeds prior bound"); for (int i = 0; i < value.length(); i++) require(value.charAt(i) >= 32 && value.charAt(i) <= 126, "text is not printable ASCII"); }
    private static void path(String value, int bound) { ascii(value, bound); Path path = Path.of(value); require(path.isAbsolute() && path.normalize().toString().equals(value), "path is not an exact absolute route"); }
    private static void hash(String value) { require(value.matches("[a-f0-9]{64}"), "class hash type differs"); }
    private static Map<String, Object> evidence(Header header, List<RecordPart> parts, List<String> raw, String state) {
        var evidence = new LinkedHashMap<String, Object>(); evidence.put("schemaVersion", 1); evidence.put("state", state);
        evidence.put("header", header == null ? null : header.raw()); evidence.put("originParts", parts.stream().map(RecordPart::raw).toList());
        evidence.put("rawParts", List.copyOf(raw)); evidence.put("rawEncoding", "PLAIN_BOUNDED_ASCII_PARTS");
        evidence.put("scope", "ACTUAL_HEADER_IDENTITIES_AND_DECLARED_ORIGIN_FACTS_NOT_VM_INSTRUCTIONS");
        FLAGS.forEach(flag -> evidence.put(flag, false)); return evidence;
    }
    static BenchmarkReturnWorkBudget.Work localUpper() {
        long bytes = TOTAL_BYTES, nodes = Math.addExact(128, Math.multiplyExact(96L, ORIGINS));
        return new BenchmarkReturnWorkBudget.Work(Math.addExact(128, Math.multiplyExact(32, nodes)),
                Math.addExact(1024, Math.addExact(Math.multiplyExact(64, bytes), Math.multiplyExact(128, nodes))), ORIGINS, ORIGINS,
                Math.addExact(Math.multiplyExact(16, bytes), Math.multiplyExact(256, nodes)), 12,
                Math.addExact(Math.multiplyExact(16, bytes), Math.multiplyExact(32, nodes)), Math.multiplyExact(32, nodes));
    }
    private static Map<String, Object> immutable(Map<String, Object> value) { return Collections.unmodifiableMap(new LinkedHashMap<>(value)); }
    private static void require(boolean value, String reason) { if (!value) throw new AssertionError("producer origin facts " + reason); }
}
