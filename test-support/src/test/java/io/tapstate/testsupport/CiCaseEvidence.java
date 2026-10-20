package io.tapstate.testsupport;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.DynamicTestInvocationContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.opentest4j.TestAbortedException;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Opt-in observation of the original test invocation, never a selection or execution policy.
 * Markers contain hashes of arguments and display names, not their plaintext. The XML outcome
 * remains authoritative: an invocation can return before an after-each callback fails.
 *
 * <p>Canonical arguments are compact UTF-8 JSON tagged arrays. Scalars carry their Java type and
 * value; ordinary numbers use a plain decimal without insignificant zeroes, BigDecimal preserves
 * its unscaled value and scale, enums their declaring binary
 * class and name, and paths their normalized slash spelling. Lists and arrays retain order, maps
 * sort complete canonical key/value pairs by unsigned UTF-8 bytes, and records retain declaration
 * component order. Unsupported objects, cycles and bounded-size failures are explicitly unstable.
 * Only declared record components are inspected; getters and toString are never invoked.
 * Injected @TempDir arguments bind to their declared annotation and type, never a random fixture path.
 */
public final class CiCaseEvidence implements InvocationInterceptor {
    static final String ENABLED = "tapstate.ci.case-evidence";
    static final String MARKER = "tapstate.ci-case=";
    private static final int MAX_DEPTH = 32;
    private static final int MAX_NODES = 8192;
    private static final int MAX_ITEMS = 4096;
    private static final int MAX_STRING_LENGTH = 65536;
    private static final Set<String> BUILTIN_LISTS = Set.of(
            "java.util.ArrayList", "java.util.LinkedList", "java.util.Arrays$ArrayList",
            "java.util.ImmutableCollections$List12", "java.util.ImmutableCollections$ListN",
            "java.util.Collections$EmptyList", "java.util.Collections$SingletonList");
    private static final Set<String> BUILTIN_MAPS = Set.of(
            "java.util.HashMap", "java.util.LinkedHashMap", "java.util.TreeMap",
            "java.util.ImmutableCollections$Map1", "java.util.ImmutableCollections$MapN",
            "java.util.Collections$EmptyMap", "java.util.Collections$SingletonMap");
    private static final Pattern REPETITION = Pattern.compile("\\[test-template-invocation:#(\\d+)]$");

    static final String CLASS_DIRECTORY = "tapstate.ci.class-gate-dir";
    static final String CLASS_MARKER = "tapstate.ci-class-gate=";
    private static final List<String> GATE_PROPERTIES = List.of(
            "tapstate.e2e.connectors-dir", "tapstate.e2e.upgrade-lane", "tapstate.pdk.it.postgres-jar",
            "tapstate.pdk.it.mysql-jar", "tapstate.pdk.it.oracle-jar", "tapstate.pdk.it.sqlserver-jar");
    private static final List<String> GATE_ENVIRONMENTS = List.of(
            "TAPSTATE_TEST_NATIVE_CLI", "TAPSTATE_ATLAS_TEST_URI", "TAPSTATE_ATLAS_STANDARD_URI");

    @Override
    public void interceptBeforeAllMethod(Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext, ExtensionContext context) throws Throwable {
        if (!Boolean.getBoolean(ENABLED)) {
            invocation.proceed();
            return;
        }
        String record;
        try {
            Method method = invocationContext.getExecutable();
            record = "\"schema\":1,\"class\":" + quote(context.getTestClass().orElse(method.getDeclaringClass()).getName())
                    + ",\"method\":" + quote(method.getName()) + ",\"declaring_class\":" + quote(method.getDeclaringClass().getName())
                    + ",\"unique_id\":" + quote(context.getUniqueId()) + ",\"stable\":true"
                    + ",\"source_sha\":" + optional(System.getenv("GITHUB_SHA"))
                    + ",\"run_id\":" + optional(System.getenv("GITHUB_RUN_ID"))
                    + ",\"run_attempt\":" + optional(System.getenv("GITHUB_RUN_ATTEMPT"))
                    + ",\"gate_inputs\":" + gateInputs();
        } catch (Throwable failedObservation) {
            record = "\"schema\":1,\"stable\":false,\"reason\":\"class-observation-unavailable\"";
        }
        emitWithPrefix(CLASS_MARKER, record, "start", null, null);
        try {
            invocation.proceed();
        } catch (Throwable original) {
            String reason = null;
            if (original.getClass() == TestAbortedException.class) {
                try { reason = hash(quote(original.getMessage())); } catch (Throwable unavailable) { }
            }
            emitWithPrefix(CLASS_MARKER, record, "finish", original instanceof TestAbortedException ? "aborted" : "failed", reason);
            throw original;
        }
        emitWithPrefix(CLASS_MARKER, record, "finish", "executed", null);
    }

    static String gateInputs() {
        List<String> properties = new ArrayList<>();
        for (String name : GATE_PROPERTIES) {
            properties.add(quote(name) + ":" + flags(System.getProperty(name)));
        }
        List<String> environments = new ArrayList<>();
        for (String name : GATE_ENVIRONMENTS) {
            environments.add(quote(name) + ":" + flags(System.getenv(name)));
        }
        return "{\"properties\":{" + String.join(",", properties) + "},\"environment\":{" + String.join(",", environments) + "}}";
    }

    private static String flags(String value) {
        return "{\"present\":" + (value != null) + ",\"nonblank\":" + (value != null && !value.isBlank())
                + ",\"boolean_true\":" + "true".equalsIgnoreCase(value) + "}";
    }

    private static String optional(String value) {
        return value == null ? "null" : quote(value);
    }

    private static void emitWithPrefix(String prefix, String record, String phase, String status, String reason) {
        try {
            String line = "{" + record + ",\"phase\":" + quote(phase)
                    + (status == null ? "" : ",\"status\":" + quote(status))
                    + (reason == null ? "" : ",\"reason_digest\":" + quote(reason)) + "}";
            // The optional directory is pre-created and checked by the worker. No
            // resource is activated here; a failed append means missing evidence.
            String configured = System.getProperty(CLASS_DIRECTORY);
            if (configured != null) {
                try {
                    Path directory = Path.of(configured);
                    if (directory.isAbsolute() && Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                        Path file = directory.resolve("before-all-" + ProcessHandle.current().pid() + ".jsonl");
                        if (!Files.isSymbolicLink(file)) {
                            synchronized (CiCaseEvidence.class) {
                                Files.writeString(file, line + "\n", StandardCharsets.UTF_8,
                                        StandardOpenOption.CREATE, StandardOpenOption.APPEND, LinkOption.NOFOLLOW_LINKS);
                            }
                        }
                    }
                } catch (Throwable unavailableFile) {
                    // Original invocation and exception handling remain authoritative.
                }
            }
            System.out.println(prefix + line);
        } catch (Throwable unavailableOutput) {
            // Observation failure cannot replace the original invocation outcome.
        }
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext, ExtensionContext context) throws Throwable {
        observe(invocation, invocationContext.getExecutable(), invocationContext.getArguments(), context, "test");
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext, ExtensionContext context) throws Throwable {
        Method method = invocationContext.getExecutable();
        observe(invocation, method, invocationContext.getArguments(), context,
                method.isAnnotationPresent(RepeatedTest.class) ? "repeated" : "template");
    }

    @Override
    public void interceptDynamicTest(Invocation<Void> invocation,
            DynamicTestInvocationContext invocationContext, ExtensionContext context) throws Throwable {
        observe(invocation, testMethod(context).orElse(null), List.of(), context, "dynamic");
    }

    private static Optional<Method> testMethod(ExtensionContext context) {
        for (ExtensionContext current = context; current != null; current = current.getParent().orElse(null)) {
            if (current.getTestMethod().isPresent()) {
                return current.getTestMethod();
            }
        }
        return Optional.empty();
    }

    private static void observe(Invocation<Void> invocation, Method method, List<Object> arguments,
            ExtensionContext context, String kind) throws Throwable {
        if (!Boolean.getBoolean(ENABLED)) {
            invocation.proceed();
            return;
        }
        String record;
        try {
            record = record(method, arguments, context, kind);
        } catch (Throwable failedObservation) {
            // Evidence collection must not turn an original passing invocation into a failure.
            // Admission rejects the explicit incomplete observation instead.
            record = "\"schema\":1,\"stable\":false,\"reason\":\"observation-unavailable\"";
        }
        emit(record, "start", null);
        try {
            invocation.proceed();
        } catch (Throwable original) {
            emit(record, "finish", original instanceof TestAbortedException ? "aborted" : "failed");
            throw original;
        }
        emit(record, "finish", "executed");
    }

    private static void emit(String record, String phase, String status) {
        try {
            System.out.println(MARKER + "{" + record + ",\"phase\":" + quote(phase)
                    + (status == null ? "" : ",\"status\":" + quote(status)) + "}");
        } catch (Throwable unavailableOutput) {
            // Missing evidence is rejected by admission; an output failure cannot replace the test.
        }
    }

    private static String record(Method method, List<Object> arguments, ExtensionContext context, String kind) {
        List<String> values = new ArrayList<>();
        List<String> descriptors = new ArrayList<>();
        boolean stable = method != null && validUnicode(context.getDisplayName());
        String firstString = null;
        for (int i = 0; i < arguments.size(); i++) {
            Object argument = arguments.get(i);
            boolean temporary = method != null && i < method.getParameterCount()
                    && method.getParameters()[i].isAnnotationPresent(TempDir.class);
            boolean testInfo = method != null && i < method.getParameterCount()
                    && method.getParameterTypes()[i] == TestInfo.class;
            boolean injected = temporary || testInfo || method != null && i < method.getParameterCount()
                    && method.getParameterTypes()[i] == RepetitionInfo.class;
            Canonical value = temporary ? known("[\"injected\",\"org.junit.jupiter.api.io.TempDir\","
                    + quote(method.getParameterTypes()[i].getName()) + "]")
                    : testInfo ? known("[\"injected\",\"org.junit.jupiter.api.TestInfo\"]") : canonical(argument);
            values.add(value.json());
            stable &= value.stable();
            descriptors.add("{\"position\":" + i + ",\"type\":" + quote(type(argument))
                    + ",\"declared_type\":" + quote(method != null && i < method.getParameterCount()
                            ? method.getParameterTypes()[i].getName() : "unknown")
                    + ",\"stable\":" + value.stable() + ",\"injected\":" + injected
                    + ",\"digest\":" + quote(hash(value.json()))
                    + (value.reason() == null ? "" : ",\"reason\":" + quote(value.reason())) + "}");
            if (i == 0 && argument instanceof String && value.stable()) {
                firstString = hash(value.json());
            }
        }
        Integer repetition = repetition(method, arguments, context, kind);
        if (kind.equals("repeated") && repetition == null) {
            stable = false;
        }
        String className = context.getTestClass().map(Class::getName)
                .orElse(method == null ? "unknown" : method.getDeclaringClass().getName());
        return "\"schema\":1,\"class\":" + quote(className)
                + ",\"method\":" + quote(method == null ? "unknown" : method.getName())
                + ",\"unique_id\":" + quote(context.getUniqueId())
                + ",\"display_name_digest\":" + quote(hash(quote(context.getDisplayName())))
                + ",\"argument_digest\":" + quote(hash("[\"arguments\",[" + String.join(",", values) + "]]"))
                + ",\"argument_descriptors\":[" + String.join(",", descriptors) + "]"
                + ",\"first_string_digest\":" + (firstString == null ? "null" : quote(firstString))
                + ",\"stable\":" + stable + ",\"identity_kind\":" + quote(kind)
                + ",\"repetition\":" + (repetition == null ? "null" : repetition)
                + ",\"gate_inputs\":" + gateInputs();
    }

    private static Integer repetition(Method method, List<Object> arguments, ExtensionContext context, String kind) {
        if (!kind.equals("repeated")) {
            return null;
        }
        for (Object argument : arguments) {
            if (argument instanceof RepetitionInfo info) {
                return info.getCurrentRepetition();
            }
        }
        // A template invocation ordinal is a repetition only when the source is @RepeatedTest.
        // ParameterizedTest indices must never become stable parameter identities.
        RepeatedTest repeated = method.getAnnotation(RepeatedTest.class);
        Matcher match = REPETITION.matcher(context.getUniqueId());
        if (match.find()) {
            int current = Integer.parseInt(match.group(1));
            if (current > 0 && current <= repeated.value()) {
                return current;
            }
        }
        return null;
    }

    record Canonical(String json, boolean stable, String reason) {
    }

    static Canonical canonical(Object value) {
        try {
            return canonical(value, new IdentityHashMap<>(), new int[] {0}, 0);
        } catch (Throwable unavailableObservation) {
            return opaque(value, "observation-unavailable");
        }
    }

    private static boolean builtinCollection(Object value, Set<String> names) {
        Class<?> type = value.getClass();
        // Exact bootstrap types exclude user subclasses and wrappers delegating to unknown code.
        return type.getClassLoader() == null && names.contains(type.getName());
    }

    private static boolean validUnicode(String text) {
        for (int i = 0; i < text.length(); i++) {
            char character = text.charAt(i);
            if (Character.isHighSurrogate(character)) {
                if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(++i))) {
                    return false;
                }
            } else if (Character.isLowSurrogate(character)) {
                return false;
            }
        }
        return true;
    }

    private static Canonical canonical(Object value, IdentityHashMap<Object, Boolean> active, int[] nodes, int depth) {
        if (++nodes[0] > MAX_NODES || depth > MAX_DEPTH) {
            return opaque(value, "size-limit");
        }
        if (value == null) {
            return known("[\"null\"]");
        }
        if (value instanceof String text) {
            if (!validUnicode(text)) {
                return opaque(value, "ill-formed-unicode");
            }
            return text.length() <= MAX_STRING_LENGTH ? known("[\"java.lang.String\"," + quote(text) + "]")
                    : opaque(value, "size-limit");
        }
        if (value instanceof Character character) {
            return Character.isSurrogate(character) ? opaque(value, "ill-formed-unicode")
                    : known("[\"java.lang.Character\"," + quote(character.toString()) + "]");
        }
        if (value instanceof Boolean bool) {
            return known("[\"java.lang.Boolean\"," + quote(bool.toString()) + "]");
        }
        if (number(value)) {
            if (value instanceof BigDecimal decimal
                    && (Math.abs((long) decimal.scale()) > MAX_STRING_LENGTH || decimal.precision() > MAX_STRING_LENGTH)
                    || value instanceof BigInteger integer && integer.bitLength() > MAX_STRING_LENGTH * 3) {
                return opaque(value, "size-limit");
            }
            if (value instanceof BigDecimal decimal) {
                return known("[\"java.math.BigDecimal\"," + quote(decimal.unscaledValue().toString())
                        + "," + decimal.scale() + "]");
            }
            String spelling = value.toString();
            boolean negativeZero = value instanceof Double doubleValue && Double.doubleToLongBits(doubleValue) == Long.MIN_VALUE
                    || value instanceof Float floatValue && Float.floatToIntBits(floatValue) == Integer.MIN_VALUE;
            if (negativeZero) {
                spelling = "-0";
            } else if (!(spelling.equals("NaN") || spelling.equals("Infinity") || spelling.equals("-Infinity"))) {
                spelling = new BigDecimal(spelling).stripTrailingZeros().toPlainString();
            }
            return known("[" + quote(type(value)) + "," + quote(spelling) + "]");
        }
        if (value instanceof Enum<?> enumeration) {
            return known("[\"enum\"," + quote(enumeration.getDeclaringClass().getName()) + ","
                    + quote(enumeration.name()) + "]");
        }
        if (value instanceof Path path) {
            String implementation = value.getClass().getName();
            if (!(implementation.equals("sun.nio.fs.UnixPath") || implementation.equals("sun.nio.fs.WindowsPath")
                    || implementation.equals("jdk.nio.zipfs.ZipPath"))) {
                return opaque(value, "unsupported-path-implementation");
            }
            return known("[\"path\"," + quote(path.normalize().toString().replace('\\', '/')) + "]");
        }
        if (value instanceof RepetitionInfo info) {
            return known("[\"repetition\"," + info.getCurrentRepetition() + "," + info.getTotalRepetitions() + "]");
        }
        if (value instanceof List<?> && !builtinCollection(value, BUILTIN_LISTS)
                || value instanceof Map<?, ?> && !builtinCollection(value, BUILTIN_MAPS)
                        && !value.getClass().getName().equals("org.bson.Document")) {
            return opaque(value, "unsupported-collection-implementation");
        }
        boolean compound = value instanceof List<?> || value instanceof Map<?, ?> || value.getClass().isArray()
                || value.getClass().isRecord();
        if (!compound) {
            return opaque(value, "unsupported-type");
        }
        if (active.put(value, Boolean.TRUE) != null) {
            return opaque(value, "cycle");
        }
        try {
            List<Canonical> children = new ArrayList<>();
            String tag;
            if (value instanceof List<?> list) {
                if (list.size() > MAX_ITEMS) {
                    return opaque(value, "size-limit");
                }
                tag = "\"list\"";
                for (Object element : list) {
                    children.add(canonical(element, active, nodes, depth + 1));
                }
            } else if (value instanceof Map<?, ?> map) {
                if (map.size() > MAX_ITEMS) {
                    return opaque(value, "size-limit");
                }
                tag = "\"map\"";
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Canonical key = canonical(entry.getKey(), active, nodes, depth + 1);
                    Canonical mapped = canonical(entry.getValue(), active, nodes, depth + 1);
                    children.add(new Canonical("[" + key.json() + "," + mapped.json() + "]",
                            key.stable() && mapped.stable(), key.reason() != null ? key.reason() : mapped.reason()));
                }
                children.sort((left, right) -> Arrays.compareUnsigned(left.json().getBytes(StandardCharsets.UTF_8),
                        right.json().getBytes(StandardCharsets.UTF_8)));
            } else if (value.getClass().isArray()) {
                if (Array.getLength(value) > MAX_ITEMS) {
                    return opaque(value, "size-limit");
                }
                tag = "\"array\"," + quote(value.getClass().getComponentType().getName());
                for (int i = 0; i < Array.getLength(value); i++) {
                    children.add(canonical(Array.get(value, i), active, nodes, depth + 1));
                }
            } else {
                tag = "\"record\"," + quote(value.getClass().getName());
                for (RecordComponent component : value.getClass().getRecordComponents()) {
                    Field field = value.getClass().getDeclaredField(component.getName());
                    if (!field.trySetAccessible()) {
                        return opaque(value, "record-access-unavailable");
                    }
                    // Reading a declared record field never executes a custom accessor body.
                    Canonical child = canonical(field.get(value), active, nodes, depth + 1);
                    children.add(new Canonical("[" + quote(component.getName()) + "," + child.json() + "]",
                            child.stable(), child.reason()));
                }
            }
            String reason = children.stream().map(Canonical::reason).filter(java.util.Objects::nonNull).findFirst().orElse(null);
            return new Canonical("[" + tag + ",[" + String.join(",", children.stream().map(Canonical::json).toList()) + "]]",
                    children.stream().allMatch(Canonical::stable), reason);
        } catch (ReflectiveOperationException | RuntimeException failedObservation) {
            return opaque(value, "structural-access-unavailable");
        } finally {
            active.remove(value);
        }
    }

    private static boolean number(Object value) {
        Class<?> type = value.getClass();
        return type == Byte.class || type == Short.class || type == Integer.class || type == Long.class
                || type == Float.class || type == Double.class || type == BigInteger.class || type == BigDecimal.class;
    }

    private static Canonical known(String json) {
        return new Canonical(json, true, null);
    }

    private static Canonical opaque(Object value, String reason) {
        return new Canonical("[\"opaque\"," + quote(type(value)) + "]", false, reason);
    }

    private static String type(Object value) {
        if (value == null) {
            return "null";
        }
        Class<?> type = value.getClass();
        // Hidden lambda and generated proxy names contain allocation ordinals or VM addresses.
        // Their actual value is already unstable; declared_type binds the reviewed source slot.
        return type.isHidden() || type.isSynthetic() || Proxy.isProxyClass(type)
                ? "opaque-generated" : type.getName();
    }

    static String hash(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }

    private static String quote(String text) {
        StringBuilder json = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char character = text.charAt(i);
            switch (character) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (character < 0x20) {
                        json.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) character));
                    } else {
                        json.append(character);
                    }
                }
            }
        }
        return json.append('"').toString();
    }
}
