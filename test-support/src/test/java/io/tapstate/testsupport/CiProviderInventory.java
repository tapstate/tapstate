package io.tapstate.testsupport;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.params.provider.Arguments;

/** Enumerates a bounded, source-reviewed WHAT before a JUnit or shard selector runs. */
public final class CiProviderInventory {
    private static final int MAX_CASES = 4096;
    private static final Map<String, List<String>> PROVIDERS = Map.ofEntries(
            Map.entry("io.tapstate.app.ServerTimeZoneTest", List.of("scenarios")),
            Map.entry("io.tapstate.core.dsl.CanonicalRoundTripTest", List.of("canonicalResources")),
            Map.entry("io.tapstate.core.dsl.CorpusSemanticGateTest", List.of("validScenarios", "invalidCases")),
            Map.entry("io.tapstate.core.dsl.DslPipelineParserTest", List.of("allValidDocs")),
            Map.entry("io.tapstate.core.dsl.DslReferenceClosureTest", List.of("validScenarios", "closureViolations")),
            Map.entry("io.tapstate.core.dsl.TargetConnectorRulesTest", List.of("sinkConnectors")),
            Map.entry("io.tapstate.adapters.pdk.PdkTypeMappingTest", List.of("declaredColumnShapes", "driverNativeColumnShapes")),
            Map.entry("io.tapstate.control.restapi.WrittenFilterTest", List.of("theVocabularyDownBothRoads")),
            Map.entry("io.tapstate.cli.PublishedConnectorArtifactsTest", List.of("publishedIds")),
            Map.entry("io.tapstate.e2e.JoinConformanceIT", List.of("carriers")),
            Map.entry("io.tapstate.core.event.HowManyBytesARowIsTest", List.of("eachRuleChargesWhatTheDefinitionSays")));

    private CiProviderInventory() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("requests TSV and output JSONL paths are required");
        }
        List<String> requests = Files.readAllLines(Path.of(arguments[0]), StandardCharsets.UTF_8);
        // Validate the entire whitelist before loading any requested test class.
        for (String request : requests) {
            String[] fields = request.split("\t", -1);
            if (fields.length != 4 || !PROVIDERS.getOrDefault(fields[0], List.of()).contains(fields[2])
                    || !(fields[3].equals("source") || fields[3].equals("dynamic"))) {
                throw new IllegalArgumentException("unreviewed provider request");
            }
        }
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Path.of(arguments[1]), StandardCharsets.UTF_8))) {
            for (String request : requests) {
                String[] fields = request.split("\t", -1);
                Class<?> type = Class.forName(fields[0], false, CiProviderInventory.class.getClassLoader());
                Method provider = type.getDeclaredMethod(fields[2]);
                if (provider.getParameterCount() != 0 || !provider.trySetAccessible()) {
                    throw new IllegalArgumentException("provider is not accessible without parameters");
                }
                Object instance = null;
                if (!Modifier.isStatic(provider.getModifiers())) {
                    if (!fields[3].equals("dynamic")
                            || !fields[0].equals("io.tapstate.core.event.HowManyBytesARowIsTest")) {
                        throw new IllegalArgumentException("unreviewed instance provider");
                    }
                    var constructor = type.getDeclaredConstructor();
                    if (!constructor.trySetAccessible()) {
                        throw new IllegalArgumentException("factory constructor is inaccessible");
                    }
                    instance = constructor.newInstance();
                }
                Object values = provider.invoke(instance);
                List<String> rows;
                try (Stream<?> stream = asStream(values)) {
                    rows = enumerate(stream, fields[3].equals("dynamic"));
                }
                byte[] classBytes;
                try (var input = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
                    if (input == null) throw new IOException("provider class bytes unavailable");
                    classBytes = input.readAllBytes();
                }
                writer.println("{\"schema\":1,\"class\":" + quote(fields[0])
                        + ",\"test_method\":" + quote(fields[1]) + ",\"provider\":" + quote(fields[2])
                        + ",\"kind\":" + quote(fields[3]) + ",\"provider_class_sha256\":"
                        + quote(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(classBytes)))
                        + ",\"cases\":[" + String.join(",", rows) + "]}");
            }
        }
    }

    private static Stream<?> asStream(Object values) {
        if (values instanceof Stream<?> stream) return stream;
        if (values instanceof List<?> list) return list.stream();
        if (values instanceof Object[] array) return Arrays.stream(array);
        throw new IllegalArgumentException("unsupported provider result type");
    }

    static List<String> enumerate(Stream<?> values, boolean dynamic) {
        List<String> rows = new ArrayList<>();
        Iterator<?> iterator = values.iterator();
        while (iterator.hasNext()) {
            if (rows.size() == MAX_CASES) throw new IllegalArgumentException("provider inventory limit exceeded");
            Object value = iterator.next();
            if (dynamic) {
                // Reading a name must never execute a DynamicTest's assertion.
                if (!(value instanceof DynamicTest test)) throw new IllegalArgumentException("unsupported dynamic node");
                rows.add("{\"stable\":true,\"display_name_digest\":"
                        + quote(CiCaseEvidence.hash(quote(test.getDisplayName()))) + "}");
                continue;
            }
            Object[] arguments = value instanceof Arguments supplied ? supplied.get() : new Object[] {value};
            List<String> canonical = new ArrayList<>();
            List<String> descriptors = new ArrayList<>();
            boolean stable = true;
            String firstString = null;
            for (int position = 0; position < arguments.length; position++) {
                var encoded = CiCaseEvidence.canonical(arguments[position]);
                canonical.add(encoded.json());
                stable &= encoded.stable();
                descriptors.add("{\"position\":" + position + ",\"stable\":" + encoded.stable()
                        + ",\"digest\":" + quote(CiCaseEvidence.hash(encoded.json())) + "}");
                if (position == 0 && arguments[position] instanceof String && encoded.stable()) {
                    firstString = CiCaseEvidence.hash(encoded.json());
                }
            }
            rows.add("{\"stable\":" + stable + ",\"argument_digest\":"
                    + quote(CiCaseEvidence.hash("[\"arguments\",[" + String.join(",", canonical) + "]]"))
                    + ",\"first_string_digest\":" + (firstString == null ? "null" : quote(firstString))
                    + ",\"argument_descriptors\":[" + String.join(",", descriptors) + "]}");
        }
        if (rows.isEmpty()) throw new IllegalArgumentException("provider inventory is empty");
        return rows;
    }

    private static String quote(String value) {
        return CiCaseEvidence.canonical(value).json().substring("[\"java.lang.String\",".length(),
                CiCaseEvidence.canonical(value).json().length() - 1);
    }
}
