package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class BenchmarkReturnProducerOriginsTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1000);
    private static final String APP = "/controlled/application.jar", NATIVE = "{\"native\":\"left-for-the-original-parser\"}";

    @Test void complete_boot_packet_preserves_native_part_and_actual_identity_with_all32_ordered_origins() {
        String[] packet = packet(); var parsed = parse(packet);
        assertThat(parsed.nativeRaw()).isEqualTo(NATIVE);
        assertThat(parsed.header().before().pid()).isEqualTo(OWNER.pid());
        assertThat(parsed.header().after()).isEqualTo(parsed.header().before());
        assertThat(parsed.typedParts()).extracting(BenchmarkReturnProducerOrigins.RecordPart::binaryName)
                .containsExactlyElementsOf(BenchmarkReturnProducerOrigins.NAMES);
        assertThat(parsed.typedParts()).allSatisfy(part -> {
            assertThat(part.kind()).isEqualTo(BenchmarkReturnProducerOrigins.Kind.BOOT_NESTED);
            assertThat(part.container()).isEqualTo(APP); assertThat(part.outer()).isEqualTo(APP);
            assertThat(part.loaderIdentityHashCode()).isEqualTo(-2147483648L);
        });
        assertThat(parsed.evidence().get("rawParts")).isEqualTo(List.of(packet));
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(parsed.evidence()).containsEntry(flag, false);
        packet[0] = "changed caller array"; assertThat(parsed.nativeRaw()).isEqualTo(NATIVE);
        assertThatThrownBy(parsed.typedParts()::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void fixed_roster_order_duplicate_name_and_part_count_refuse_without_native_fallback() {
        String[] wrongOrder = packet(); String first = wrongOrder[2]; wrongOrder[2] = wrongOrder[3]; wrongOrder[3] = first;
        refusal(wrongOrder, "ordered static origin roster differs");
        String[] repeated = packet(); repeated[3] = repeated[2]; refusal(repeated, "ordered static origin roster differs");
        String[] shortPacket = java.util.Arrays.copyOf(packet(), 33);
        assertThatThrownBy(() -> parse(shortPacket)).isInstanceOf(BenchmarkReturnProducerOrigins.Refusal.class);
        assertThatThrownBy(() -> BenchmarkReturnProducerOrigins.parse(List.of(packet()), OWNER, APP))
                .isInstanceOf(BenchmarkReturnProducerOrigins.Refusal.class);
    }

    @Test void recorded_header_requires_actual_owned_before_after_count_and_full_byte_totals() {
        String[] pid = packet(); var header = object(pid[1]);
        header.put("pid", 18L); var before = objectValue(header.get("beforeRuntime")); before.put("pid", 18L);
        header.put("beforeRuntime", before); header.put("afterRuntime", before); pid[1] = JsonWriter.write(header);
        refusal(pid, "another runtime or failure");
        String[] count = packet(); var incomplete = object(count[1]); incomplete.put("completedClassCount", 31L); count[1] = JsonWriter.write(incomplete);
        refusal(count, "completed count differs");
        String[] totals = packet(); var inconsistent = object(totals[1]); inconsistent.put("classBytesHashed", 543L); totals[1] = JsonWriter.write(inconsistent);
        refusal(totals, "inconsistent byte totals");
        String[] expected = packet(); var extra = object(expected[1]); extra.put("expectedClassCount", 33L); expected[1] = JsonWriter.write(extra);
        refusal(expected, "expected class count differs");
        String[] services = packet(); var exceeded = object(services[1]); exceeded.put("serviceRequests", BenchmarkReturnProducerOrigins.SERVICE_REQUEST_UPPER + 1);
        services[1] = JsonWriter.write(exceeded); refusal(services, "service requests exceed fixed source bound");
    }

    @Test void qualification_and_missing_extra_duplicate_json_fields_never_authenticate_the_packet() {
        for (int index : List.of(1, 2)) {
            String[] claimed = packet(); var map = object(claimed[index]); map.put("wholeMethodCostQualified", true); claimed[index] = JsonWriter.write(map);
            refusal(claimed, "claims a qualification");
        }
        String[] unknownKey = packet(); var map = object(unknownKey[2]); map.put("callerVerified", true); unknownKey[2] = JsonWriter.write(map);
        refusal(unknownKey, "fields differ");
        String[] duplicate = packet(); duplicate[2] = duplicate[2].replaceFirst("\\{", "{\"state\":\"RECORDED\",");
        refusal(duplicate, "noncanonical or has duplicate fields");
    }

    @Test void recorded_route_binds_outer_module_entry_locality_and_positive_exact_class_bytes() {
        for (String field : List.of("outer", "module", "entry", "resource", "bytes")) {
            String[] packet = packet(); var row = object(packet[2]);
            row.put(field, switch (field) {
                case "outer" -> "/foreign/application.jar";
                case "module" -> "BOOT-INF/lib/foreign-0.6.0.jar";
                case "entry" -> "../foreign.class";
                case "resource" -> "https://example.invalid/foreign.class";
                default -> 0L;
            }); packet[2] = JsonWriter.write(row);
            assertThatThrownBy(() -> parse(packet)).isInstanceOf(BenchmarkReturnProducerOrigins.Refusal.class);
        }
        String[] zero = packet(); var row = object(zero[2]); row.put("bytes", 0L); zero[2] = JsonWriter.write(row);
        refusal(zero, "incomplete or failed");
    }

    @Test void actual_directory_and_local_jar_layouts_are_retained_as_distinct_fact_kinds() {
        for (boolean directory : List.of(true, false)) {
            String[] packet = packet();
            for (int i = 2; i < packet.length; i++) {
                var row = object(packet[i]); String member = (String) row.get("entry"); row.put("module", null);
                if (directory) {
                    row.put("container", "/controlled/classes"); row.put("outer", null);
                    row.put("codeSource", "file:/controlled/classes/"); row.put("resource", "file:/controlled/classes/" + member);
                } else {
                    row.put("codeSource", "file:" + APP); row.put("resource", "jar:file:" + APP + "!/" + member);
                }
                packet[i] = JsonWriter.write(row);
            }
            assertThat(parse(packet).typedParts()).allSatisfy(row -> assertThat(row.kind()).isEqualTo(directory
                    ? BenchmarkReturnProducerOrigins.Kind.DIRECTORY : BenchmarkReturnProducerOrigins.Kind.JAR));
        }
    }

    @Test void unknown_header_retains_partial_actual_fields_and_all_unobserved_tail_parts() {
        String[] packet = packet(); var header = object(packet[1]); header.put("state", "UNKNOWN"); header.put("completedClassCount", 0L);
        header.put("classBytesConsumed", 3L); header.put("classBytesHashed", 2L); header.put("reason", "ORIGIN_READ_REFUSED");
        header.put("failureType", "java.io.IOException"); packet[1] = JsonWriter.write(header);
        for (int i = 2; i < packet.length; i++) {
            var row = object(packet[i]); row.put("state", "UNOBSERVED");
            for (String field : List.of("actualName", "loaderClass", "loaderIdentityHashCode", "codeSource", "resource", "container", "outer", "module", "entry", "sha256", "bytes")) row.put(field, null);
            row.put("reason", "NOT_OBSERVED"); row.put("failureType", "NONE"); packet[i] = JsonWriter.write(row);
        }
        var failed = object(packet[2]); failed.put("state", "UNKNOWN"); failed.put("actualName", BenchmarkReturnProducerOrigins.NAMES.getFirst());
        failed.put("bytes", 0L); failed.put("reason", "ORIGIN_READ_REFUSED"); failed.put("failureType", "java.io.IOException"); packet[2] = JsonWriter.write(failed);
        var parsed = parse(packet); assertThat(parsed.evidence()).containsEntry("state", "UNKNOWN");
        assertThat(parsed.typedParts().getFirst().actualName()).isEqualTo(BenchmarkReturnProducerOrigins.NAMES.getFirst());
        assertThat(parsed.typedParts().getFirst().bytes()).isZero();
        assertThat(parsed.typedParts().subList(1, 32)).allSatisfy(part -> assertThat(part.state()).isEqualTo("UNOBSERVED"));
        assertThat(parsed.nativeRaw()).isEqualTo(NATIVE);
        var partialRuntime = new LinkedHashMap<String, Object>(); partialRuntime.put("pid", OWNER.pid()); partialRuntime.put("jvmStartTimeMillis", OWNER.jvmStartTimeMillis());
        partialRuntime.put("javaHome", null); partialRuntime.put("javaVersion", null);
        header.put("beforeRuntime", partialRuntime); header.put("afterRuntime", Map.of()); header.put("javaHome", null); header.put("javaVersion", null);
        packet[1] = JsonWriter.write(header);
        var partial = parse(packet);
        assertThat(partial.header().before().pid()).isEqualTo(OWNER.pid()); assertThat(partial.header().before().javaHome()).isNull();
        assertThat(partial.header().after().pid()).isNull();
        header.put("reason", "NONE"); header.put("failureType", "NONE"); packet[1] = JsonWriter.write(header);
        refusal(packet, "unknown header has no refusal cause");
    }

    @Test void oversized_non_ascii_deep_and_scalar_overflow_inputs_refuse_before_unbounded_decoding() {
        String[] large = packet(); large[2] = "x".repeat(64513); refusal(large, "exceeds prior bound");
        String[] nativeLarge = packet(); nativeLarge[0] = "x".repeat(8193);
        assertThatThrownBy(() -> parse(nativeLarge)).isInstanceOf(BenchmarkReturnProducerOrigins.Refusal.class);
        String[] ascii = packet(); ascii[2] += "\n"; refusal(ascii, "not printable ASCII");
        String[] deep = packet(); deep[2] = "[[[[[0]]]]]"; refusal(deep, "prior shape bound");
        String[] scalar = packet(); scalar[1] = scalar[1].replace("\"serviceRequests\":1", "\"serviceRequests\":999999999999999999999");
        refusal(scalar, "producer numeric text bound");
    }

    @Test void suppressed_roster_overflow_preserves_the_first_eight_obtained_types_as_unknown_facts() {
        String[] packet = packet(); var header = object(packet[1]);
        List<String> types = java.util.stream.IntStream.range(0, 8).mapToObj(i -> "controlled.CleanupFailure" + i).toList();
        header.put("state", "UNKNOWN"); header.put("reason", "SUPPRESSED_ROSTER_OVERFLOW");
        header.put("failureType", "java.io.IOException"); header.put("suppressedFailureTypes", types); packet[1] = JsonWriter.write(header);
        var parsed = parse(packet);
        assertThat(parsed.evidence()).containsEntry("state", "UNKNOWN");
        assertThat(parsed.header().raw().get("suppressedFailureTypes")).isEqualTo(types);
        assertThat(parsed.header().raw()).containsEntry("reason", "SUPPRESSED_ROSTER_OVERFLOW");
        assertThat(parsed.nativeRaw()).isEqualTo(NATIVE);
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(parsed.evidence()).containsEntry(flag, false);
    }

    @Test void failed_middle_part_retains_plain_original_parts_parsed_prefix_and_original_scope_cause() {
        String[] packet = packet(); var map = object(packet[7]); map.put("bytes", 0L); packet[7] = JsonWriter.write(map);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-producer-parser")) {
            var error = (BenchmarkReturnProducerOrigins.Refusal) catchThrowable(() -> parse(packet));
            assertThat(error.nativeRaw()).isEqualTo(NATIVE); assertThat(error.retainedEvidence()).containsEntry("state", "UNKNOWN");
            assertThat((List<?>) error.retainedEvidence().get("rawParts")).hasSize(34);
            assertThat((List<?>) error.retainedEvidence().get("originParts")).hasSize(5);
            assertThat(scope.primaryCause()).isSameAs(error.getCause());
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
            assertThat(actual(scope.snapshot(), "CLOCK_REQUEST", "requests")).isZero();
        }
        var work = BenchmarkReturnProducerOrigins.localUpper();
        assertThat(work.requests()).isGreaterThan(work.elements()); assertThat(work.bytes()).isGreaterThanOrEqualTo(2080768);
        assertThatThrownBy(() -> work.times(Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
    }

    private static String[] packet() {
        var runtime = Map.of("pid", OWNER.pid(), "jvmStartTimeMillis", OWNER.jvmStartTimeMillis(), "javaHome", "/controlled/java", "javaVersion", "21.0.11");
        var header = new LinkedHashMap<String, Object>(); header.put("schemaVersion", 1L); header.put("state", "RECORDED_UNQUALIFIED");
        header.putAll(runtime); header.put("beforeRuntime", runtime); header.put("afterRuntime", runtime); header.put("expectedClassCount", 32L);
        header.put("completedClassCount", 32L); header.put("serviceRequests", 1L); header.put("classBytesConsumed", 544L); header.put("classBytesHashed", 544L);
        header.put("reason", "NONE"); header.put("failureType", "NONE"); header.put("suppressedFailureTypes", List.of());
        header.put("resolution", "EXACT_NAMES_INITIALIZE_FALSE"); header.put("byteScope", "OWN_RESOURCE_CODE_SOURCE_AND_CONTAINER_NOT_VM_INSTRUCTION_BODY"); flags(header);
        var packet = new String[34]; packet[0] = NATIVE; packet[1] = JsonWriter.write(header);
        for (int i = 0; i < 32; i++) {
            String name = BenchmarkReturnProducerOrigins.NAMES.get(i), module = "BOOT-INF/lib/" + (name.startsWith("io.tapstate.core.common.") ? "core-common" : "adapter-pdk") + "-0.6.0.jar";
            String member = name.replace('.', '/') + ".class", base = "jar:nested:" + APP + "/!" + module + "!/";
            var row = new LinkedHashMap<String, Object>(); row.put("binaryName", name); row.put("actualName", name); row.put("state", "RECORDED");
            row.put("loaderClass", "org.springframework.boot.loader.launch.LaunchedClassLoader"); row.put("loaderIdentityHashCode", -2147483648L);
            row.put("codeSource", base); row.put("resource", base + member); row.put("container", APP); row.put("outer", APP); row.put("module", module); row.put("entry", member);
            row.put("sha256", "a".repeat(64)); row.put("bytes", 17L); row.put("reason", "NONE"); row.put("failureType", "NONE"); flags(row);
            packet[i + 2] = JsonWriter.write(row);
        }
        return packet;
    }
    private static void flags(Map<String, Object> map) { BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> map.put(flag, false)); }
    private static BenchmarkReturnProducerOrigins.Parsed parse(String[] packet) { return BenchmarkReturnProducerOrigins.parse(packet, OWNER, APP); }
    private static void refusal(String[] packet, String reason) {
        Throwable error = catchThrowable(() -> parse(packet)); assertThat(error).isInstanceOf(BenchmarkReturnProducerOrigins.Refusal.class);
        assertThat(error.getCause()).isInstanceOf(AssertionError.class).hasMessageContaining(reason);
        if (packet[0].length() <= 8192) assertThat(((BenchmarkReturnProducerOrigins.Refusal) error).nativeRaw()).isEqualTo(packet[0]);
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> object(String raw) { return new LinkedHashMap<>((Map<String, Object>) JsonReader.parse(raw)); }
    private static Map<String, Object> objectValue(Object value) {
        var map = new LinkedHashMap<String, Object>(); ((Map<?, ?>) value).forEach((key, field) -> map.put((String) key, field)); return map;
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) upper.put(activity, new BenchmarkReturnWorkBudget.Work(
                1000000, 1000000, 1000000, 1000000, 1000000000, 1000000, 10000000, 1000000));
        return new BenchmarkReturnWorkBudget.Definition("controlled-producer-parser", "a".repeat(64), Map.of(), upper, Set.of());
    }
    private static long actual(Map<String, Object> receipt, String actor, String unit) {
        Map<?, ?> row = (Map<?, ?>) ((Map<?, ?>) receipt.get("activities")).get(actor);
        return ((Number) ((Map<?, ?>) row.get("actual")).get(unit)).longValue();
    }
}
