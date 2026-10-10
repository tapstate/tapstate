package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeClockEvidenceTest {
    static final String LIBRARY = "/private/tmp/native-clock-control.dylib";
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1_000);

    @Test void matching_native_routes_are_immutable_facts_with_every_qualification_false() {
        var owned = parse(metadata(OWNER)); var root = parse(metadata(OWNER));
        BenchmarkNativeClockEvidence.requireMatchingNative(owned, root);
        assertThat(owned).containsEntry("state", "NATIVE_RECORDED").containsEntry("mixedDomainPossible", false);
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(owned.get(flag)).isEqualTo(false));
        assertThatThrownBy(() -> owned.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((Map<?, ?>) ((Map<?, ?>) owned.get("before")).get("classHashes")).clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void unknown_and_system_metadata_are_retained_refusal_facts_without_native_promotion() {
        var unknown = metadataMap(OWNER); unknown.put("state", "UNKNOWN"); unknown.put("reason", "NATIVE_READ_UNAVAILABLE");
        unknown.put("failureType", "java.lang.IllegalStateException"); unknown.put("activeProvider", "SYSTEM_NANO_TIME");
        unknown.put("mixedDomainPossible", true);
        var recorded = parse(JsonWriter.write(unknown)); assertThat(recorded).containsEntry("state", "UNKNOWN");
        assertThatThrownBy(() -> BenchmarkNativeClockEvidence.requireMatchingNative(recorded, parse(metadata(OWNER))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("fallback");
        unknown.put("before", Map.of()); unknown.put("after", Map.of()); unknown.put("reason", "NATIVE_SETUP_UNAVAILABLE");
        assertThat(parse(JsonWriter.write(unknown))).containsEntry("state", "UNKNOWN");
        var system = metadataMap(OWNER); system.put("state", "SYSTEM_UNQUALIFIED"); system.put("configured", false);
        system.put("provider", "SYSTEM_NANO_TIME"); system.put("activeProvider", "SYSTEM_NANO_TIME");
        Map<String, Object> classes = snapshot();
        var minimal = Map.of("classSha256", classes.get("classSha256"), "classHashes", classes.get("classHashes"));
        system.put("before", minimal); system.put("after", minimal);
        assertThatThrownBy(() -> BenchmarkNativeClockEvidence.requireMatchingNative(parse(JsonWriter.write(system)), parse(metadata(OWNER))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("fallback");
    }

    @Test void bounded_canonical_schema_and_owned_identity_refuse_malformed_or_duplicate_facts() {
        for (String raw : List.of("{}", metadata(OWNER) + "x", "x".repeat(8193),
                "{\"pid\":17," + metadata(OWNER).substring(1))) {
            assertThatThrownBy(() -> parse(raw)).isInstanceOf(AssertionError.class);
        }
        for (String field : List.of("pid", "jvmStartTimeMillis", "schemaVersion")) {
            var map = metadataMap(OWNER); map.put(field, (Long) map.get(field) + 1);
            assertThatThrownBy(() -> parse(JsonWriter.write(map))).isInstanceOf(AssertionError.class);
        }
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
            var map = metadataMap(OWNER); map.put(flag, true);
            assertThatThrownBy(() -> parse(JsonWriter.write(map))).isInstanceOf(AssertionError.class);
        }
    }

    @Test void changed_boot_offset_or_code_is_refused_within_process_and_across_processes() {
        Map<String, Object> changes = Map.of("bootUuid", "11111111-1111-1111-1111-111111111111",
                "timebaseOffsetUnsigned", "18", "osFunctionImageOffsetUnsigned", "4097",
                "osFunctionCodePrefixSha256", "a".repeat(64), "loadedJniSha256", "b".repeat(64),
                "timebaseNumer", 126L, "osImageUuid", "b".repeat(32), "userTimebaseSelector", 2L);
        for (var change : changes.entrySet()) {
            var map = metadataMap(OWNER); var changed = snapshot(); changed.put(change.getKey(), change.getValue());
            map.put("after", changed);
            assertThatThrownBy(() -> parse(JsonWriter.write(map))).isInstanceOf(AssertionError.class).hasMessageContaining("changed");
            map.put("before", changed); var other = parse(JsonWriter.write(map));
            assertThatThrownBy(() -> BenchmarkNativeClockEvidence.requireMatchingNative(parse(metadata(OWNER)), other))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("identities differ");
        }
        var map = metadataMap(OWNER); var changed = snapshot();
        changed.put("classHashes", Map.of("PdkBenchmarkClock", "a".repeat(64), "PdkBenchmarkClock$Clock", "f".repeat(64),
                "PdkBenchmarkClock$JniAccess", "c".repeat(64), "PdkBenchmarkClock$NativeAccess", "d".repeat(64)));
        map.put("before", changed); map.put("after", changed);
        assertThatThrownBy(() -> BenchmarkNativeClockEvidence.requireMatchingNative(parse(metadata(OWNER)), parse(JsonWriter.write(map))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("identities differ");
    }

    @Test void scale_prefix_class_roster_and_unsigned_offsets_are_strict() {
        for (var change : List.of(Map.entry("timebaseNumer", (Object) 0L), Map.entry("timebaseDenom", (Object) 1.0),
                Map.entry("timebaseNumer", (Object) 4_294_967_296L), Map.entry("userTimebaseSelector", (Object) 256L),
                Map.entry("osFunctionCodePrefixBytes", (Object) 143L), Map.entry("timebaseOffsetUnsigned", (Object) "18446744073709551616"),
                Map.entry("timebaseOffsetUnsigned", (Object) "+0"), Map.entry("timebaseOffsetUnsigned", (Object) "00"),
                Map.entry("classHashes", (Object) Map.of("PdkBenchmarkClock", "a".repeat(64))))) {
            var map = metadataMap(OWNER); var changed = snapshot(); changed.put(change.getKey(), change.getValue());
            map.put("before", changed); map.put("after", changed);
            assertThatThrownBy(() -> parse(JsonWriter.write(map))).isInstanceOf(AssertionError.class);
        }
    }

    static BenchmarkCausalClock.Identity rootOwner() {
        return new BenchmarkCausalClock.Identity(ProcessHandle.current().pid(), java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime());
    }
    static String metadata(BenchmarkCausalClock.Identity owner) { return JsonWriter.write(metadataMap(owner)); }
    private static Map<String, Object> parse(String raw) { return BenchmarkNativeClockEvidence.parse(raw, OWNER, LIBRARY); }
    static Map<String, Object> metadataMap(BenchmarkCausalClock.Identity owner) {
        var map = new LinkedHashMap<String, Object>();
        map.put("schemaVersion", 1L); map.put("state", "NATIVE_RECORDED"); map.put("reason", "NONE"); map.put("failureType", "NONE");
        map.put("configured", true); map.put("pid", owner.pid()); map.put("jvmStartTimeMillis", owner.jvmStartTimeMillis());
        map.put("provider", "DIRECT_MACH_ABSOLUTE_TIME_JNI"); map.put("activeProvider", "DIRECT_MACH_ABSOLUTE_TIME_JNI");
        map.put("counterUnit", "nominal-ns"); map.put("mixedDomainPossible", false); map.put("before", snapshot()); map.put("after", snapshot());
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> map.put(flag, false)); return map;
    }
    private static Map<String, Object> snapshot() {
        var map = new LinkedHashMap<String, Object>();
        map.put("bootUuid", "00000000-0000-0000-0000-000000000000"); map.put("timebaseNumer", 125L); map.put("timebaseDenom", 3L);
        map.put("conversion", "UNSIGNED_128_MULTIPLY_INTEGER_DIVIDE_TRUNCATE"); map.put("loadedJniPath", LIBRARY);
        map.put("loadedJniSha256", "e".repeat(64)); map.put("classSha256", "a".repeat(64));
        map.put("classHashes", Map.of("PdkBenchmarkClock", "a".repeat(64), "PdkBenchmarkClock$Clock", "b".repeat(64),
                "PdkBenchmarkClock$JniAccess", "c".repeat(64), "PdkBenchmarkClock$NativeAccess", "d".repeat(64)));
        map.put("osFunction", "mach_absolute_time"); map.put("osImagePath", "/usr/lib/system/libsystem_kernel.dylib");
        map.put("osImageUuid", "a".repeat(32)); map.put("osFunctionImageOffsetUnsigned", "4096");
        map.put("osFunctionCodePrefixBytes", 144L); map.put("osFunctionCodePrefixSha256", "f".repeat(64));
        map.put("userTimebaseSelector", 1L); map.put("timebaseOffsetUnsigned", "17"); return map;
    }
}
