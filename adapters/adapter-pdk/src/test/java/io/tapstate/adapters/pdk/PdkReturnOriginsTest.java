package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.ProtectionDomain;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkReturnOriginsTest {
    @TempDir Path temporary;
    private static final String NATIVE = "{\"unchangedNativeControl\":true}";

    @Test void exact_named_resources_have_actual_owners_and_hashes_without_changing_the_native_part() throws Exception {
        var budget = budget(); String[] packet = PdkReturnOrigins.packet(NATIVE, budget);
        assertThat(packet).hasSize(34); assertThat(packet[0]).isSameAs(NATIVE);
        Map<String, Object> header = parse(packet[1]);
        assertThat(header).containsEntry("state", "RECORDED_UNQUALIFIED").containsEntry("completedClassCount", 32L)
                .containsEntry("pid", ProcessHandle.current().pid()).containsEntry("resolution", PdkReturnOrigins.RESOLUTION);
        assertThat(header.get("beforeRuntime")).isEqualTo(header.get("afterRuntime"));
        long sum = 0;
        for (int i = 0; i < 32; i++) {
            Map<String, Object> row = parse(packet[i + 2]); String name = PdkReturnOrigins.classNames().get(i);
            assertThat(row).containsEntry("binaryName", name).containsEntry("actualName", name).containsEntry("state", "RECORDED");
            Class<?> type = Class.forName(name, false, getClass().getClassLoader()); byte[] bytes = bytes(type);
            assertThat(row).containsEntry("bytes", (long) bytes.length).containsEntry("sha256", sha(bytes));
            assertThat(row.get("loaderClass")).isEqualTo(type.getClassLoader().getClass().getName());
            assertThat(row.get("loaderIdentityHashCode")).isEqualTo((long) System.identityHashCode(type.getClassLoader()));
            PdkReturnOrigins.FLAGS.forEach(flag -> assertThat(row).containsEntry(flag, false)); sum += bytes.length;
        }
        assertThat(header).containsEntry("classBytesConsumed", sum).containsEntry("classBytesHashed", sum);
        assertThat(((Number) header.get("serviceRequests")).longValue()).isLessThanOrEqualTo(PdkReturnOrigins.serviceRequestUpper());
        Map<String, Object> receipt = parse(budget.snapshot());
        assertThat(activity(receipt)).containsEntry("invocations", 32L).containsEntry("completed", 32L).containsEntry("failed", 0L).containsEntry("pending", 0L);
        PdkReturnOrigins.FLAGS.forEach(flag -> assertThat(header).containsEntry(flag, false));
    }

    @Test void wrong_resolved_names_refuse_before_opening_and_keep_all_unobserved_parts_explicit() {
        var opened = new AtomicInteger(); var failure = new AtomicReference<Throwable>();
        String[] packet = PdkReturnOrigins.packet(NATIVE, budget(), name -> JsonWriter.class,
                connection -> { opened.incrementAndGet(); return connection.getInputStream(); }, failure::set);
        assertThat(opened).hasValue(0); assertThat(failure.get()).isInstanceOf(IOException.class).hasMessageContaining("name differs");
        assertThat(parse(packet[1])).containsEntry("state", "UNKNOWN").containsEntry("completedClassCount", 0L);
        assertThat(parse(packet[2])).containsEntry("state", "UNKNOWN").containsEntry("actualName", JsonWriter.class.getName())
                .containsEntry("bytes", null).containsEntry("sha256", null);
        for (int i = 3; i < packet.length; i++) {
            var row = parse(packet[i]); assertThat(row).containsEntry("state", "UNOBSERVED").containsEntry("reason", "NOT_OBSERVED")
                    .containsEntry("actualName", null).containsEntry("codeSource", null).containsEntry("entry", null);
        }
    }

    @Test void read_primary_and_close_suppression_survive_without_replacing_or_poisoning_native_metadata() {
        var primary = new IOException("controlled class read"); var cleanup = new IOException("controlled class close");
        var observed = new AtomicReference<Throwable>(); var closes = new AtomicInteger(); var budget = budget();
        String[] packet = PdkReturnOrigins.packet(NATIVE, budget, PdkReturnOriginsTest::resolve,
                connection -> new InputStream() {
                    @Override public int read() throws IOException { throw primary; }
                    @Override public int read(byte[] bytes, int offset, int length) throws IOException { throw primary; }
                    @Override public void close() throws IOException { closes.incrementAndGet(); throw cleanup; }
                }, observed::set);
        assertThat(observed.get()).isSameAs(primary); assertThat(primary.getSuppressed()).containsExactly(cleanup); assertThat(closes).hasValue(1);
        assertThat(packet[0]).isSameAs(NATIVE);
        var header = parse(packet[1]); assertThat(header).containsEntry("state", "UNKNOWN").containsEntry("failureType", IOException.class.getName());
        assertThat(header.get("suppressedFailureTypes")).isEqualTo(List.of(IOException.class.getName()));
        var partial = parse(packet[2]); assertThat(partial.get("loaderClass")).isNotNull(); assertThat(partial.get("resource")).isNotNull();
        assertThat(partial).containsEntry("bytes", 0L).containsEntry("sha256", null);
        assertThat(activity(parse(budget.snapshot()))).containsEntry("failed", 1L).containsEntry("pending", 0L);
    }

    @Test void programmer_read_error_keeps_its_exact_primary_when_cleanup_also_fails() {
        var primary = new AssertionError("controlled invariant read"); var cleanup = new IOException("controlled cleanup");
        assertThatThrownBy(() -> PdkReturnOrigins.packet(NATIVE, budget(), PdkReturnOriginsTest::resolve,
                connection -> new InputStream() {
                    @Override public int read() { throw primary; }
                    @Override public int read(byte[] bytes, int offset, int length) { throw primary; }
                    @Override public void close() throws IOException { throw cleanup; }
                }, null)).isSameAs(primary);
        assertThat(primary.getSuppressed()).containsExactly(cleanup);
    }

    @Test void zero_progress_overbound_material_and_close_only_failures_cannot_complete_a_class() {
        for (int kind = 0; kind < 3; kind++) {
            int selected = kind; var observed = new AtomicReference<Throwable>(); var budget = budget();
            String[] packet = PdkReturnOrigins.packet(NATIVE, budget, PdkReturnOriginsTest::resolve,
                    connection -> selected == 0 ? new InputStream() {
                        @Override public int read() { return 0; }
                        @Override public int read(byte[] bytes, int offset, int length) { return 0; }
                    } : selected == 1 ? new ByteArrayInputStream(new byte[PdkReturnOrigins.CLASS_BYTES + 1])
                    : new ByteArrayInputStream(new byte[]{1}) {
                        @Override public void close() throws IOException { throw new IOException("controlled close only"); }
                    }, observed::set);
            assertThat(observed.get()).isInstanceOf(IOException.class);
            assertThat(parse(packet[1])).containsEntry("state", "UNKNOWN").containsEntry("completedClassCount", 0L);
            assertThat(activity(parse(budget.snapshot()))).containsEntry("failed", 1L).containsEntry("pending", 0L);
            if (selected == 1) assertThat(parse(packet[2])).containsEntry("bytes", PdkReturnOrigins.CLASS_BYTES + 1L);
            if (selected == 2) assertThat(parse(packet[2]).get("sha256")).isNotNull();
        }
    }

    @Test void changed_runtime_is_retained_after_resolution_instead_of_reusing_the_before_value() {
        String version = System.getProperty("java.version");
        try {
            String[] packet = PdkReturnOrigins.packet(NATIVE, budget(), name -> {
                System.setProperty("java.version", version + ".changed"); return resolve(name);
            }, URLConnection::getInputStream, null);
            var header = parse(packet[1]); assertThat(header).containsEntry("state", "UNKNOWN").containsEntry("completedClassCount", 32L);
            assertThat(((Map<?, ?>) header.get("beforeRuntime")).get("javaVersion")).isEqualTo(version);
            assertThat(((Map<?, ?>) header.get("afterRuntime")).get("javaVersion")).isEqualTo(version + ".changed");
        } finally { System.setProperty("java.version", version); }
    }

    @Test void long_java_home_refuses_before_resolution_but_retains_obtained_pid_and_start() {
        String home = System.getProperty("java.home"); var resolved = new AtomicInteger();
        try {
            System.setProperty("java.home", "x".repeat(513));
            String[] packet = PdkReturnOrigins.packet(NATIVE, budget(), name -> { resolved.incrementAndGet(); return resolve(name); },
                    URLConnection::getInputStream, null);
            var header = parse(packet[1]); assertThat(header).containsEntry("state", "UNKNOWN").containsEntry("javaHome", null)
                    .containsEntry("pid", ProcessHandle.current().pid());
            assertThat(((Map<?, ?>) header.get("beforeRuntime")).get("jvmStartTimeMillis")).isNotNull(); assertThat(resolved).hasValue(0);
        } finally { System.setProperty("java.home", home); }
    }

    @Test void actual_local_nested_locator_is_checked_without_closing_a_supplied_archive() throws Exception {
        Path outer = Files.write(temporary.resolve("owned-app.jar"), new byte[]{1}).toRealPath(); var opened = new AtomicInteger();
        String base = "jar:nested:" + outer.toUri().getRawPath() + "/!BOOT-INF/lib/adapter-pdk-control.jar!/";
        byte[] body = bytes(PdkBenchmarkClock.class); String member = PdkBenchmarkClock.class.getName().replace('.', '/') + ".class";
        URL source = localUrl(base, opened); URL resource = localUrl(base + member, opened);
        var loader = new ResourceLoader(body, source, resource); var closes = new AtomicInteger();
        String[] packet = PdkReturnOrigins.packet(NATIVE, budget(), name -> name.equals(PdkBenchmarkClock.class.getName()) ? loader.defined : resolve(name),
                connection -> connection.getURL().toExternalForm().startsWith(base) ? new ByteArrayInputStream(body) {
                    @Override public void close() throws IOException { closes.incrementAndGet(); super.close(); }
                } : connection.getInputStream(), null);
        var row = parse(packet[2]); assertThat(row).containsEntry("state", "RECORDED").containsEntry("container", outer.toString())
                .containsEntry("outer", outer.toString()).containsEntry("module", "BOOT-INF/lib/adapter-pdk-control.jar")
                .containsEntry("entry", member).containsEntry("sha256", sha(body));
        assertThat(opened).hasValue(1); assertThat(closes).hasValue(1); assertThat(parse(packet[1])).containsEntry("state", "RECORDED_UNQUALIFIED");
    }

    @Test void nonlocal_or_shadow_resources_are_refused_before_connection_or_stream_open() throws Exception {
        byte[] body = bytes(PdkBenchmarkClock.class); URL source = PdkBenchmarkClock.class.getProtectionDomain().getCodeSource().getLocation();
        for (String text : List.of("https://example.invalid/foreign.class", "jar:https://example.invalid/foreign.jar!/foreign.class")) {
            var opened = new AtomicInteger(); var streams = new AtomicInteger(); var loader = new ResourceLoader(body, source, localUrl(text, opened));
            String[] packet = PdkReturnOrigins.packet(NATIVE, budget(), name -> loader.defined,
                    connection -> { streams.incrementAndGet(); return connection.getInputStream(); }, null);
            assertThat(parse(packet[1])).containsEntry("state", "UNKNOWN"); assertThat(opened).hasValue(0); assertThat(streams).hasValue(0);
        }
        Path shadow = Files.write(temporary.resolve("shadow.class"), body).toRealPath(); var opened = new AtomicInteger();
        var loader = new ResourceLoader(body, source, shadow.toUri().toURL());
        String[] packet = PdkReturnOrigins.packet(NATIVE, budget(), name -> loader.defined,
                connection -> { opened.incrementAndGet(); return connection.getInputStream(); }, null);
        assertThat(parse(packet[1])).containsEntry("state", "UNKNOWN"); assertThat(opened).hasValue(0);
    }

    @Test void final_encoding_refusal_keeps_the_original_primary_and_the_actual_guard_failure() {
        Map<String, Object> invalid = Map.of("actualRetainedText", String.valueOf((char) 0));
        var original = new IOException("controlled earlier read refusal");
        assertThatThrownBy(() -> PdkReturnOrigins.encodeRetainedPart(invalid, false, original))
                .isInstanceOf(IllegalStateException.class).satisfies(failure -> assertThat(failure.getCause()).isSameAs(original));
        assertThat(original.getSuppressed()).hasSize(1);
        assertThat(original.getSuppressed()[0]).isInstanceOf(IOException.class).hasMessageContaining("printable ASCII");
        var invariant = new AssertionError("controlled earlier invariant");
        assertThatThrownBy(() -> PdkReturnOrigins.encodeRetainedPart(invalid, false, invariant)).isSameAs(invariant);
        assertThat(invariant.getSuppressed()).hasSize(1);
        assertThatThrownBy(() -> PdkReturnOrigins.encodeRetainedPart(invalid, false, null))
                .isInstanceOf(IllegalStateException.class).hasCauseInstanceOf(IOException.class);
    }

    @Test void source_plans_are_prior_caps_and_eight_unit_allowances_are_checked_and_not_caller_mutable() {
        assertThat(PdkReturnOrigins.classNames()).hasSize(32).doesNotHaveDuplicates();
        assertThat(PdkReturnOrigins.classNames()).contains(PdkReturnOrigins.class.getName(), PdkReturnOrigins.Resolver.class.getName(),
                PdkReturnOrigins.Streams.class.getName(), "io.tapstate.core.common.JsonReader$Parser");
        assertThatThrownBy(PdkReturnOrigins.classNames()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(PdkReturnOrigins.headerEncodingUpper()).isLessThanOrEqualTo(8192);
        assertThat(PdkReturnOrigins.originEncodingUpper()).isLessThanOrEqualTo(64512);
        assertThat(PdkReturnOrigins.MAX_PACKET_BYTES).isEqualTo(2080768).isLessThan(2 * 1024 * 1024);
        long[] first = PdkReturnOrigins.perClassUpper(); assertThat(first).hasSize(8);
        assertThat(first[1]).isGreaterThan(2L * PdkReturnOrigins.CLASS_BYTES);
        assertThatThrownBy(() -> Math.multiplyExact(first[1], Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
        first[1] = 0; assertThat(PdkReturnOrigins.perClassUpper()[1]).isPositive();
        assertThatThrownBy(() -> PdkReturnOrigins.packet("x".repeat(8193), budget(), name -> {
            throw new AssertionError("resolver must not run");
        }, connection -> { throw new AssertionError("stream must not open"); }, null)).isInstanceOf(AssertionError.class);
    }

    private static Class<?> resolve(String name) throws ClassNotFoundException { return Class.forName(name, false, PdkReturnOriginsTest.class.getClassLoader()); }
    private static byte[] bytes(Class<?> type) throws IOException {
        try (var input = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
            if (input == null) throw new AssertionError("owned compiled class resource is missing");
            byte[] result = input.readNBytes(PdkReturnOrigins.CLASS_BYTES + 1);
            assertThat(result.length).isLessThanOrEqualTo(PdkReturnOrigins.CLASS_BYTES); return result;
        }
    }
    private static String sha(byte[] bytes) throws Exception { return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    private static URL localUrl(String external, AtomicInteger opened) throws Exception {
        return new URL(null, external, new URLStreamHandler() {
            @Override protected URLConnection openConnection(URL url) {
                opened.incrementAndGet(); return new URLConnection(url) { @Override public void connect() { } };
            }
        });
    }
    private static final class ResourceLoader extends ClassLoader {
        final Class<?> defined; private final URL resource;
        ResourceLoader(byte[] bytes, URL source, URL resource) {
            super(PdkReturnOriginsTest.class.getClassLoader()); this.resource = resource;
            defined = defineClass(PdkBenchmarkClock.class.getName(), bytes, 0, bytes.length,
                    new ProtectionDomain(new CodeSource(source, (java.security.cert.Certificate[]) null), null, this, null));
        }
        @Override public URL getResource(String name) { return resource; }
    }
    private static PdkWriteReturnWorkBudget budget() {
        var limits = new LinkedHashMap<String, Object>(); long[] cold = PdkReturnOrigins.perClassUpper();
        for (int i = 0; i < 8; i++) {
            long value = Math.addExact(Math.multiplyExact(cold[i], 32), PdkWriteReturnWorkBudget.Activity.CONFIG.upper[i]);
            value = Math.addExact(value, Math.multiplyExact(PdkWriteReturnWorkBudget.Activity.BOOKKEEPING.upper[i], PdkReturnOrigins.serviceRequestUpper() * 4));
            value = Math.addExact(value, PdkWriteReturnWorkBudget.Activity.MBEAN_BUDGET.upper[i]); limits.put(PdkWriteReturnWorkBudget.UNITS.get(i), value);
        }
        var material = Map.of("connectorHashBytes", 134217728L, "runtimeArguments", 128L, "runtimeArgumentBytes", 2093056L, "primaryKeys", 2L);
        var root = new LinkedHashMap<String, Object>(); root.put("schemaVersion", 1); root.put("definitionId", PdkWriteReturnWorkBudget.DEFINITION_ID);
        root.put("sourceFormulaSha256", PdkWriteReturnWorkBudget.SOURCE_FORMULA_SHA256); root.put("inputSha256", "a".repeat(64)); root.put("runId", "cold-origin-control");
        root.put("limits", limits); root.put("materialLimits", material);
        return PdkWriteReturnWorkBudget.testing(Base64.getEncoder().encodeToString(JsonWriter.write(root).getBytes(StandardCharsets.UTF_8)),
                ProcessHandle.current().pid(), ManagementFactory.getRuntimeMXBean().getStartTime());
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> parse(String text) { return (Map<String, Object>) JsonReader.parse(text); }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> activity(Map<String, Object> receipt) {
        return (Map<String, Object>) ((Map<?, ?>) receipt.get("activities")).get("COLD_ORIGINS");
    }
}
