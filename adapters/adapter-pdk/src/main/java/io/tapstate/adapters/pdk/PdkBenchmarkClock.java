package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/** Optional diagnostic counter. Its observations never establish clock or performance qualification. */
public final class PdkBenchmarkClock {
    static final String PROPERTY = "tapstate.benchmark.native-clock-library";
    private static final LongSupplier SYSTEM = System::nanoTime;
    private static final Clock CLOCK = configuredClock();

    private PdkBenchmarkClock() { }
    public static long nanoTime() { return CLOCK.source().getAsLong(); }
    public static LongSupplier source() { return CLOCK.source(); }
    public static String metadata() { return CLOCK.metadata(); }
    static boolean configured() { return CLOCK.configured; }

    private static Clock configuredClock() {
        try { return new Clock(System.getProperty(PROPERTY), new JniAccess(), SYSTEM); }
        catch (SecurityException unavailable) {
            Clock clock = new Clock(null, new JniAccess(), SYSTEM);
            clock.fail("CONFIGURATION_UNAVAILABLE", unavailable);
            return clock;
        }
    }

    interface NativeAccess {
        void load(Path path);
        long read();
        String[] facts();
    }

    static final class Clock {
        private final boolean configured;
        private final NativeAccess nativeAccess;
        private final LongSupplier system;
        private final LongSupplier source;
        private final long pid = ProcessHandle.current().pid();
        private final long startMillis = ManagementFactory.getRuntimeMXBean().getStartTime();
        private Path library;
        private Map<String, Object> before = Map.of();
        private Map<String, Object> after = Map.of();
        private volatile String reason;
        private String failureType = "NONE";

        Clock(String configuredPath, NativeAccess nativeAccess, LongSupplier system) {
            this.configured = configuredPath != null;
            this.nativeAccess = java.util.Objects.requireNonNull(nativeAccess);
            this.system = java.util.Objects.requireNonNull(system);
            source = configured ? this::read : system;
            if (!configured) { return; }
            try {
                library = checkedPath(configuredPath);
                nativeAccess.load(library);
                before = snapshot();
            } catch (Exception | LinkageError unavailable) { fail("NATIVE_SETUP_UNAVAILABLE", unavailable); }
        }

        LongSupplier source() { return source; }

        private long read() {
            if (reason != null) { return system.getAsLong(); }
            try { return nativeAccess.read(); }
            catch (RuntimeException | LinkageError unavailable) {
                fail("NATIVE_READ_UNAVAILABLE", unavailable);
                return system.getAsLong();
            }
        }

        private synchronized void fail(String next, Throwable failure) {
            if (reason == null) {
                String type = failure.getClass().getName();
                failureType = type.length() <= 128 && type.chars().allMatch(value -> value >= 0x20 && value <= 0x7e)
                        ? type : "UNAVAILABLE";
                reason = next;
            }
        }

        synchronized String metadata() {
            if (configured && reason == null) {
                try {
                    after = snapshot();
                    if (!before.equals(after)) { fail("NATIVE_IDENTITY_CHANGED", new IllegalStateException()); }
                } catch (Exception | LinkageError unavailable) { fail("NATIVE_METADATA_UNAVAILABLE", unavailable); }
            } else if (!configured && reason == null) {
                try {
                    Map<String, String> hashes = classHashes();
                    after = Map.of("classSha256", hashes.get("PdkBenchmarkClock"), "classHashes", hashes);
                    if (before.isEmpty()) { before = after; }
                    if (!before.equals(after)) { fail("CLASS_IDENTITY_CHANGED", new IllegalStateException()); }
                } catch (Exception | LinkageError unavailable) { fail("CLASS_IDENTITY_UNAVAILABLE", unavailable); }
            }
            var evidence = new LinkedHashMap<String, Object>();
            evidence.put("schemaVersion", 1);
            evidence.put("state", reason != null ? "UNKNOWN" : configured ? "NATIVE_RECORDED" : "SYSTEM_UNQUALIFIED");
            evidence.put("reason", reason == null ? "NONE" : reason); evidence.put("failureType", failureType);
            evidence.put("configured", configured); evidence.put("pid", pid); evidence.put("jvmStartTimeMillis", startMillis);
            evidence.put("provider", configured ? "DIRECT_MACH_ABSOLUTE_TIME_JNI" : "SYSTEM_NANO_TIME");
            evidence.put("activeProvider", configured && reason == null ? "DIRECT_MACH_ABSOLUTE_TIME_JNI" : "SYSTEM_NANO_TIME");
            evidence.put("counterUnit", "nominal-ns"); evidence.put("mixedDomainPossible", configured && reason != null);
            evidence.put("before", before); evidence.put("after", after);
            evidence.put("clockQualified", false); evidence.put("siTimeQualified", false); evidence.put("utcAccuracyQualified", false);
            evidence.put("samplingAgeQualified", false); evidence.put("callBoundaryQualified", false);
            evidence.put("loadingQualified", false); evidence.put("classIntrinsicQualified", false);
            evidence.put("completeOsFunctionBodyQualified", false); evidence.put("samplingCostQualified", false);
            evidence.put("costAcceptanceEligible", false); evidence.put("performanceAcceptanceEligible", false);
            evidence.put("formalPerformance", false);
            String result = JsonWriter.write(evidence);
            if (result.length() > 8192) { throw new AssertionError("bounded clock metadata exceeded its transport limit"); }
            return result;
        }

        private Map<String, Object> snapshot() throws Exception {
            String[] facts = nativeAccess.facts();
            if (facts == null || facts.length != 11) { throw new IOException("Native fact roster is invalid"); }
            for (String fact : facts) { checkedText(fact, 512); }
            if (!facts[0].matches("[A-Fa-f0-9]{8}(?:-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}")
                    || !facts[5].matches("[A-Fa-f0-9]{32}") || !"mach_absolute_time".equals(facts[10])
                    || !facts[7].matches("[A-Fa-f0-9]{288}")) {
                throw new IOException("Native route identity is invalid");
            }
            long numer = Long.parseLong(facts[1]), denom = Long.parseLong(facts[2]);
            int selector = Integer.parseInt(facts[8]);
            if (numer <= 0 || numer > 0xffff_ffffL || denom <= 0 || denom > 0xffff_ffffL || selector < 0 || selector > 255) {
                throw new IOException("Native scale or selector is invalid");
            }
            Long.parseUnsignedLong(facts[6]); Long.parseUnsignedLong(facts[9]);
            Path loaded = checkedPath(facts[3]);
            if (!library.equals(loaded)) { throw new IOException("Native getter belongs to a different library"); }
            if (!Path.of(facts[4]).isAbsolute()) { throw new IOException("OS image path is not absolute"); }
            var result = new LinkedHashMap<String, Object>();
            result.put("bootUuid", facts[0]); result.put("timebaseNumer", numer); result.put("timebaseDenom", denom);
            result.put("conversion", "UNSIGNED_128_MULTIPLY_INTEGER_DIVIDE_TRUNCATE");
            result.put("loadedJniPath", loaded.toString()); result.put("loadedJniSha256", hash(Files.newInputStream(loaded), 16 * 1024 * 1024));
            Map<String, String> hashes = classHashes();
            result.put("classSha256", hashes.get("PdkBenchmarkClock")); result.put("classHashes", hashes);
            result.put("osFunction", facts[10]); result.put("osImagePath", facts[4]); result.put("osImageUuid", facts[5]);
            result.put("osFunctionImageOffsetUnsigned", facts[6]); result.put("osFunctionCodePrefixBytes", 144);
            result.put("osFunctionCodePrefixSha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(HexFormat.of().parseHex(facts[7]))));
            result.put("userTimebaseSelector", selector); result.put("timebaseOffsetUnsigned", facts[9]);
            return Map.copyOf(result);
        }
    }

    private static Path checkedPath(String value) throws IOException {
        checkedText(value, 512);
        Path path = Path.of(value);
        if (!path.isAbsolute()) { throw new IOException("Native library path must be absolute"); }
        Path real = path.toRealPath(); checkedText(real.toString(), 512);
        if (!Files.isRegularFile(real) || Files.size(real) > 16 * 1024 * 1024) {
            throw new IOException("Native library file exceeds its admission bound");
        }
        return real;
    }

    private static void checkedText(String value, int limit) throws IOException {
        if (value == null || value.isEmpty() || value.length() > limit) { throw new IOException("Native text is missing or exceeds its bound"); }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < 0x20 || value.charAt(i) > 0x7e) { throw new IOException("Native text is not bounded printable ASCII"); }
        }
    }

    private static String hash(InputStream input, int limit) throws Exception {
        if (input == null) { throw new IOException("Actual class bytes are unavailable"); }
        try (input) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = new byte[8192]; int total = 0;
            for (int size; (size = input.read(bytes)) != -1;) {
                if (size > limit - total) { throw new IOException("Native identity bytes exceed their bound"); }
                total += size; digest.update(bytes, 0, size);
            }
            return HexFormat.of().formatHex(digest.digest());
        }
    }

    private static Map<String, String> classHashes() throws Exception {
        return Map.of(
                "PdkBenchmarkClock", hash(PdkBenchmarkClock.class.getResourceAsStream("PdkBenchmarkClock.class"), 256 * 1024),
                "PdkBenchmarkClock$Clock", hash(Clock.class.getResourceAsStream("PdkBenchmarkClock$Clock.class"), 256 * 1024),
                "PdkBenchmarkClock$JniAccess", hash(JniAccess.class.getResourceAsStream("PdkBenchmarkClock$JniAccess.class"), 256 * 1024),
                "PdkBenchmarkClock$NativeAccess", hash(NativeAccess.class.getResourceAsStream("PdkBenchmarkClock$NativeAccess.class"), 256 * 1024));
    }

    private static final class JniAccess implements NativeAccess {
        public void load(Path path) { System.load(path.toString()); }
        public long read() { return nativeNanoTime(); }
        public String[] facts() { return nativeFacts(); }
    }

    private static native long nativeNanoTime();
    private static native String[] nativeFacts();
    static native long convertedForControl(long unsignedTicks, long numerator, long denominator);
}
