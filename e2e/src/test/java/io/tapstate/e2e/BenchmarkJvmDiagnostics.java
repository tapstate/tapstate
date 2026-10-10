package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Built-in JVM diagnostics for one owned child; these launches cannot establish performance acceptance. */
final class BenchmarkJvmDiagnostics {
    private BenchmarkJvmDiagnostics() { }

    static BenchmarkForkEnvironment.OwnedBoot start(String storeUri, String operatorDatabase, Path jar)
            throws Exception {
        return start(storeUri, operatorDatabase, jar, false);
    }

    static BenchmarkForkEnvironment.OwnedBoot startWithWriteReturns(String storeUri, String operatorDatabase, Path jar)
            throws Exception {
        return start(storeUri, operatorDatabase, jar, true);
    }

    private static BenchmarkForkEnvironment.OwnedBoot start(String storeUri, String operatorDatabase, Path jar,
            boolean writeReturns) throws Exception {
        Path directory = Files.createTempDirectory("tapstate-benchmark-jvm-gap-");
        Path settings = directory.resolve("owned-jvm-gap.jfc");
        try (var input = BenchmarkJvmDiagnostics.class.getResourceAsStream("/benchmark-jvm-gap.jfc")) {
            if (input == null) { throw new AssertionError("owned JVM diagnostic settings are absent"); }
            Files.copy(input, settings);
        }
        Path recording = directory.resolve("runtime.jfr");
        Path gcLog = directory.resolve("gc-safepoint.log");
        List<String> options = jvmOptions(settings, recording, gcLog, writeReturns);
        RealProcessServer server = RealProcessServer.start(storeUri, operatorDatabase, jar, "127.0.0.1",
                ignored -> List.of(), options);
        var receipt = new java.util.LinkedHashMap<String, Object>(Map.of(
                "pid", server.pid(), "applicationJar", jar.toString(),
                "recording", recording.toString(), "gcSafepointLog", gcLog.toString(),
                "jvmArguments", options, "performanceAcceptanceEligible", false));
        if (writeReturns) {
            receipt.put("writeReturnDiagnostics", true); receipt.put("formalPerformance", false);
            receipt.put("costAcceptanceEligible", false); receipt.put("phaseAttributionQualified", false);
        }
        System.out.println("benchmark-jvm-diagnostic-files=" + JsonWriter.write(receipt));
        return new BenchmarkForkEnvironment.OwnedBoot(server, null);
    }

    static List<String> jvmOptions(Path settings, Path recording, Path gcLog, boolean writeReturns) {
        var options = new java.util.ArrayList<>(List.of(
                "-Xlog:gc*,safepoint:file=" + gcLog + ":utctime,uptimenanos,level,tags:filecount=1,filesize=4m",
                "-XX:StartFlightRecording=filename=" + recording
                        + ",settings=" + settings + ",dumponexit=true,maxsize=64m"));
        if (writeReturns) { options.add("-Dtapstate.benchmark.write-return=true"); }
        return List.copyOf(options);
    }
}
