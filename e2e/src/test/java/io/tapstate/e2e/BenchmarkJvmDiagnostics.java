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
        Path directory = Files.createTempDirectory("tapstate-benchmark-jvm-gap-");
        Path settings = directory.resolve("owned-jvm-gap.jfc");
        try (var input = BenchmarkJvmDiagnostics.class.getResourceAsStream("/benchmark-jvm-gap.jfc")) {
            if (input == null) { throw new AssertionError("owned JVM diagnostic settings are absent"); }
            Files.copy(input, settings);
        }
        Path recording = directory.resolve("runtime.jfr");
        Path gcLog = directory.resolve("gc-safepoint.log");
        List<String> options = List.of(
                "-Xlog:gc*,safepoint:file=" + gcLog + ":utctime,uptimenanos,level,tags:filecount=1,filesize=4m",
                "-XX:StartFlightRecording=filename=" + recording
                        + ",settings=" + settings + ",dumponexit=true,maxsize=64m");
        RealProcessServer server = RealProcessServer.start(storeUri, operatorDatabase, jar, "127.0.0.1",
                ignored -> List.of(), options);
        System.out.println("benchmark-jvm-diagnostic-files=" + JsonWriter.write(Map.of(
                "pid", server.pid(), "applicationJar", jar.toString(),
                "recording", recording.toString(), "gcSafepointLog", gcLog.toString(),
                "jvmArguments", options, "performanceAcceptanceEligible", false)));
        return new BenchmarkForkEnvironment.OwnedBoot(server, null);
    }
}
