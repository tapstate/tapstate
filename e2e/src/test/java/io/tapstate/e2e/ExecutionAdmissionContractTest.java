package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Metadata fixtures prove method selection only; actual bytecode, loader and call correlation remain live checks. */
class ExecutionAdmissionContractTest {
    private static final String WRAPPER = "io.tapstate.adapters.mongostore.MongoWorkloadClaimStore";
    private static final String SHARED = "io.tapstate.adapters.mongostore.MongoExecutionGenerationWrites";
    private static final String SHARED_SIGNATURE =
            "(Lcom/mongodb/client/ClientSession;Ljava/lang/String;Ljava/lang/String;)Ljava/util/Optional;";
    private static final String WRAPPER_SIGNATURE = "(Ljava/lang/String;Ljava/lang/String;)Ljava/util/OptionalLong;";
    @TempDir Path directory;

    @Test
    void theSharedMethodIsSelectedOnceWhenBothWrapperAndTransactionalPathsExist() throws Exception {
        var provenance = ExecutionAdmissionJdiSession.advanceProvenance(artifact(true, false));
        assertThat(provenance.type()).isEqualTo(SHARED);
        assertThat(provenance.method()).isEqualTo("advanceStandalone");
        assertThat(provenance.signature()).isEqualTo(SHARED_SIGNATURE);
        assertThat(provenance.artifactOrigin()).isEqualTo("BOOT-INF/classes/" + resource(SHARED));
        assertThat(provenance.methodSha256()).matches("[0-9a-f]{64}");
    }

    @Test
    void absentSharedMetadataSelectsThePinnedLegacyWrapperWithoutInventingTransactionalCoverage() throws Exception {
        var provenance = ExecutionAdmissionJdiSession.advanceProvenance(artifact(false, false));
        assertThat(provenance.type()).isEqualTo(WRAPPER);
        assertThat(provenance.signature()).isEqualTo(WRAPPER_SIGNATURE);
        var boundary = boundary(false, 1, 1);
        assertThat(boundary.drained()).isTrue();
        assertThat(boundary.observesTransactionalAdvance()).isFalse();
        var report = new BenchmarkLiveReport(directory.resolve("legacy.json"));
        new ExecutionAdmissionStages(report, true).recordCounts("legacy-start", boundary, 1, 1);
        Map<?, ?> evidence = firstFork(report);
        assertThat(evidence.get("transactionalAdvanceObservationSupported")).isEqualTo(false);
        assertThat(evidence.get("advanceObservationMode")).isEqualTo("EXACT_LEGACY_PUBLIC_WRAPPER");
    }

    @Test
    void duplicateSharedClassProvenanceCannotSilentlyFallBackToTheWrapper() throws Exception {
        assertThatThrownBy(() -> ExecutionAdmissionJdiSession.advanceProvenance(artifact(true, true)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicated");
    }

    @Test
    void aRebuildRequiresOneAuthoritativeAdvanceAndOneSubmitWithoutMongoCommitInference() throws Exception {
        var report = new BenchmarkLiveReport(directory.resolve("shared.json"));
        var stages = new ExecutionAdmissionStages(report, true);
        stages.recordCounts("rebuild-resume", boundary(true, 1, 1), 1, 1);
        Map<?, ?> evidence = firstFork(report);
        assertThat(evidence.get("transactionalAdvanceObservationSupported")).isEqualTo(true);
        assertThat(evidence.get("advanceObservationMode")).isEqualTo("EXACT_SHARED_GENERATION_METHOD");
        assertThat(evidence.get("physicalMongoCommandsInferred")).isEqualTo(false);
        assertThat(evidence.get("generationMethodReturnProvesTransactionCommit")).isEqualTo(false);

        var doubled = new ExecutionAdmissionStages(new BenchmarkLiveReport(directory.resolve("doubled.json")), true);
        assertThatThrownBy(() -> doubled.recordCounts("rebuild-resume", boundary(true, 2, 1), 1, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("ADVANCE_STANDALONE entries");
    }

    private Path artifact(boolean shared, boolean duplicate) throws Exception {
        Path jar = directory.resolve("metadata-" + shared + "-" + duplicate + ".jar");
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(jar))) {
            entry(output, "BOOT-INF/classes/" + resource(WRAPPER), classBytes(WRAPPER));
            entry(output, "BOOT-INF/classes/" + resource("io.tapstate.runtime.engine.Engine"),
                    classBytes("io.tapstate.runtime.engine.Engine"));
            if (shared) { entry(output, "BOOT-INF/classes/" + resource(SHARED), classBytes(SHARED)); }
            if (duplicate) {
                var bytes = new java.io.ByteArrayOutputStream();
                try (ZipOutputStream library = new ZipOutputStream(bytes)) { entry(library, resource(SHARED), classBytes(SHARED)); }
                entry(output, "BOOT-INF/lib/duplicate.jar", bytes.toByteArray());
            }
        }
        return jar;
    }

    private static void entry(ZipOutputStream output, String name, byte[] value) throws Exception {
        output.putNextEntry(new ZipEntry(name)); output.write(value); output.closeEntry();
    }

    private static byte[] classBytes(String type) throws Exception {
        try (InputStream input = ExecutionAdmissionContractTest.class.getResourceAsStream("/" + resource(type))) {
            if (input == null) { throw new AssertionError("compiled method fixture unavailable: " + type); }
            return input.readAllBytes();
        }
    }

    private static String resource(String type) { return type.replace('.', '/') + ".class"; }

    private static ExecutionAdmissionJdiSession.Boundary boundary(boolean shared, long advances, long submits) {
        Map<ExecutionAdmissionJdiSession.Target, ExecutionAdmissionJdiSession.Binding> bindings =
                new EnumMap<>(ExecutionAdmissionJdiSession.Target.class);
        Map<ExecutionAdmissionJdiSession.Target, ExecutionAdmissionJdiSession.Counts> counts =
                new EnumMap<>(ExecutionAdmissionJdiSession.Target.class);
        bindings.put(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE,
                binding(shared ? SHARED : WRAPPER, "advanceStandalone", shared ? SHARED_SIGNATURE : WRAPPER_SIGNATURE));
        bindings.put(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB, binding("io.tapstate.runtime.engine.Engine", "submitJob",
                "(Ljava/lang/String;Lcom/hazelcast/jet/core/DAG;)V"));
        counts.put(ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE, new ExecutionAdmissionJdiSession.Counts(advances, advances, 0, 0));
        counts.put(ExecutionAdmissionJdiSession.Target.SUBMIT_JOB, new ExecutionAdmissionJdiSession.Counts(submits, submits, 0, 0));
        Map<ExecutionAdmissionJdiSession.LeaseTarget, ExecutionAdmissionJdiSession.Binding> leases =
                new EnumMap<>(ExecutionAdmissionJdiSession.LeaseTarget.class);
        Map<ExecutionAdmissionJdiSession.LeaseTarget, ExecutionAdmissionJdiSession.Counts> leaseCounts =
                new EnumMap<>(ExecutionAdmissionJdiSession.LeaseTarget.class);
        for (var target : ExecutionAdmissionJdiSession.LeaseTarget.values()) {
            leases.put(target, binding(WRAPPER, target.name().toLowerCase(java.util.Locale.ROOT), "fixture"));
            leaseCounts.put(target, new ExecutionAdmissionJdiSession.Counts(0, 0, 0, 0));
        }
        return new ExecutionAdmissionJdiSession.Boundary(1, "a".repeat(64), "orders", bindings, counts,
                2, 0, true, true, leases, leaseCounts, false, false);
    }

    private static ExecutionAdmissionJdiSession.Binding binding(String type, String method, String signature) {
        return new ExecutionAdmissionJdiSession.Binding(type, method, signature, "fixture", "b".repeat(64),
                1, "org.springframework.boot.loader.launch.LaunchedClassLoader", 0, List.of(1));
    }

    private static Map<?, ?> firstFork(BenchmarkLiveReport report) throws Exception {
        Map<?, ?> document = (Map<?, ?>) JsonReader.parse(Files.readString(report.output()));
        return (Map<?, ?>) ((List<?>) document.get("forks")).getFirst();
    }
}
