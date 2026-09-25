package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A stopped run retains its completed fork evidence and a gate failure remains visible. */
class BenchmarkLiveReportTest {

    @TempDir
    Path directory;

    @Test
    void aFailureRetainsTheLastCompletedForkOnDisk() throws Exception {
        Path output = directory.resolve("raw.json");
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.begin(Map.of("gate", "OBSERVABILITY_COST"), Map.of("jdk", "21"), List.of());
        report.addFork(Map.of("id", "copy-A-1", "deliveryNanos", List.of(11L, 17L)));

        Map<?, ?> afterFork = read(output);
        assertThat(afterFork.get("status")).isEqualTo("RUNNING");
        assertThat((List<?>) afterFork.get("forks")).hasSize(1);
        assertThat(((Map<?, ?>) ((List<?>) afterFork.get("forks")).getFirst()).get("id"))
                .isEqualTo("copy-A-1");

        report.fail(new IllegalStateException("source stopped"));
        Map<?, ?> failed = read(output);
        assertThat(failed.get("status")).isEqualTo("FAILED");
        assertThat((List<?>) failed.get("forks")).hasSize(1);
        assertThat(((Map<?, ?>) failed.get("failure")).get("message")).isEqualTo("source stopped");
        try (var files = Files.list(directory)) {
            assertThat(files.map(Path::getFileName).map(Path::toString).toList())
                    .containsExactly("raw.json");
        }
    }

    @Test
    void anEvaluationFailureIsPersistedBeforeTheRunIsRejected() throws Exception {
        Path output = directory.resolve("gate.json");
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.finish(Map.of("passed", false, "failures", List.of("throughput regressed")), false);

        Map<?, ?> failed = read(output);
        assertThat(failed.get("status")).isEqualTo("FAILED");
        assertThat(((Map<?, ?>) failed.get("evaluation")).get("failures"))
                .isEqualTo(List.of("throughput regressed"));
        assertThatThrownBy(() -> new BenchmarkLiveReport(output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already exists");
    }

    private static Map<?, ?> read(Path path) throws Exception {
        return (Map<?, ?>) JsonReader.parse(Files.readString(path));
    }
}
