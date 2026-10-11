package io.tapstate.testsupport;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CiProviderInventoryTest {
    @Test
    void discoveryNeverExecutesADynamicAssertion() {
        AtomicInteger executions = new AtomicInteger();
        var rows = CiProviderInventory.enumerate(Stream.of(
                DynamicTest.dynamicTest("first named case", executions::incrementAndGet),
                DynamicTest.dynamicTest("second named case", () -> { throw new AssertionError("must not execute"); })), true);
        assertThat(rows).hasSize(2);
        assertThat(executions).hasValue(0);
        assertThat(rows.getFirst()).isNotEqualTo(rows.getLast());
    }

    @Test
    void equallySizedDifferentArgumentsHaveDifferentDigests() {
        var before = CiProviderInventory.enumerate(Stream.of(Arguments.of("named", 1)), false);
        var after = CiProviderInventory.enumerate(Stream.of(Arguments.of("named", 2)), false);
        assertThat(before).isNotEqualTo(after);
        assertThat(before.getFirst()).doesNotContain("named");
    }

    @Test
    void unknownObjectsRemainUnstable() {
        var rows = CiProviderInventory.enumerate(Stream.of(Arguments.of("named", new Object())), false);
        assertThat(rows.getFirst()).contains("\"stable\":false");
    }

    @Test
    void emptyProvidersFailClosed() {
        assertThatThrownBy(() -> CiProviderInventory.enumerate(Stream.empty(), false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
    }

    @Test
    void unreviewedClassesAreRejectedBeforeInitialization(@TempDir Path directory) throws Exception {
        Path requests = directory.resolve("requests.tsv");
        Files.writeString(requests, "unreviewed.Type\ttest\tprovider\tsource\n");
        assertThatThrownBy(() -> CiProviderInventory.main(new String[] {requests.toString(), directory.resolve("out.jsonl").toString()}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unreviewed");
    }
}
