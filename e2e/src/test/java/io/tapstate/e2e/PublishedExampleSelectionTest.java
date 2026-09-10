package io.tapstate.e2e;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class PublishedExampleSelectionTest {
    private static final Path FIRST = Path.of("examples/a/a.e2e.yml");
    private static final Path SECOND = Path.of("examples/b/b.e2e.yml");
    private static final List<Path> ALL = List.of(FIRST, SECOND);

    @Test
    void absentSelectionKeepsTheEntireDiscoveredSet() {
        assertThat(PublishedExampleSelection.select(ALL, null)).isEqualTo(ALL);
    }

    @Test
    void selectionKeepsSourceOrderAndBothTiersWithStableIdentity() {
        assertThat(PublishedExampleSelection.select(ALL, SECOND.toString())).containsExactly(SECOND);
        assertThat(PublishedExampleSelection.select(ALL, SECOND + "," + FIRST)).containsExactly(FIRST, SECOND);
        assertThat(PublishedExampleSelection.identity(SECOND, Tiers.IN_PROCESS))
                .isEqualTo("examples/b/b.e2e.yml on IN_PROCESS");
        assertThat(PublishedExampleSelection.identity(SECOND, Tiers.REAL_PROCESS))
                .isEqualTo("examples/b/b.e2e.yml on REAL_PROCESS");
    }

    @Test
    void invalidSelectionFailsRatherThanSilentlyRunningEverything() {
        for (String selection : List.of("", " ", "missing", FIRST + "," + FIRST, FIRST + ",", "," + FIRST)) {
            assertThatIllegalArgumentException().isThrownBy(() -> PublishedExampleSelection.select(ALL, selection));
        }
    }

    @Test
    void eachExecutionWritesExactlyOneAtomicIdentityLine() {
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            PublishedExampleSelection.reportIdentity(SECOND, Tiers.REAL_PROCESS);
        } finally {
            System.setOut(original);
        }
        assertThat(output.toString(StandardCharsets.UTF_8)).isEqualTo(
                "tapstate.published-case=examples/b/b.e2e.yml on REAL_PROCESS" + System.lineSeparator());
    }

    @Test
    void separateSweepJvmsCannotTruncateEachOthersLedgers(@TempDir Path build) throws Exception {
        runLedgerJvm(build, true);
        runLedgerJvm(build, true);
        try (var paths = Files.list(build.resolve("witness-ledgers"))) {
            List<Path> ledgers = paths.toList();
            assertThat(ledgers).hasSize(2);
            for (Path ledger : ledgers) {
                assertThat(Files.readString(ledger)).isEqualTo("examples/b/b.e2e.yml on REAL_PROCESS" + System.lineSeparator());
            }
        }
        assertThat(build.resolve("witness-ledger.txt")).doesNotExist();
    }

    @Test
    void defaultSweepKeepsTheReleaseManifestLedgerVocabulary(@TempDir Path build) throws Exception {
        runLedgerJvm(build, false);
        assertThat(Files.readString(build.resolve("witness-ledger.txt")))
                .isEqualTo("b on REAL_PROCESS" + System.lineSeparator());
        assertThat(build.resolve("witness-ledgers")).doesNotExist();
    }

    private static void runLedgerJvm(Path build, boolean selected) throws Exception {
        var command = new java.util.ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dtapstate.e2e.build-directory=" + build));
        if (selected) {
            command.add("-D" + PublishedExampleSelection.PROPERTY + "=" + SECOND);
        }
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), LedgerProbe.class.getName()));
        Process process = new ProcessBuilder(command).inheritIO().start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).isZero();
        } finally {
            process.destroyForcibly();
        }
    }

    public static class LedgerProbe {
        public static void main(String[] args) {
            WitnessLedger.record(SECOND, Tiers.REAL_PROCESS);
        }
    }

    @Test
    void actualProviderFiltersExamplesAndKeepsBothTiers() {
        String previous = System.getProperty(PublishedExampleSelection.PROPERTY);
        try {
            System.clearProperty(PublishedExampleSelection.PROPERTY);
            var full = PublishedExamplesIT.everyPublishedExampleOnEveryTier()
                    .map(argument -> argument.get()).toList();
            assertThat(full).hasSize(Examples.specifications().size() * Tiers.values().length);
            for (Path path : Examples.specifications()) {
                assertThat(full.stream().filter(argument -> argument[0].equals(path)).map(argument -> argument[1]))
                        .containsExactly((Object[]) Tiers.values());
            }
            Path chosen = Examples.specifications().getLast();
            System.setProperty(PublishedExampleSelection.PROPERTY, chosen.toString());
            var arguments = PublishedExamplesIT.everyPublishedExampleOnEveryTier()
                    .map(argument -> argument.get()).toList();
            assertThat(arguments).hasSize(Tiers.values().length);
            assertThat(arguments.stream().map(argument -> argument[0])).containsOnly(chosen);
            assertThat(arguments.stream().map(argument -> argument[1])).containsExactly((Object[]) Tiers.values());
        } finally {
            if (previous == null) {
                System.clearProperty(PublishedExampleSelection.PROPERTY);
            } else {
                System.setProperty(PublishedExampleSelection.PROPERTY, previous);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void executionIdentityIsCapturedInsideEachParameterizedTestcase(Tiers tier) {
        PublishedExampleSelection.reportIdentity(SECOND, tier);
        assertThat(PublishedExampleSelection.identity(SECOND, tier)).endsWith(" on " + tier.name());
    }

    @Test
    void newlyDiscoveredExamplesJoinTheDefaultSelection() {
        Path added = Path.of("examples/new/new.e2e.yml");
        assertThat(PublishedExampleSelection.select(List.of(FIRST, SECOND, added), null)).containsExactly(FIRST, SECOND, added);
    }
}
