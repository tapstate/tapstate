package io.tapstate.archtests;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.ProductErrorModule;
import io.tapstate.core.common.RequestErrorDefinitions;
import io.tapstate.core.common.Severity;
import io.tapstate.messages.RequestDiagnosticCatalog;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class RequestDiagnosticDefinitionGatesTest {
    @Test
    void explicitAllocationsAndSharedRequestAuthorityMatchTheReservedContract() {
        assertThat(ProductErrorModule.CONNECTOR.number()).isEqualTo("006");
        assertThat(ProductErrorModule.CONTROL.number()).isEqualTo("021");
        assertThat(java.util.Arrays.stream(ProductErrorModule.values()).map(ProductErrorModule::number).toList())
                .doesNotHaveDuplicates().allMatch(number -> number.matches("(?!000)[0-9]{3}"));
        var request = RequestErrorDefinitions.MALFORMED_REQUEST;
        assertThat(RequestErrorDefinitions.registered()).containsExactly(request);
        assertThat(request.code()).isEqualTo("TS0210001");
        assertThat(request.parameters()).isEqualTo(Set.of("reason"));
        assertThat(request.severity()).isEqualTo(Severity.ERROR);
        assertThat(ControlError.MALFORMED_REQUEST.code()).isEqualTo(request.code());
        assertThat(ControlError.MALFORMED_REQUEST.placeholders()).isEqualTo(request.parameters());
        assertThat(ControlError.MALFORMED_REQUEST.severity()).isEqualTo(request.severity());
        var lookup = RequestDiagnosticCatalog.bundled();
        Set<String> messages = new java.util.HashSet<>();
        Set<String> solutions = new java.util.HashSet<>();
        for (Locale locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
            var resolved = lookup.resolve(request.code(), Map.of("reason", "fixture"), List.of(), locale).orElseThrow();
            assertThat(resolved.code()).isEqualTo("TS0210001");
            assertThat(resolved.message()).contains("fixture").doesNotContain("{reason}");
            assertThat(resolved.solution()).isNotBlank();
            messages.add(resolved.message());
            solutions.add(resolved.solution());
        }
        assertThat(messages).hasSize(3);
        assertThat(solutions).hasSize(3);
        assertThat(lookup.resolve("control.malformed-request", Map.of("reason", "fixture"), List.of(), Locale.ENGLISH))
                .isEmpty();
    }

    @Test
    void onlyTheExplicitRequestMigrationDiffersFromTheIndependentLegacySnapshot() throws Exception {
        var baseline = readCodes("legacy-error-codes-before-request.golden");
        assertThat(baseline).hasSize(454).contains("control.malformed-request").doesNotContain("TS0210001");
        baseline.remove("control.malformed-request");
        baseline.add("TS0210001");
        assertThat(readCodes("error-codes.golden")).isEqualTo(baseline);
    }

    private static Set<String> readCodes(String file) throws Exception {
        var result = new TreeSet<String>();
        for (String line : Files.readAllLines(Path.of("src/test/resources", file))) {
            if (!line.isBlank() && !line.startsWith("#"))
                assertThat(result.add(line.strip())).as("duplicate identity: %s", line).isTrue();
        }
        return result;
    }
}
