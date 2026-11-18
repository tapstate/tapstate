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
        assertThat(RequestErrorDefinitions.registered()).hasSize(5).contains(request);
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
    void onlyExplicitMigrationsDifferFromTheIndependentLegacySnapshot() throws Exception {
        var baseline = readCodes("legacy-error-codes-before-request.golden");
        assertThat(baseline).hasSize(454).contains("control.malformed-request").doesNotContain("TS0210001");
        Set<String> replacements = new TreeSet<>();
        for (String line : Files.readAllLines(Path.of("src/test/resources/product-code-migrations.tsv"))) {
            if (line.isBlank() || line.startsWith("#")) continue;
            String[] pair = line.split("\t");
            assertThat(pair).hasSize(2);
            assertThat(baseline.remove(pair[0])).as("existing legacy identity %s", pair[0]).isTrue();
            assertThat(replacements.add(pair[1])).as("unique replacement %s", pair[1]).isTrue();
        }
        assertThat(replacements).hasSize(38);
        baseline.addAll(replacements);
        assertThat(readCodes("error-codes.golden")).isEqualTo(baseline);
    }

    @Test
    void authenticationDefinitionsPreserveTypedContractsAndHistoricalEnglishText() {
        var definitions = List.of(RequestErrorDefinitions.AUTH_FAILED, RequestErrorDefinitions.UNAUTHENTICATED,
                RequestErrorDefinitions.FORBIDDEN, RequestErrorDefinitions.UNREACHABLE);
        var owners = List.of(ControlError.AUTH_FAILED, ControlError.UNAUTHENTICATED,
                ControlError.FORBIDDEN, ControlError.UNREACHABLE);
        var historical = List.of("control.auth-failed", "control.unauthenticated", "control.forbidden", "control.unreachable");
        var catalog = io.tapstate.messages.MessageCatalog.bundled();
        for (int i = 0; i < definitions.size(); i++) {
            var definition = definitions.get(i);
            var owner = owners.get(i);
            assertThat(owner.code()).isEqualTo(definition.code());
            assertThat(owner.placeholders()).isEqualTo(definition.parameters());
            assertThat(owner.severity()).isEqualTo(definition.severity()).isEqualTo(Severity.ERROR);
            Map<String, Object> args = new java.util.HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            assertThat(catalog.render(definition.code(), args)).isEqualTo(catalog.render(historical.get(i), args));
            var messages = new java.util.HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = RequestDiagnosticCatalog.bundled().resolve(definition.code(), args, List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
                for (var parameter : definition.parameters())
                    assertThat(resolved.message()).contains("value-" + parameter).doesNotContain("{" + parameter + "}");
            }
            assertThat(messages).hasSize(3);
            assertThat(RequestDiagnosticCatalog.bundled().resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
        }
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
