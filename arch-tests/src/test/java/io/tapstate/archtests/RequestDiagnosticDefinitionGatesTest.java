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
        assertThat(RequestErrorDefinitions.registered()).contains(request);
        assertThat(RequestErrorDefinitions.registered().stream().map(definition -> definition.code()).toList())
                .containsExactlyInAnyOrderElementsOf(java.util.Arrays.stream(ControlError.values())
                        .map(ControlError::code).toList());
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
        // This identity landed on main after the immutable migration snapshot.
        assertThat(baseline.add("actuation.sync-output-identity-unresolved")).isTrue();
        // These retained startup and upgrade identities were introduced after the frozen snapshot.
        for (String legacy : java.util.List.of("boot.migration-mode-conflict", "store.config-component-identity-required",
                "store.config-component-identity-unverified")) assertThat(baseline.add(legacy)).isTrue();
        var aggregateBaseline = readCodes("legacy-error-codes-before-aggregate-migration.golden");
        assertThat(aggregateBaseline).hasSize(33).doesNotContainAnyElementsOf(baseline);
        baseline.addAll(aggregateBaseline);
        var managedBaseline = readCodes("legacy-error-codes-before-managed-ai-migration.golden");
        assertThat(managedBaseline).containsExactlyInAnyOrder("control.ai-managed-provider-auth-failed",
                "control.ai-managed-provider-input-limit").doesNotContainAnyElementsOf(baseline);
        baseline.addAll(managedBaseline);
        Set<String> replacements = new TreeSet<>();
        for (String line : Files.readAllLines(Path.of("src/test/resources/product-code-migrations.tsv"))) {
            if (line.isBlank() || line.startsWith("#")) continue;
            String[] pair = line.split("\t");
            assertThat(pair).hasSize(2);
            assertThat(baseline.remove(pair[0])).as("existing legacy identity %s", pair[0]).isTrue();
            assertThat(replacements.add(pair[1])).as("unique replacement %s", pair[1]).isTrue();
        }
        assertThat(replacements).hasSize(493).contains("TS0310032", "TS0470019", "TS0470020");
        baseline.addAll(replacements);
        // Existing migrated contracts stay present; new product identities are append-only.
        assertThat(readCodes("error-codes.golden")).containsAll(baseline);
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

    @Test
    void assistantDefinitionsPreserveTypedContractsAndLocalizedRecoveryWithoutAliases() {
        var owners = List.of(ControlError.AI_NOT_CONFIGURED, ControlError.AI_PROVIDER_AUTH_FAILED,
                ControlError.AI_PROVIDER_REQUEST_REJECTED, ControlError.AI_PROVIDER_RATE_LIMITED,
                ControlError.AI_PROVIDER_FAILED, ControlError.AI_PROVIDER_OUTPUT_LIMIT,
                ControlError.AI_INVALID_PROPOSAL, ControlError.AI_OVERLOADED);
        var historical = List.of("control.ai-not-configured", "control.ai-provider-auth-failed",
                "control.ai-provider-request-rejected", "control.ai-provider-rate-limited",
                "control.ai-provider-failed", "control.ai-provider-output-limit",
                "control.ai-invalid-proposal", "control.ai-overloaded");
        var catalog = io.tapstate.messages.MessageCatalog.bundled();
        for (int i = 0; i < owners.size(); i++) {
            var owner = owners.get(i);
            assertThat(owner.code()).isEqualTo("TS021" + String.format("%04d", i + 6));
            assertThat(owner.placeholders()).isEmpty();
            assertThat(owner.severity()).isEqualTo(Severity.ERROR);
            var current = catalog.render(owner.code(), Map.of());
            var previous = catalog.render(historical.get(i), Map.of());
            assertThat(current.message()).isEqualTo(previous.message());
            if (previous.solution() != null) assertThat(current.solution()).isEqualTo(previous.solution());
            var messages = new java.util.HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = RequestDiagnosticCatalog.bundled().resolve(owner.code(), Map.of(), List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
            }
            assertThat(messages).hasSize(3);
            assertThat(RequestDiagnosticCatalog.bundled().resolve(historical.get(i), Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
        }
    }

    @Test
    void controlCapabilityAndWarningDefinitionsPreserveSeverityParametersAndHistoricalText() {
        var owners = List.of(ControlError.PREVIEW_OVERLOADED, ControlError.AUDIT_BLOCKED,
                ControlError.SCHEMA_DERIVATION_INCOMPLETE, ControlError.BOOTSTRAP_FORBIDDEN,
                ControlError.BOOTSTRAP_CLOSED, ControlError.AUTH_MODE_UNAVAILABLE,
                ControlError.BROWSER_AUTH_CONFIG_INVALID, ControlError.CLOUD_AUTH_UNAVAILABLE,
                ControlError.STATE_DATABASE_UNAVAILABLE, ControlError.SOURCE_CONFIG_REVEAL_UNAVAILABLE);
        var catalog = io.tapstate.messages.MessageCatalog.bundled();
        for (int i = 0; i < owners.size(); i++) {
            var owner = owners.get(i);
            String historical = "control." + owner.name().toLowerCase(Locale.ROOT).replace('_', '-');
            assertThat(owner.code()).isEqualTo("TS021" + String.format("%04d", i + 14));
            assertThat(owner.severity()).isEqualTo(owner == ControlError.SCHEMA_DERIVATION_INCOMPLETE
                    ? Severity.WARNING : Severity.ERROR);
            Map<String, Object> args = new java.util.HashMap<>();
            owner.placeholders().forEach(name -> args.put(name, "value-" + name));
            assertThat(catalog.render(owner.code(), args)).isEqualTo(catalog.render(historical, args));
            var messages = new java.util.HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = RequestDiagnosticCatalog.bundled().resolve(owner.code(), args, List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
            }
            assertThat(messages).hasSize(3);
            assertThat(RequestDiagnosticCatalog.bundled().resolve(historical, args, List.of(), Locale.ENGLISH)).isEmpty();
        }
    }

    @Test
    void storageDefinitionsKeepTheirParametersAndHistoricalMessagesWithLocalizedRecovery() {
        var owners = List.of(ControlError.STORAGE_DATABASES_OVERLAP, ControlError.STORAGE_TEST_REQUIRED,
                ControlError.STORAGE_TEST_FAILED, ControlError.STORAGE_CACHE_DATABASE_OCCUPIED,
                ControlError.STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED, ControlError.STORAGE_SCOPE_CONFLICT,
                ControlError.STORAGE_UNAVAILABLE, ControlError.STORAGE_QUOTA_EXCEEDED);
        var catalog = io.tapstate.messages.MessageCatalog.bundled();
        for (int i = 0; i < owners.size(); i++) {
            var owner = owners.get(i);
            String historical = "control." + owner.name().toLowerCase(Locale.ROOT).replace('_', '-');
            assertThat(owner.code()).isEqualTo("TS021" + String.format("%04d", i + 24));
            assertThat(owner.severity()).isEqualTo(Severity.ERROR);
            Map<String, Object> args = new java.util.HashMap<>();
            owner.placeholders().forEach(name -> args.put(name, "value-" + name));
            var current = catalog.render(owner.code(), args);
            var previous = catalog.render(historical, args);
            assertThat(current.message()).isEqualTo(previous.message());
            if (previous.solution() != null) assertThat(current.solution()).isEqualTo(previous.solution());
            var messages = new java.util.HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = RequestDiagnosticCatalog.bundled().resolve(owner.code(), args, List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
            }
            assertThat(messages).hasSize(3);
            assertThat(RequestDiagnosticCatalog.bundled().resolve(historical, args, List.of(), Locale.ENGLISH)).isEmpty();
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
