package io.tapstate.archtests;

import io.tapstate.messages.ConnectionDiagnosticCatalog;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionDiagnosticDefinitionGatesTest {
    @Test
    void registeredCodesAndHistoricalReservationAgree() throws Exception {
        Map<String, String> active = new HashMap<>();
        for (var entry : ConnectionDiagnosticCatalog.Definition.values()) {
            assertThat(entry.code()).matches("TS006(?!0000)[0-9]{4}");
            assertThat(active.putIfAbsent(entry.code(), entry.name())).isNull();
        }
        try (var resource = getClass().getResourceAsStream("/connection-diagnostic-published.tsv")) {
            assertThat(resource).isNotNull();
            verifyHistory(active, new String(resource.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertThat(ConnectionDiagnosticCatalog.bundled()).isNotNull();
    }

    @Test
    void publishedIdentitiesCannotBeRenamedOrReusedAfterRetirement() {
        String code = "TS0060012";
        verifyHistory(Map.of(code, "ORIGINAL"), code + "\tORIGINAL\tactive\n");
        verifyHistory(Map.of(), code + "\tORIGINAL\tretired\n");
        assertThatThrownBy(() -> verifyHistory(Map.of(code, "OTHER"), code + "\tORIGINAL\tactive\n"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> verifyHistory(Map.of(code, "OTHER"), code + "\tORIGINAL\tretired\n"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void verifyHistory(Map<String, String> active, String baseline) {
        Set<String> recorded = new HashSet<>();
        for (String line : baseline.lines().toList()) {
            if (line.isBlank() || line.startsWith("#")) continue;
            String[] fields = line.split("\t", -1);
            if (fields.length != 3 || !recorded.add(fields[0]) || !fields[0].matches("TS006(?!0000)[0-9]{4}")
                    || !fields[1].matches("[A-Z][A-Z0-9_]*")) {
                throw new IllegalArgumentException("invalid published diagnostic declaration");
            }
            boolean allowed = switch (fields[2]) {
                case "active" -> fields[1].equals(active.get(fields[0]));
                case "retired" -> !active.containsKey(fields[0]);
                default -> false;
            };
            if (!allowed) throw new IllegalArgumentException("published diagnostic identity cannot be reassigned");
        }
    }
}
