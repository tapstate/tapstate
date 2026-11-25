package io.tapstate.cli;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class CliDiagnosticContractTest {
    @Test void cliDefinitionsKeepParametersSeverityAndHistoricalRecovery() {
        var codes = new ArrayList<TapstateErrorCode>();
        codes.addAll(List.of(CliError.values()));
        assertThat(codes).hasSize(65);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            String legacy = "cli."
                    + ((Enum<?>) code).name().toLowerCase(Locale.ROOT).replace('_', '-');
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            assertThat(code.severity()).isEqualTo(code == CliError.START_CHECKS_NOT_RUN || code == CliError.VERSION_MISMATCH
                    ? Severity.WARNING : Severity.ERROR);
            assertThat(code.code()).matches("TS033(?!0000)[0-9]{4}");
            boolean hasLegacyKey = code != CliError.WORKBENCH_NEEDS_A_TERMINAL && code != CliError.WORKBENCH_UNAVAILABLE;
            if (hasLegacyKey) {
                var historical = catalog.render(legacy, params);
                if (!Set.of(CliError.REQUEST_FAILED, CliError.CONNECTION_LOST, CliError.STREAM_NO_FRAMES).contains(code)) {
                    assertThat(catalog.render(code.code(), params).message()).isEqualTo(historical.message());
                }
                if (historical.solution() != null) {
                    assertThat(catalog.render(code.code(), params).solution()).isEqualTo(historical.solution());
                }
            }
            var messages = new HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var text = catalog.render(code.code(), params, locale);
                assertThat(text.message()).isNotBlank().doesNotContain(code.code());
                assertThat(text.solution()).isNotBlank();
                messages.add(text.message());
                code.placeholders().forEach(name -> assertThat(text.message() + text.solution()).contains("value-" + name));
            }
            assertThat(messages).hasSize(3);
        }
    }
}
