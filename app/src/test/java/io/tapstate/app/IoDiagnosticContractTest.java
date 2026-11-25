package io.tapstate.app;

import io.tapstate.core.common.Severity;
import io.tapstate.spi.store.IoError;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class IoDiagnosticContractTest {
    @Test void definitionsKeepParametersSeverityAndHistoricalRecovery() {
        var commitOutcomeUnknown = IoError.PIPELINE_COMMIT_OUTCOME_UNKNOWN;
        assertThat(commitOutcomeUnknown.code()).isEqualTo("TS0480042");
        assertThat(commitOutcomeUnknown.placeholders()).containsExactly("pipeline");
        var keyMove = IoError.KEY_MOVE_RECOVERY_UNCERTAIN;
        assertThat(keyMove.code()).isEqualTo("TS0480043");
        assertThat(keyMove.placeholders()).containsExactly("pipeline");
        var codes = List.of(IoError.values());
        assertThat(codes).hasSize(43);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            assertThat(code.severity()).isEqualTo(Severity.ERROR);
            assertThat(code.code()).matches("TS048(?!0000)[0-9]{4}");
            if (code != commitOutcomeUnknown && code != keyMove) {
                String legacy = "io." + code.name().toLowerCase(Locale.ROOT).replace('_', '-');
                assertThat(catalog.render(code.code(), params)).isEqualTo(catalog.render(legacy, params));
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
