package io.tapstate.app;

import io.tapstate.core.common.Severity;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class IoDiagnosticContractTest {
    @Test void remainingDefinitionsKeepParametersSeverityAndHistoricalRecovery() {
        var codes = new ArrayList<TapstateErrorCode>();
        codes.addAll(List.of(IoError.values()));
        assertThat(codes).hasSize(42);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            String legacy = "io." + ((Enum<?>) code).name().toLowerCase(Locale.ROOT).replace('_', '-');
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            assertThat(code.severity()).isEqualTo(Severity.ERROR);
            assertThat(code.code()).matches("TS048(?!0000)[0-9]{4}");
            if (code != IoError.KEY_MOVE_RECOVERY_UNCERTAIN) {
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
