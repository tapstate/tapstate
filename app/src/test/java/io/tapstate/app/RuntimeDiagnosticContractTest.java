package io.tapstate.app;

import io.tapstate.core.common.Severity;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class RuntimeDiagnosticContractTest {
    @Test void runtimeDefinitionsKeepParametersSeverityAndHistoricalRecovery() {
        var codes = new ArrayList<TapstateErrorCode>();
        codes.addAll(List.of(AlertError.values()));
        codes.addAll(List.of(CaptureError.values()));
        assertThat(codes).hasSize(21);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            String legacy = (code instanceof AlertError ? "alert." : "capture.")
                    + ((Enum<?>) code).name().toLowerCase(Locale.ROOT).replace('_', '-');
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            assertThat(code.severity()).isEqualTo(code == CaptureError.OBSERVER_FAILED ? Severity.WARNING : Severity.ERROR);
            assertThat(code.code()).matches("TS03[45](?!0000)[0-9]{4}");
            assertThat(catalog.render(code.code(), params)).isEqualTo(catalog.render(legacy, params));
            var messages = new HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var text = catalog.render(code.code(), params, locale);
                assertThat(text.message()).isNotBlank().doesNotContain(code.code());
                assertThat(text.solution()).isNotBlank();
                messages.add(text.message());
                code.placeholders().forEach(name -> assertThat(text.message()).contains("value-" + name));
            }
            assertThat(messages).hasSize(3);
        }
    }
}
