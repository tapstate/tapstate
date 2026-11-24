package io.tapstate.app;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.dsl.NestSizingError;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.adapters.transform.TransformError;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class EngineTransformDiagnosticContractTest {
    @Test void runtimeIdentitiesPreserveWarningsParametersAndHistoricalText() {
        var codes = new ArrayList<TapstateErrorCode>();
        codes.addAll(List.of(EngineError.values()));
        codes.addAll(List.of(TransformError.values()));
        codes.addAll(List.of(NestSizingError.values()));
        assertThat(codes).hasSize(26);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            String domain = code instanceof EngineError ? "engine" : code instanceof TransformError ? "transform" : "nest";
            String legacy = domain + "." + ((Enum<?>) code).name().toLowerCase(Locale.ROOT).replace('_', '-');
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            boolean warning = code instanceof NestSizingError || code == EngineError.FRONTIER_PINNED
                    || code == EngineError.JOIN_DIMENSION_ROW_DISPLACED || code == TransformError.UNWIND_ROWS_SHARE_A_KEY;
            assertThat(code.severity()).isEqualTo(warning ? Severity.WARNING : Severity.ERROR);
            assertThat(code.code()).matches("TS04[456](?!0000)[0-9]{4}");
            var historical = catalog.render(legacy, params);
            assertThat(catalog.render(code.code(), params).message()).isEqualTo(historical.message());
            if (code != EngineError.EXECUTION_LEASE_LOST && code != EngineError.EXECUTION_RENEWAL_FAILED)
                assertThat(catalog.render(code.code(), params).solution()).isEqualTo(historical.solution());
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
