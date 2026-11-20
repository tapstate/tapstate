package io.tapstate.app;

import io.tapstate.core.common.Severity;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ConnectorDiagnosticContractTest {
    @Test void remainingDefinitionsKeepParametersSeverityAndHistoricalRecovery() {
        var codes = new ArrayList<TapstateErrorCode>();
        codes.addAll(List.of(ConnectorError.values()));
        assertThat(codes).hasSize(42);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            String legacy = "connector." + ((Enum<?>) code).name().toLowerCase(Locale.ROOT).replace('_', '-');
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            assertThat(code.severity()).isEqualTo(Severity.ERROR);
            assertThat(code.code()).matches("TS006(?!0000)[0-9]{4}");
            if (code == ConnectorError.DISCOVERY_AUTHENTICATION_REJECTED) assertThat(code.code()).isEqualTo("TS0060042");
            else if (code == ConnectorError.DISCOVERY_CONNECTION_TIMEOUT) assertThat(code.code()).isEqualTo("TS0060041");
            else if (code == ConnectorError.DISCOVER_FAILED) {
                assertThat(catalog.render(legacy, params).message()).contains("value-detail");
                assertThat(catalog.render(code.code(), params).message()).doesNotContain("value-detail");
                assertThat(catalog.render(code.code(), params).solution())
                        .isEqualTo("Check that the source database is reachable and the connection settings are correct, then refresh the schema.");
            } else if (code == ConnectorError.WRITE_FAILED) {
                assertThat(catalog.render(legacy, params).message()).contains("value-detail");
                assertThat(catalog.render(code.code(), params).message()).doesNotContain("value-detail");
                assertThat(catalog.render(code.code(), params).solution()).contains("technical details");
            } else if (code != ConnectorError.TEST_FAILED)
                assertThat(catalog.render(code.code(), params)).isEqualTo(catalog.render(legacy, params));
            else assertThat(code.code()).isEqualTo("TS0060001");
            var messages = new HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var text = catalog.render(code.code(), params, locale);
                assertThat(text.message()).isNotBlank().doesNotContain(code.code());
                assertThat(text.solution()).isNotBlank();
                messages.add(text.message());
                code.placeholders().stream().filter(name -> (code != ConnectorError.DISCOVER_FAILED && code != ConnectorError.WRITE_FAILED) || !name.equals("detail"))
                        .forEach(name -> assertThat(text.message() + text.solution()).contains("value-" + name));
            }
            assertThat(messages).hasSize(3);
        }
    }
}
