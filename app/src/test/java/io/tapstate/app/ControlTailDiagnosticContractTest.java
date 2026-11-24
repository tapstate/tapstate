package io.tapstate.app;

import io.tapstate.core.common.Severity;
import io.tapstate.control.core.*;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.core.event.RowValueError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ControlTailDiagnosticContractTest {
    @Test void remainingDefinitionsKeepParametersSeverityAndHistoricalRecovery() {
        var codes = new ArrayList<TapstateErrorCode>();
        codes.addAll(List.of(ClusterError.values()));
        codes.addAll(List.of(ConnectorCatalogError.values()));
        codes.addAll(List.of(DataBrowserError.values()));
        codes.addAll(List.of(MonitorError.values()));
        codes.addAll(List.of(PositionError.values()));
        codes.addAll(List.of(MigrationError.values()));
        codes.addAll(List.of(RowValueError.values()));
        assertThat(codes).hasSize(22);
        var catalog = MessageCatalog.bundled();
        for (var code : codes) {
            String domain = code instanceof ClusterError ? "cluster" : code instanceof ConnectorCatalogError ? "connector"
                    : code instanceof DataBrowserError ? "data-browser" : code instanceof MonitorError ? "monitor"
                    : code instanceof PositionError ? "position" : "migration";
            String legacy = code instanceof RowValueError ? "core.row-map-key-unsupported"
                    : domain + "." + ((Enum<?>) code).name().toLowerCase(Locale.ROOT).replace('_', '-');
            var params = new HashMap<String, Object>();
            code.placeholders().forEach(name -> params.put(name, "value-" + name));
            assertThat(code.severity()).isEqualTo(Severity.ERROR);
            assertThat(code.code()).matches("TS(?:03[789]|04[0123])(?!0000)[0-9]{4}");
            assertThat(catalog.render(code.code(), params)).isEqualTo(catalog.render(legacy, params));
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
