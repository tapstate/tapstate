package io.tapstate.control.restapi;

import io.tapstate.messages.MessageCatalog;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ApiErrorHistoricalTextTest {
    @Test
    void emptyHistoricalFactsRenderWithoutPretendingTheParameterContractIsComplete() {
        var catalog = MessageCatalog.bundled();
        var diagnostic = Map.<String, Object>of("code", "TS0480004", "codeSource", "tapstate-standard");
        var messages = new java.util.HashSet<String>();
        for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
            var result = ApiError.render("TS0480004", Map.of(), diagnostic, catalog, locale);
            assertThat(result.code()).isEqualTo("TS0480004");
            assertThat(result.params()).isEmpty();
            assertThat(result.message()).isNotEqualTo("TS0480004").doesNotContain("{id}", "{field}");
            assertThat(result.solution()).isNotBlank();
            assertThat(result.diagnostic()).containsEntry("message", result.message())
                    .containsEntry("solution", result.solution()).doesNotContainKeys("params", "rawMessage");
            messages.add(result.message());
        }
        assertThat(messages).hasSize(3);
        assertThat(catalog.renderComplete("TS0480004", Map.of())).isEmpty();
        var partial = Map.<String, Object>of("code", "TS0480004", "codeSource", "tapstate-standard",
                "params", Map.of("id", "only-id"), "message", "original captured text");
        assertThat(ApiError.localizeDiagnostic(partial, catalog, Locale.SIMPLIFIED_CHINESE)).isEqualTo(partial);
        var other = Map.<String, Object>of("code", "TS0060027", "codeSource", "tapstate-standard",
                "message", "original captured text");
        assertThat(ApiError.localizeDiagnostic(other, catalog, Locale.SIMPLIFIED_CHINESE)).isEqualTo(other);
    }
}
