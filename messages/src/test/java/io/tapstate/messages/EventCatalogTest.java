package io.tapstate.messages;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EventCatalogTest {

    @Test
    void everyPublicKindHasAStableDisplayMessageAndFailuresUseTheErrorCatalog() {
        EventCatalog catalog = EventCatalog.bundled();
        for (String kind : List.of("state-changed", "failure", "execution-restarted",
                "execution-recovered", "telemetry-degraded", "telemetry-restored",
                "cleanup-incomplete", "telemetry-gap")) {
            String key = "event." + kind;
            assertThat(catalog.render(key, Map.of())).as(key).isNotBlank().isNotEqualTo(key);
        }
        Map<String, Object> params = Map.of("pipeline", "orders", "cause", "sink refused the batch");
        assertThat(catalog.render("engine.job-failed", params))
                .isEqualTo(MessageCatalog.bundled().render("engine.job-failed", params).message());
    }
}
