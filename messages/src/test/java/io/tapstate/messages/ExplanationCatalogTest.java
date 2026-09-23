package io.tapstate.messages;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExplanationCatalogTest {

    private static final ExplanationCatalog EN = ExplanationCatalog.bundled();

    @Test
    void rendersExplainTextWithNamedArguments() {
        assertThat(EN.render("explain.reconcile-failures", Map.of("count", 3)))
                .contains("3 passes")
                .doesNotContain("{");
    }

    @Test
    void carriedFailureCodesUseTheErrorCatalog() {
        assertThat(EN.render("monitor.no-observation", Map.of("pipeline", "orders")))
                .contains("orders")
                .doesNotContain("monitor.no-observation");
    }
}
