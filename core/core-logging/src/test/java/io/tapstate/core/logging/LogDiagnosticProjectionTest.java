package io.tapstate.core.logging;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class LogDiagnosticProjectionTest {
    @Test
    void normalCapturedDiagnosticAndNumericParametersRemainIntact() {
        Map<String, Object> input = Map.of("code", "28000", "codeSource", "jdbc-sqlstate",
                "rawMessage", "authentication failed", "params", Map.of("attempt", 2,
                        "offset", new BigInteger("12345678901234567890"), "ratio", new BigDecimal("0.125")));
        assertThat(LogDiagnosticProjection.fit(input)).isEqualTo(input);
    }

    @Test
    void largeNumbersCannotBypassTheAggregateBudget() {
        assertThat(LogDiagnosticProjection.fit(Map.of("params", Map.of("offset", BigInteger.ONE.shiftLeft(300000)))))
                .isEqualTo(Map.of("truncated", true));
    }

    @Test
    void arbitraryNumericMethodsCannotReplaceTheOriginalFailure() {
        var custom = new BigInteger("1") {
            @Override public int bitLength() { throw new IllegalStateException("must not be called"); }
            @Override public String toString() { throw new IllegalStateException("must not be called"); }
        };
        assertThat(LogDiagnosticProjection.fit(Map.of("params", Map.of("custom", custom))))
                .isEqualTo(Map.of("truncated", true));
    }

    @Test
    void escapingAndCauseBranchesShareOneBudget() {
        var params = new java.util.LinkedHashMap<String, Object>();
        for (int i = 0; i < 4; i++) params.put("field" + i, "\u0001".repeat(1000));
        var child = Map.<String, Object>of("params", params);
        assertThat(LogDiagnosticProjection.fit(Map.of("causes", java.util.List.of(child, Map.of("rawMessage", "safe")))))
                .containsKey("causes");
        assertThat(LogDiagnosticProjection.fit(Map.of("params", params, "cause", child)))
                .isEqualTo(Map.of("truncated", true));
    }
}
