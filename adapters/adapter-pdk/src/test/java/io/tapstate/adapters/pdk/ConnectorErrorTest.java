package io.tapstate.adapters.pdk;

import io.tapstate.core.common.Domain;
import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code connector} error domain: the first-party, diagnosable failures the PDK bridge raises
 * when it loads, level-gates or projects a connector. These are two-segment {@code connector.<symbol>}
 * codes, structurally distinct from the reserved {@code connector.<connector-id>.<symbol>} namespace a
 * connector's own thrown codes occupy.
 */
class ConnectorErrorTest {

    @Test
    void everyCodeIsUnderTheRegisteredConnectorDomain() {
        for (ConnectorError e : ConnectorError.values()) {
            assertThat(e.code()).startsWith("connector.");
            String symbol = e.code().substring("connector.".length());
            assertThat(symbol).doesNotContain(".");
            assertThat(Domain.isRegistered("connector")).isTrue();
        }
    }

    @Test
    void codesAreTheExpectedCanonicalStrings() {
        assertThat(ConnectorError.LOAD_FAILED.code()).isEqualTo("connector.load-failed");
        assertThat(ConnectorError.CLASS_NOT_FOUND.code()).isEqualTo("connector.class-not-found");
        assertThat(ConnectorError.LOGMINER_IDENTIFIER_TOO_LONG.code())
                .isEqualTo("connector.logminer-identifier-too-long");
        assertThat(ConnectorError.API_LEVEL_INCOMPATIBLE.code()).isEqualTo("connector.api-level-incompatible");
        assertThat(ConnectorError.API_LEVEL_UNKNOWN.code()).isEqualTo("connector.api-level-unknown");
        assertThat(ConnectorError.PROJECTION_FAILED.code()).isEqualTo("connector.projection-failed");
        assertThat(ConnectorError.CAPTURE_FAILED.code()).isEqualTo("connector.capture-failed");
        assertThat(ConnectorError.TEST_FAILED.code()).isEqualTo("connector.test-failed");
        assertThat(ConnectorError.DISCOVER_FAILED.code()).isEqualTo("connector.discover-failed");
        assertThat(ConnectorError.WRITE_FAILED.code()).isEqualTo("connector.write-failed");
    }

    @Test
    void placeholdersAreDeclaredPerCode() {
        assertThat(ConnectorError.LOAD_FAILED.placeholders()).isEqualTo(Set.of("connector"));
        assertThat(ConnectorError.CLASS_NOT_FOUND.placeholders()).isEqualTo(Set.of("connector", "class"));
        assertThat(ConnectorError.LOGMINER_IDENTIFIER_TOO_LONG.placeholders())
                .isEqualTo(Set.of("connector", "kind", "identifier", "limit"));
        assertThat(ConnectorError.API_LEVEL_INCOMPATIBLE.placeholders())
                .isEqualTo(Set.of("connector", "required", "provided"));
        assertThat(ConnectorError.API_LEVEL_UNKNOWN.placeholders()).isEqualTo(Set.of("connector", "version"));
        assertThat(ConnectorError.PROJECTION_FAILED.placeholders()).isEqualTo(Set.of("connector", "detail"));
        assertThat(ConnectorError.CAPTURE_FAILED.placeholders()).isEqualTo(Set.of("connector", "detail"));
        assertThat(ConnectorError.TEST_FAILED.placeholders()).isEqualTo(Set.of("connector", "detail"));
        assertThat(ConnectorError.DISCOVER_FAILED.placeholders()).isEqualTo(Set.of("connector", "detail"));
        assertThat(ConnectorError.WRITE_FAILED.placeholders()).isEqualTo(Set.of("connector", "detail"));
    }

    @Test
    void everyConnectorErrorIsAnError() {
        for (ConnectorError e : ConnectorError.values()) {
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }
}
