package io.tapstate.adapters.pdk;

import io.tapstate.core.common.Domain;
import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code connector} error domain: the first-party, diagnosable failures the PDK bridge raises
 * when it loads, level-gates or projects a connector. These are allocated numeric product
 * codes, structurally distinct from the reserved {@code connector.<connector-id>.<symbol>} namespace a
 * connector's own thrown codes occupy.
 */
class ConnectorErrorTest {

    @Test
    void everyCodeIsUnderTheRegisteredConnectorDomain() {
        for (ConnectorError e : ConnectorError.values()) {
            assertThat(e.code()).matches("TS006(?!0000)[0-9]{4}");
            assertThat(Domain.isRegistered("connector")).isTrue();
        }
    }

    @Test
    void codesAreTheExpectedCanonicalStrings() {
        assertThat(ConnectorError.LOAD_FAILED.code()).isEqualTo("TS0060009");
        assertThat(ConnectorError.CLASS_NOT_FOUND.code()).isEqualTo("TS0060012");
        assertThat(ConnectorError.LOGMINER_IDENTIFIER_TOO_LONG.code())
                .isEqualTo("TS0060019");
        assertThat(ConnectorError.API_LEVEL_INCOMPATIBLE.code()).isEqualTo("TS0060021");
        assertThat(ConnectorError.API_LEVEL_UNKNOWN.code()).isEqualTo("TS0060022");
        assertThat(ConnectorError.PROJECTION_FAILED.code()).isEqualTo("TS0060023");
        assertThat(ConnectorError.CAPTURE_FAILED.code()).isEqualTo("TS0060024");
        assertThat(ConnectorError.TEST_FAILED.code()).isEqualTo("TS0060001");
        assertThat(ConnectorError.DISCOVER_FAILED.code()).isEqualTo("TS0060026");
        assertThat(ConnectorError.WRITE_FAILED.code()).isEqualTo("TS0060027");
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
