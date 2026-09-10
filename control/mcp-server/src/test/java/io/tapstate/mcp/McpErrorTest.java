package io.tapstate.mcp;

import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class McpErrorTest {

    @Test
    void errorsExposeStableSeverityCodesAndPlaceholders() {
        assertThat(McpError.SERVER_REJECTED.code()).isEqualTo("mcp.server-rejected");
        assertThat(McpError.SERVER_REJECTED.severity()).isEqualTo(Severity.ERROR);
        assertThat(McpError.CONNECTOR_SPEC_UNAVAILABLE.placeholders())
                .containsExactly("connector");
        assertThat(McpError.INVALID_SERVER_RESPONSE.placeholders())
                .containsExactly("operation");
    }
}
