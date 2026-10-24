package io.tapstate.control.core;

import io.tapstate.core.common.JsonReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class AiDslGuidanceTest {
    @Test void bundlesEveryContractFileWithVerifiableProvenance() throws Exception {
        Map<?, ?> manifest = (Map<?, ?>) JsonReader.parse(
                new String(resource("manifest.json"), StandardCharsets.UTF_8));
        assertThat((String) manifest.get("revision")).matches("[0-9a-f]{40}");
        Map<?, ?> files = (Map<?, ?>) manifest.get("files");
        assertThat(files.size()).isEqualTo(4);
        for (var entry : files.entrySet()) {
            String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(resource((String) entry.getKey())));
            assertThat(actual).isEqualTo(((Map<?, ?>) entry.getValue()).get("sha256"));
        }
        Map<?, ?> schema = (Map<?, ?>) JsonReader.parse(
                new String(resource("tapstate-v1.schema.json"), StandardCharsets.UTF_8));
        assertThat(schema).isNotEmpty();
    }

    @Test void includesDslSemanticsWithoutHistoricalRuntimeClaims() {
        assertThat(AiDslGuidance.text()).contains("### Filter", "### Map", "### JavaScript",
                "### Union", "### Nest", "### Join", "after.field", "tool schema")
                .doesNotContain("a03aa7265be765b9a48dd2464e2e987f24ada60e");
    }

    private static byte[] resource(String name) throws Exception {
        try (var stream = AiDslGuidance.class.getResourceAsStream("/ai/dsl/" + name)) {
            assertThat(stream).as(name).isNotNull();
            return stream.readAllBytes();
        }
    }
}
