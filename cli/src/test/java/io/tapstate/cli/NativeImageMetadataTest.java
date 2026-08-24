package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies reflection metadata for command models created outside the generated Picocli tree. */
class NativeImageMetadataTest {

    @Test
    void launchOptionsRemainReflectiveInTheNativeImage() throws IOException {
        InputStream metadata = NativeImageMetadataTest.class.getResourceAsStream(
                "/META-INF/native-image/io.tapstate/cli/reflect-config.json");

        assertThat(metadata).as("LaunchOptions native reflection metadata").isNotNull();
        try (metadata) {
            String json = new String(metadata.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(json).contains("\"name\" : \"io.tapstate.cli.LaunchOptions\"")
                    .contains("\"name\" : \"connect\"")
                    .contains("\"name\" : \"context\"")
                    .contains("\"name\" : \"user\"")
                    .contains("\"name\" : \"password\"")
                    .contains("\"name\" : \"workdir\"")
                    .contains("\"name\" : \"command\"");
        }
    }
}
