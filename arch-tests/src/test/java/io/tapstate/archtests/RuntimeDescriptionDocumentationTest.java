package io.tapstate.archtests;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeDescriptionDocumentationTest {

    @Test
    void runtimeDescriptionsDoNotRepeatObsoletePreviewClaims() throws IOException {
        Path repository = Path.of("..");
        String dockerfile = Files.readString(repository.resolve("deploy/docker/Dockerfile"));
        Matcher description = Pattern.compile("org\\.opencontainers\\.image\\.description=\"([^\"]+)\"")
                .matcher(dockerfile);
        assertThat(description.find()).as("the server image has an OCI description label").isTrue();

        Map<String, String> surfaces = new LinkedHashMap<>();
        surfaces.put("deploy/docker/Dockerfile OCI description", description.group(1));
        surfaces.put("docs/quickstart-online.md", Files.readString(repository.resolve("docs/quickstart-online.md")));
        surfaces.put("README.md", Files.readString(repository.resolve("README.md")));
        List<String> obsoleteClaims = List.of(
                "single-node, in-memory",
                "**Single node, in-memory.**",
                "Until the first release,",
                "Alpha is single-node");
        List<String> offenders = new ArrayList<>();
        surfaces.forEach((surface, text) -> {
            assertThat(text).as("%s is not empty", surface).isNotBlank();
            String normalized = text.replaceAll("\\s+", " ");
            for (String claim : obsoleteClaims) {
                if (normalized.contains(claim)) {
                    offenders.add(surface + ": " + claim);
                }
            }
        });

        // Source-specific restart and delivery guarantees need separate evidence before a rewrite.
        assertThat(offenders)
                .as("runtime descriptions must account for opt-in cluster mode, MongoDB control-plane state "
                        + "and published releases")
                .isEmpty();
    }
}
