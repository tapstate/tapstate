package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A context file written before projects binds directories under {@code workspaceBindings}. Upgrading
 * must not lose a single binding: the old key is read, and the file is written back under
 * {@code projectBindings} alone, so the next reader sees one key rather than two that could disagree.
 */
class LegacyBindingKeyIsReadTest {

    @Test
    void theOldKeyIsReadAndWrittenBackUnderTheNewOne(@TempDir Path home) throws IOException {
        Path project = Files.createDirectory(home.resolve("orders")).toRealPath();
        Path root = Files.createDirectory(home.resolve(".tapstate"));
        ownerOnly(root, true);
        Path config = root.resolve("config.yaml");
        Files.writeString(config, """
                version: 1
                lastContext: "dev"
                contexts:
                  "dev":
                    id: 018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2
                    seeds:
                      - "https://tapstate.example.com"
                    tls:
                      verify: true
                    authRef: 5c199643-04da-4f72-9831-3a77e3590eed
                workspaceBindings:
                  "%s": "dev"
                """.formatted(project), StandardCharsets.UTF_8);
        ownerOnly(config, false);
        ContextConfigStore store = ContextConfigStore.underHome(home);

        ContextConfig loaded = store.load();
        assertThat(loaded.projectBindings()).isEqualTo(Map.of(project.toString(), "dev"));

        store.save(loaded);
        String written = Files.readString(config);
        assertThat(written).contains("projectBindings:", "\"" + project + "\": \"dev\"")
                .doesNotContain("workspaceBindings");
        assertThat(store.load().projectBindings()).isEqualTo(Map.of(project.toString(), "dev"));
    }

    private static void ownerOnly(Path path, boolean directory) throws IOException {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            Set<PosixFilePermission> permissions = directory
                    ? Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                            PosixFilePermission.OWNER_EXECUTE)
                    : Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(path, permissions);
        }
    }
}
