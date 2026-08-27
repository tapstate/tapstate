package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContextConfigStoreTest {

    private static final UUID CONTEXT_ID = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
    private static final UUID AUTH_REF = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");

    @Test
    void roundTripsTheVersionedContextSchemaWithOwnerOnlyAtomicStorage(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders")).toRealPath();
        ContextConfigStore store = ContextConfigStore.underHome(home);
        ContextDefinition dev = new ContextDefinition(
                CONTEXT_ID,
                List.of(URI.create("https://tapstate.example.com"), URI.create("https://backup.example.com")),
                new ContextTls(true),
                AUTH_REF);
        ContextConfig expected = new ContextConfig(
                ContextConfig.CURRENT_VERSION,
                "dev",
                Map.of("dev", dev),
                Map.of(workspace.toString(), "dev"));

        store.save(expected);

        assertThat(store.load()).isEqualTo(expected);
        String yaml = Files.readString(home.resolve(".tapstate/config.yaml"));
        assertThat(yaml).contains(
                "version: 1",
                "lastContext: dev",
                "id: 018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2",
                "- https://tapstate.example.com",
                "verify: true",
                "authRef: 5c199643-04da-4f72-9831-3a77e3590eed",
                "\"" + workspace + "\": dev");
        if (Files.getFileStore(home).supportsFileAttributeView("posix")) {
            assertThat(Files.getPosixFilePermissions(home.resolve(".tapstate")))
                    .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                            PosixFilePermission.OWNER_EXECUTE));
            assertThat(Files.getPosixFilePermissions(home.resolve(".tapstate/config.yaml")))
                    .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
            assertThat(Files.getPosixFilePermissions(home.resolve(".tapstate/.config.lock")))
                    .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        }
        try (var files = Files.list(home.resolve(".tapstate"))) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactlyInAnyOrder(".config.lock", "config.yaml");
        }
    }

    @Test
    void refusesCorruptDuplicateUnknownAndSymlinkConfigsWithoutReplacingThem(@TempDir Path home) throws IOException {
        Path root = Files.createDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(root);
        Path config = root.resolve("config.yaml");
        ContextConfigStore store = ContextConfigStore.underHome(home);

        assertRejectedAndUnchanged(store, config, "version: [not yaml\n", "cli.context-config-invalid");
        assertRejectedAndUnchanged(store, config, "version: 1\nversion: 1\ncontexts: {}\nworkspaceBindings: {}\n",
                "cli.context-config-invalid");
        assertRejectedAndUnchanged(store, config, "version: 99\ncontexts: {}\nworkspaceBindings: {}\n",
                "cli.context-config-version");

        Files.delete(config);
        Path target = home.resolve("outside.yaml");
        Files.writeString(target, "outside", StandardCharsets.UTF_8);
        Files.createSymbolicLink(config, target);
        assertThatThrownBy(store::load)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-config-invalid"));
        assertThatThrownBy(() -> store.save(ContextConfig.empty()))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-config-invalid"));
        assertThat(Files.readString(target)).isEqualTo("outside");
    }

    @Test
    void invokesAnExplicitMigrationHookButFailsClosedWhenNoneSupportsTheVersion(@TempDir Path home) throws IOException {
        Path root = Files.createDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(root);
        Path config = root.resolve("config.yaml");
        Files.writeString(config, "version: 0\ncurrent: legacy\n", StandardCharsets.UTF_8);
        ownerOnlyFile(config);
        ContextConfig migrated = new ContextConfig(1, null,
                Map.of("legacy", new ContextDefinition(CONTEXT_ID,
                        List.of(URI.create("https://legacy.example.com")), new ContextTls(true), AUTH_REF)),
                Map.of());
        ContextConfigMigration migration = new ContextConfigMigration() {
            @Override
            public int sourceVersion() {
                return 0;
            }

            @Override
            public ContextConfig migrate(Map<String, Object> document) {
                assertThat(document).containsEntry("current", "legacy");
                return migrated;
            }
        };

        assertThat(ContextConfigStore.underHome(home, List.of(migration)).load()).isEqualTo(migrated);
        assertThatThrownBy(ContextConfigStore.underHome(home)::load)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-config-version"));
    }

    @Test
    void concurrentSavesAlwaysLeaveOneCompleteReadableDocument(@TempDir Path home) throws Exception {
        ContextConfigStore.underHome(home).save(ContextConfig.empty());
        ContextConfig alpha = configNamed("alpha", UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"));
        ContextConfig beta = configNamed("beta", UUID.fromString("118f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"));

        for (int attempt = 0; attempt < 100; attempt++) {
            saveConcurrently(home, alpha, beta);
        }

        assertThat(ContextConfigStore.underHome(home).load()).isIn(alpha, beta);
        try (var files = Files.list(home.resolve(".tapstate"))) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactlyInAnyOrder(".config.lock", "config.yaml");
        }
    }

    @Test
    void concurrentFirstSavesCreateTheSecureDirectoryWithoutAFalsePermissionFailure(@TempDir Path home) throws Exception {
        ContextConfig alpha = configNamed("alpha", UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"));
        ContextConfig beta = configNamed("beta", UUID.fromString("118f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"));

        for (int attempt = 0; attempt < 100; attempt++) {
            Path attemptHome = Files.createDirectory(home.resolve("home-" + attempt));

            saveConcurrently(attemptHome, alpha, beta);

            assertThat(ContextConfigStore.underHome(attemptHome).load()).isIn(alpha, beta);
            try (var files = Files.list(attemptHome.resolve(".tapstate"))) {
                assertThat(files.map(path -> path.getFileName().toString()).toList())
                        .containsExactlyInAnyOrder(".config.lock", "config.yaml");
            }
        }
    }

    @Test
    void ignoresCrashTempsAndRejectsBroadModesAndHardLinksBeforeParsing(@TempDir Path home) throws IOException {
        ContextConfigStore store = ContextConfigStore.underHome(home);
        ContextConfig expected = configNamed("dev", CONTEXT_ID);
        store.save(expected);
        Path root = home.resolve(".tapstate");
        Files.writeString(root.resolve(".config.yaml.tmp-crash"), "version: 99\n", StandardCharsets.UTF_8);
        assertThat(store.load()).isEqualTo(expected);

        Path config = root.resolve("config.yaml");
        if (Files.getFileStore(home).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(config, Set.of(PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE, PosixFilePermission.GROUP_READ));
            assertThatThrownBy(store::load)
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code().code()).isEqualTo("cli.context-config-permissions"));
            ownerOnlyFile(config);
        }

        Path hardLink = home.resolve("config-copy.yaml");
        try {
            Files.createLink(hardLink, config);
            assertThatThrownBy(store::load)
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code().code()).isEqualTo("cli.context-config-invalid"));
        } catch (UnsupportedOperationException unsupported) {
            // This filesystem cannot seed the hard-link case.
        }
    }

    @Test
    void rejectsSecretOrUnknownFieldsInsteadOfBestEffortLoading(@TempDir Path home) throws IOException {
        Path root = Files.createDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(root);
        Path config = root.resolve("config.yaml");
        String yaml = """
                version: 1
                contexts:
                  dev:
                    id: 018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2
                    seeds:
                      - https://tapstate.example.com
                    tls:
                      verify: true
                    authRef: 5c199643-04da-4f72-9831-3a77e3590eed
                    accessToken: forbidden
                workspaceBindings: {}
                """;
        Files.writeString(config, yaml, StandardCharsets.UTF_8);
        ownerOnlyFile(config);

        assertThatThrownBy(ContextConfigStore.underHome(home)::load)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-config-invalid"));
        assertThat(Files.readString(config)).isEqualTo(yaml);
    }

    private static ContextConfig configNamed(String name, UUID id) {
        return new ContextConfig(1, name,
                Map.of(name, new ContextDefinition(id, List.of(URI.create("https://" + name + ".example.com")),
                        new ContextTls(true), UUID.randomUUID())),
                Map.of());
    }

    private static void saveConcurrently(Path home, ContextConfig first, ContextConfig second) throws Exception {
        ContextConfigStore firstStore = ContextConfigStore.underHome(home);
        ContextConfigStore secondStore = ContextConfigStore.underHome(home);
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var firstSave = pool.submit(() -> {
                start.await();
                firstStore.save(first);
                return null;
            });
            var secondSave = pool.submit(() -> {
                start.await();
                secondStore.save(second);
                return null;
            });
            start.countDown();
            firstSave.get();
            secondSave.get();
        }
    }

    private static void assertRejectedAndUnchanged(
            ContextConfigStore store, Path config, String content, String expectedCode) throws IOException {
        Files.deleteIfExists(config);
        Files.writeString(config, content, StandardCharsets.UTF_8);
        ownerOnlyFile(config);
        assertThatThrownBy(store::load)
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo(expectedCode));
        assertThatThrownBy(() -> store.save(ContextConfig.empty()))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo(expectedCode));
        assertThat(Files.readString(config)).isEqualTo(content);
    }

    private static void ownerOnlyDirectory(Path path) throws IOException {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(path, Set.of(PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        }
    }

    private static void ownerOnlyFile(Path path) throws IOException {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(path,
                    Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        }
    }
}
