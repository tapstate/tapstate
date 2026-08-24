package io.tapstate.cli;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthFileStoreTest {

    private static final UUID AUTH_REF = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
    private static final UUID CONTEXT_ID = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
    private static final Instant CREATED = Instant.parse("2026-08-17T10:00:00Z");
    private static final AuthSessionRecord RECORD = new AuthSessionRecord(
            1,
            AUTH_REF,
            CONTEXT_ID,
            "urn:tapstate:cluster:01J5FIXTURE",
            "admin",
            List.of("read", "write", "admin"),
            "tss_s01.session-secret",
            CREATED,
            CREATED.plusSeconds(30L * 24 * 60 * 60),
            CREATED.plusSeconds(90L * 24 * 60 * 60));

    @Test
    void roundTripsOnlyTheOpaqueSessionSchemaWithOwnerOnlyAtomicStorage(@TempDir Path home) throws IOException {
        AuthFileStore store = AuthFileStore.underHome(home);

        assertThat(store.save(RECORD, false)).isEqualTo(AuthFileStore.SaveResult.PERSISTED);

        Path authDir = home.resolve(".tapstate/auth");
        Path authFile = authDir.resolve(AUTH_REF + ".json");
        assertThat(store.load(AUTH_REF, CONTEXT_ID)).contains(RECORD);
        Map<?, ?> json = (Map<?, ?>) JsonReader.parse(Files.readString(authFile));
        assertThat(json.keySet().stream().map(String::valueOf).toList()).containsExactly(
                "version", "authRef", "contextId", "issuer", "principal", "scopes", "sessionToken",
                "createdAt", "idleExpiresAt", "absoluteExpiresAt");
        assertThat(json.get("sessionToken")).isEqualTo("tss_s01.session-secret");
        assertThat(json.toString()).doesNotContain("password", "accessToken", "jwt-access-token");
        if (Files.getFileStore(home).supportsFileAttributeView("posix")) {
            assertThat(Files.getPosixFilePermissions(authDir)).isEqualTo(Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE));
            assertThat(Files.getPosixFilePermissions(authFile)).isEqualTo(Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE));
        }
        try (var files = Files.list(authDir)) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactly(AUTH_REF + ".json");
        }
    }

    @Test
    void rejectsCorruptDuplicateUnknownAndMismatchedAuthFilesWithoutReplacingThem(@TempDir Path home)
            throws IOException {
        AuthFileStore store = AuthFileStore.underHome(home);
        Path authDir = Files.createDirectories(home.resolve(".tapstate/auth"));
        ownerOnlyDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(authDir);
        Path authFile = authDir.resolve(AUTH_REF + ".json");

        assertRejectedAndUnchanged(store, authFile, "{\"version\":", "cli.auth-cache-invalid");
        assertRejectedAndUnchanged(store, authFile,
                "{\"version\":1,\"version\":1,\"authRef\":\"" + AUTH_REF + "\"}",
                "cli.auth-cache-invalid");
        assertRejectedAndUnchanged(store, authFile, validJson().replace("\"version\":1", "\"version\":99"),
                "cli.auth-cache-version");
        assertRejectedAndUnchanged(store, authFile, validJson().replace("\"version\":1", "\"version\":1.5"),
                "cli.auth-cache-invalid");
        assertRejectedAndUnchanged(store, authFile, validJson().replace("\"version\":1", "\"version\":1e0"),
                "cli.auth-cache-invalid");
        assertRejectedAndUnchanged(store, authFile, validJson().replace(CONTEXT_ID.toString(),
                "118f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"), "cli.auth-cache-invalid");

        String withAccessToken = validJson().replace("\"createdAt\"",
                "\"accessToken\":\"jwt-access-token\",\"createdAt\"");
        assertRejectedAndUnchanged(store, authFile, withAccessToken, "cli.auth-cache-invalid");
    }

    @Test
    void doesNotEchoOpaqueSessionMaterialFromAnInvalidCacheDocument(@TempDir Path home) throws IOException {
        AuthFileStore store = AuthFileStore.underHome(home);
        Path authDir = Files.createDirectories(home.resolve(".tapstate/auth"));
        ownerOnlyDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(authDir);
        Path authFile = authDir.resolve(AUTH_REF + ".json");
        String injectedSession = "tss_s01.session-secret";
        Files.writeString(authFile, validJson().replace(AUTH_REF.toString(), injectedSession), StandardCharsets.UTF_8);
        ownerOnlyFile(authFile);

        assertThatThrownBy(() -> store.load(AUTH_REF, CONTEXT_ID))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.getMessage()).doesNotContain(injectedSession));
    }

    @Test
    void rejectsSymlinkBroadModeAndHardLinkBeforeParsing(@TempDir Path home) throws IOException {
        AuthFileStore store = AuthFileStore.underHome(home);
        store.save(RECORD, false);
        Path authDir = home.resolve(".tapstate/auth");
        Path authFile = authDir.resolve(AUTH_REF + ".json");
        Files.writeString(authDir.resolve(".auth.tmp-crash"), "{\"version\":99}", StandardCharsets.UTF_8);
        assertThat(store.load(AUTH_REF, CONTEXT_ID)).contains(RECORD);

        if (Files.getFileStore(home).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(authFile, Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_READ));
            assertThatThrownBy(() -> store.load(AUTH_REF, CONTEXT_ID))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code().code()).isEqualTo("cli.auth-cache-permissions"));
            ownerOnlyFile(authFile);
        }

        Path hardLink = home.resolve("auth-copy.json");
        try {
            Files.createLink(hardLink, authFile);
            assertThatThrownBy(() -> store.load(AUTH_REF, CONTEXT_ID))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code().code()).isEqualTo("cli.auth-cache-invalid"));
        } catch (UnsupportedOperationException unsupported) {
            // This filesystem cannot seed the hard-link case.
        }

        Files.delete(authFile);
        Path outside = home.resolve("outside.json");
        Files.writeString(outside, validJson(), StandardCharsets.UTF_8);
        Files.createSymbolicLink(authFile, outside);
        assertThatThrownBy(() -> store.load(AUTH_REF, CONTEXT_ID))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.auth-cache-invalid"));
        assertThatThrownBy(() -> store.save(RECORD, false))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.auth-cache-invalid"));
        assertThat(Files.readString(outside)).isEqualTo(validJson());
    }

    @Test
    void ttyFallsBackToMemoryOnlyWhenPersistencePermissionsCannotBeProved(@TempDir Path home) throws IOException {
        Path authDir = Files.createDirectories(home.resolve(".tapstate/auth"));
        ownerOnlyDirectory(home.resolve(".tapstate"));
        if (Files.getFileStore(home).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(authDir, Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ));
        }
        AuthFileStore store = AuthFileStore.underHome(home);

        assertThat(store.save(RECORD, true)).isEqualTo(AuthFileStore.SaveResult.MEMORY_ONLY);
        assertThatThrownBy(() -> store.save(RECORD, false))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.auth-cache-permissions"));
    }

    @Test
    void restoresThePreviousAuthFileBeforeReportingMemoryOnlyAfterPostReplaceSyncFailure(@TempDir Path home)
            throws IOException {
        Path authDir = Files.createDirectories(home.resolve(".tapstate/auth")).toAbsolutePath();
        ownerOnlyDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(authDir);
        AuthSessionRecord previous = new AuthSessionRecord(
                RECORD.version(), RECORD.authRef(), RECORD.contextId(), RECORD.issuer(), RECORD.principal(),
                RECORD.scopes(), "tss_s00.previous-session", RECORD.createdAt(), RECORD.idleExpiresAt(),
                RECORD.absoluteExpiresAt());
        assertThat(AuthFileStore.underHome(home).save(previous, false))
                .isEqualTo(AuthFileStore.SaveResult.PERSISTED);
        AtomicInteger syncAttempts = new AtomicInteger();
        AuthFileStore store = AuthFileStore.underHome(home, directory -> {
            if (directory.equals(authDir) && syncAttempts.getAndIncrement() == 0) {
                throw new IOException("injected post-replace sync failure");
            }
        });

        assertThat(store.save(RECORD, true)).isEqualTo(AuthFileStore.SaveResult.MEMORY_ONLY);
        assertThat(store.load(AUTH_REF, CONTEXT_ID)).contains(previous);
        try (var files = Files.list(authDir)) {
            assertThat(files.map(path -> path.getFileName().toString()).toList())
                    .containsExactly(AUTH_REF + ".json");
        }
        assertThat(syncAttempts).hasValue(2);
    }

    @Test
    void doesNotClaimMemoryOnlyWhenReplacementCleanupCannotBeConfirmed(@TempDir Path home)
            throws IOException {
        Path authDir = Files.createDirectories(home.resolve(".tapstate/auth")).toAbsolutePath();
        ownerOnlyDirectory(home.resolve(".tapstate"));
        ownerOnlyDirectory(authDir);
        AuthFileStore store = AuthFileStore.underHome(home, directory -> {
            if (directory.equals(authDir)) {
                throw new IOException("injected directory sync failure");
            }
        });

        assertThatThrownBy(() -> store.save(RECORD, true))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.auth-cache-permissions"));
        assertThat(authDir.resolve(AUTH_REF + ".json")).doesNotExist();
    }

    private static void assertRejectedAndUnchanged(
            AuthFileStore store, Path authFile, String content, String expectedCode) throws IOException {
        Files.deleteIfExists(authFile);
        Files.writeString(authFile, content, StandardCharsets.UTF_8);
        ownerOnlyFile(authFile);
        assertThatThrownBy(() -> store.load(AUTH_REF, CONTEXT_ID))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo(expectedCode));
        assertThatThrownBy(() -> store.save(RECORD, false))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo(expectedCode));
        assertThat(Files.readString(authFile)).isEqualTo(content);
    }

    private static String validJson() {
        return """
                {"version":1,"authRef":"5c199643-04da-4f72-9831-3a77e3590eed","contextId":"018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2","issuer":"urn:tapstate:cluster:01J5FIXTURE","principal":"admin","scopes":["read","write","admin"],"sessionToken":"tss_s01.session-secret","createdAt":"2026-08-17T10:00:00Z","idleExpiresAt":"2026-09-16T10:00:00Z","absoluteExpiresAt":"2026-11-15T10:00:00Z"}
                """.strip();
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
