package io.tapstate.app;

import io.tapstate.control.core.SetupCapabilityService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class InstallationFilesTest {
    @TempDir Path temporary;

    @Test void missingOrExistingUnknownInstallationNeverBecomesFreshImplicitly() throws Exception {
        Path directory = temporary.resolve("owner");
        assertThatThrownBy(() -> new InstallationFiles(directory)).isInstanceOf(TapstateException.class);
        Files.createDirectory(directory);
        assertThatThrownBy(() -> InstallationFiles.initialize(directory)).isInstanceOf(TapstateException.class);
        try (var paths = Files.list(directory)) { assertThat(paths.toList()).isEmpty(); }
    }

    @Test void recordsAreSeparateOwnerOnlyAndSurviveOpeningAnotherOwnerHandle() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("owner"));
        var initial = files.stage();
        var issued = new SetupCapabilityService.Issued(initial.scope(), StorageSetupStateStore.Purpose.SETUP,
                Instant.now().plusSeconds(900), "b".repeat(43));
        files.saveCapability(issued);
        files.saveStage(initial.withHandoff(InstallationFiles.Handoff.INTENT, "a".repeat(64)));
        files.saveConnection("mongodb://sentinel:secret@localhost/config", "certificate-material");
        var reopened = new InstallationFiles(files.directory());
        assertThat(reopened.stage().scope()).isEqualTo(initial.scope());
        assertThat(reopened.capability()).contains(issued);
        assertThat(reopened.configurationUri()).contains("mongodb://sentinel:secret@localhost/config");
        assertThat(Files.readString(files.directory().resolve("stage.json"))).doesNotContain("sentinel", "secret", issued.token(), "certificate-material");
        assertThat(Files.readString(files.directory().resolve("configuration.uri"))).isEqualTo("mongodb://sentinel:secret@localhost/config");
        try (var paths = Files.list(files.directory())) {
            for (Path path : paths.toList()) assertThat(Files.getPosixFilePermissions(path)).isEqualTo(PosixFilePermissions.fromString("rw-------"));
        }
        assertThatThrownBy(() -> reopened.saveConnection("mongodb://other/config", null)).isInstanceOf(TapstateException.class);
        reopened.removeCapability();
        assertThat(reopened.capability()).isEmpty();
        assertThat(reopened.stage().handoff()).isEqualTo(InstallationFiles.Handoff.INTENT);
    }

    @Test void missingStageAndSymbolicOrHardLinkedFilesAreRefusedWithoutTouchingTheirTargets() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("owner"));
        Path stage = files.directory().resolve("stage.json");
        Path original = temporary.resolve("original");
        Files.move(stage, original);
        assertThatThrownBy(files::stage).isInstanceOf(TapstateException.class);
        Files.createSymbolicLink(stage, original);
        assertThatThrownBy(files::stage).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> files.saveStage(new InstallationFiles.Stage(
                new io.tapstate.spi.store.StorageSetupScope("cluster", "installation", "operation", 1), InstallationFiles.Handoff.LOCAL, null, null)))
                .isInstanceOf(TapstateException.class);
        Files.delete(stage); Files.createLink(stage, original);
        assertThatThrownBy(files::stage).isInstanceOf(TapstateException.class);
        assertThat(Files.size(original)).isPositive();
    }

    @Test void broadPermissionsOrMalformedAuthorityRemainClosed() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("owner"));
        Path stage = files.directory().resolve("stage.json");
        Files.setPosixFilePermissions(stage, PosixFilePermissions.fromString("rw-r--r--"));
        assertThatThrownBy(files::stage).isInstanceOf(TapstateException.class);
        Files.setPosixFilePermissions(stage, PosixFilePermissions.fromString("rw-------"));
        Files.writeString(stage, "{\"formatVersion\":1}");
        assertThatThrownBy(files::stage).isInstanceOf(TapstateException.class);
        Files.setPosixFilePermissions(files.directory(), PosixFilePermissions.fromString("rwxr-xr-x"));
        assertThatThrownBy(files::requireOwner).isInstanceOf(TapstateException.class);
    }

    @Test void independentOwnerHandlesSerializeReadModifyWriteAndAllowNestedGuards() throws Exception {
        var first = InstallationFiles.initialize(temporary.resolve("owner"));
        var second = new InstallationFiles(first.directory());
        try (var executor = Executors.newFixedThreadPool(2)) {
            var tasks = java.util.List.of(first, second).stream().map(files -> executor.submit(() -> {
                for (int i = 0; i < 20; i++) files.locked(() -> {
                    var stage = files.stage(); var scope = stage.scope();
                    files.saveStage(stage.withScope(new io.tapstate.spi.store.StorageSetupScope(scope.clusterId(), scope.installationId(), scope.operationId(), scope.generation() + 1)));
                    return null;
                });
            })).toList();
            for (var task : tasks) task.get(10, TimeUnit.SECONDS);
        }
        assertThat(first.stage().scope().generation()).isEqualTo(41);
    }

    @Test void competingConnectionPublicationsCannotMixUriAndTrust() throws Exception {
        for (int attempt = 0; attempt < 12; attempt++) {
            var first = InstallationFiles.initialize(temporary.resolve("connection-" + attempt));
            var second = new InstallationFiles(first.directory());
            var start = new CountDownLatch(1);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var a = executor.submit(() -> { start.await(); return catchThrowable(() -> first.saveConnection("mongodb://a/config", "ca-a")); });
                var b = executor.submit(() -> { start.await(); return catchThrowable(() -> second.saveConnection("mongodb://b/config", "ca-b")); });
                start.countDown();
                var failures = java.util.stream.Stream.of(a.get(5, TimeUnit.SECONDS), b.get(5, TimeUnit.SECONDS)).filter(java.util.Objects::nonNull).toList();
                assertThat(failures).singleElement().isInstanceOf(TapstateException.class);
                assertThat(first.configurationCa()).contains(first.configurationUri().orElseThrow().contains("//a/") ? "ca-a" : "ca-b");
            }
        }
    }

    @Test void malformedCapabilityExpiryIsACodedRefusal() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("bad-expiry"));
        var issued = new SetupCapabilityService.Issued(files.stage().scope(), StorageSetupStateStore.Purpose.SETUP,
                Instant.now().plusSeconds(900), "b".repeat(43));
        files.saveCapability(issued);
        var path = files.directory().resolve("capability.json");
        Files.writeString(path, Files.readString(path).replace(issued.expiresAt().toString(), "not-an-instant"));
        assertThatThrownBy(files::capability).isInstanceOf(TapstateException.class);
    }
}
