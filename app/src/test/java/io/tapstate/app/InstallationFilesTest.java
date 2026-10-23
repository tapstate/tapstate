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

    @Test void oneRuntimeOwnsTheInstallationWhileOwnerCliReissueRemainsAvailable() {
        var files = InstallationFiles.initialize(temporary.resolve("runtime-owner"));
        var other = new InstallationFiles(files.directory());
        try (var runtime = files.ownRuntime()) {
            runtime.requireActive();
            assertThatThrownBy(other::ownRuntime).isInstanceOf(TapstateException.class);
            var issued = InstallationCommand.authorize(other, new MongoProperties(), java.time.Clock.systemUTC());
            assertThat(files.capability()).contains(issued);
            runtime.requireActive();
        }
        try (var successor = other.ownRuntime()) { successor.requireActive(); }
    }

    @Test void ownerFileReadsCannotReleaseTheRuntimeLockToAnotherJvm() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("runtime-process"));
        try (var owner = files.ownRuntime()) {
            files.stage(); files.saveStage(files.stage());
            owner.requireActive();
            assertThat(runtimeProbe(files)).contains("REFUSED").doesNotContain("ACQUIRED");
        }
        assertThat(runtimeProbe(files)).contains("ACQUIRED");
    }

    @Test void interruptingAFileReaderCannotReleaseLiveRuntimeOwnership() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("interrupted-reader"));
        var owner = files.ownRuntime();
        try {
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            Thread reader = Thread.ofPlatform().start(() -> {
                Thread.currentThread().interrupt();
                failure.set(catchThrowable(files::stage));
            });
            reader.join(5000); assertThat(reader.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(TapstateException.class);
            assertThat(runtimeProbe(files)).contains("REFUSED").doesNotContain("ACQUIRED");
            owner.requireActive();
        } finally { try { owner.close(); } catch (TapstateException alreadyLost) { /* The acquisition assertion above discriminates the defect. */ } }
    }

    @Test void interruptingAWaitForAnotherProcessMutationKeepsRuntimeOwnership() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("interrupted-wait"));
        try (var owner = files.ownRuntime()) {
            var blocker = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx64m", "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), InstallationRuntimeProbe.class.getName(), files.directory().toString(), "mutation")
                    .redirectErrorStream(true).start();
            Thread reader = null;
            try (var lines = new java.io.BufferedReader(new java.io.InputStreamReader(blocker.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
                assertThat(lines.readLine()).isEqualTo("MUTATION_HELD");
                var entered = new CountDownLatch(1); var done = new CountDownLatch(1);
                var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                reader = Thread.ofPlatform().start(() -> { entered.countDown(); failure.set(catchThrowable(files::stage)); done.countDown(); });
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(done.await(100, TimeUnit.MILLISECONDS)).isFalse();
                reader.interrupt(); assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(failure.get()).isInstanceOf(TapstateException.class);
                assertThat(runtimeProbe(files)).contains("REFUSED").doesNotContain("ACQUIRED"); owner.requireActive();
            } finally {
                blocker.getOutputStream().close();
                if (!blocker.waitFor(5, TimeUnit.SECONDS)) { blocker.destroy(); if (!blocker.waitFor(5, TimeUnit.SECONDS)) blocker.destroyForcibly(); }
                if (reader != null && reader.isAlive()) { reader.interrupt(); reader.join(5000); }
            }
        }
    }

    private String runtimeProbe(InstallationFiles files) throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx64m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), InstallationRuntimeProbe.class.getName(), files.directory().toString())
                .redirectErrorStream(true).start();
        try {
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).isZero();
            return new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } finally { if (process.isAlive()) { process.destroy(); if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly(); } }
    }
}
