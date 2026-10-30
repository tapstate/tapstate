package io.tapstate.app;

import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** The local authority yields to CONFIG once creation starts; a failed CONFIG read never falls back. */
final class InstallationSetupStates implements StorageSetupStateStore {
    private final InstallationFiles files;
    private final Supplier<Optional<StorageSetupStateStore>> configuration;
    private final Runnable active;

    InstallationSetupStates(InstallationFiles files, Supplier<Optional<StorageSetupStateStore>> configuration) {
        this(files, configuration, () -> {});
    }
    private InstallationSetupStates(InstallationFiles files, Supplier<Optional<StorageSetupStateStore>> configuration, Runnable active) {
        this.files = Objects.requireNonNull(files); this.configuration = Objects.requireNonNull(configuration); this.active = Objects.requireNonNull(active);
    }
    @Override public StorageSetupStateStore guardedBy(Runnable guard) {
        return new InstallationSetupStates(files, configuration, () -> { active.run(); guard.run(); });
    }
    @Override public Status load() {
        active.run(); var store = configuration.get();
        if (store.isPresent()) {
            var status = store.orElseThrow().guardedBy(active).load();
            requireRetainedGeneration(status.scope()); active.run(); return status;
        }
        return files.locked(() -> { var stage = files.stage(); requireLocal(stage); active.run(); return new Status(stage.scope(), Phase.LOCAL_SETUP, 0); });
    }
    @Override public Status rotate(StorageSetupScope expected, Purpose purpose) {
        active.run(); var store = configuration.get();
        if (store.isPresent()) {
            requireRetainedGeneration(expected);
            // Never hold the file lock while competing with a final CONFIG transaction. Its
            // authorization callbacks also read these files, so the opposite order can deadlock.
            var result = store.orElseThrow().guardedBy(active).rotate(expected, purpose);
            files.locked(() -> {
                var stage = files.stage(); requireOriginal(stage.scope(), result.scope()); requireGenerationFloor(stage.scope(), result.scope());
                files.saveStage(stage.withScope(result.scope())); return null;
            });
            active.run(); return result;
        }
        return files.locked(() -> {
            active.run(); var stage = files.stage(); requireLocal(stage);
            if (purpose != Purpose.SETUP || !stage.scope().equals(expected) || expected.generation() >= Long.MAX_VALUE - 1) throw InstallationFiles.refused();
            var result = new Status(new StorageSetupScope(expected.clusterId(), expected.installationId(), expected.operationId(), expected.generation() + 1), Phase.LOCAL_SETUP, 0);
            files.saveStage(stage.withScope(result.scope()));
            active.run(); return result;
        });
    }
    @Override public void requireCurrent(StorageSetupScope expected, Purpose purpose) {
        active.run(); var store = configuration.get();
        if (store.isPresent()) {
            requireRetainedGeneration(expected);
            store.orElseThrow().guardedBy(active).requireCurrent(expected, purpose);
            requireRetainedGeneration(expected); active.run(); return;
        }
        files.locked(() -> {
            active.run(); var stage = files.stage();
            requireOriginal(stage.scope(), expected);
            requireGenerationFloor(stage.scope(), expected);
            requireLocal(stage);
            if (purpose != Purpose.SETUP || !stage.scope().equals(expected)) throw InstallationFiles.refused();
            active.run(); return null;
        });
    }
    private void requireRetainedGeneration(StorageSetupScope expected) {
        var retained = files.stage().scope(); requireOriginal(retained, expected); requireGenerationFloor(retained, expected);
    }
    private void requireLocal(InstallationFiles.Stage stage) {
        if (stage.handoff().ordinal() >= InstallationFiles.Handoff.CREATE_STARTED.ordinal()) throw StorageRuntime.unavailable();
        // Missing a previously published connection file is unknown, never an empty installation.
        if (stage.handoff() == InstallationFiles.Handoff.URI_WRITTEN && files.configurationUri().isEmpty()) throw StorageRuntime.unavailable();
        if (stage.handoff() == InstallationFiles.Handoff.LOCAL && files.configurationUri().isPresent()) throw StorageRuntime.unavailable();
    }
    static void requireOriginal(StorageSetupScope original, StorageSetupScope actual) {
        if (!original.clusterId().equals(actual.clusterId()) || !original.installationId().equals(actual.installationId())
                || !original.operationId().equals(actual.operationId())) throw InstallationFiles.refused();
    }
    private static void requireGenerationFloor(StorageSetupScope local, StorageSetupScope expected) {
        if (expected.generation() < local.generation()) throw InstallationFiles.refused();
    }
}
