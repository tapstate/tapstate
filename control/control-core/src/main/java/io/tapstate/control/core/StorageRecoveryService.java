package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.StorageRecoveryCommands;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Authenticated range confirmation; runtime actuation separately observes the original CONFIG intent. */
public final class StorageRecoveryService {
    /** Reads actual store relationships and registered source/target capabilities, without starting jobs. */
    public interface Inspector {
        StorageRecoveryCommands.Manifest inspect(CacheAuthScope scope, StorageRecoveryCommands.Selection selection);
        /** Rechecks the original influence/capability closure before every CONFIG transaction attempt. */
        void requireCurrent(CacheAuthScope scope, StorageRecoveryCommands.Manifest original);
    }
    private final StorageRecoveryCommands commands;
    private final Inspector inspector;
    private final AuditGate audit;
    private final java.util.function.Function<StorageRecoveryCommands.Plan,StorageRecoveryCommands.Plan> published;
    public StorageRecoveryService(StorageRecoveryCommands commands, Inspector inspector, AuditGate audit) {
        this(commands, inspector, audit, ignored -> {});
    }
    public StorageRecoveryService(StorageRecoveryCommands commands, Inspector inspector, AuditGate audit,
            java.util.function.Consumer<StorageRecoveryCommands.Plan> published) {
        this(commands, inspector, audit, (java.util.function.Function<StorageRecoveryCommands.Plan,StorageRecoveryCommands.Plan>) plan -> { published.accept(plan); return plan; });
    }
    public static StorageRecoveryService ownedPublication(StorageRecoveryCommands commands, Inspector inspector, AuditGate audit,
            java.util.function.Function<StorageRecoveryCommands.Plan,StorageRecoveryCommands.Plan> published) {
        return new StorageRecoveryService(commands, inspector, audit, published);
    }
    private StorageRecoveryService(StorageRecoveryCommands commands, Inspector inspector, AuditGate audit,
            java.util.function.Function<StorageRecoveryCommands.Plan,StorageRecoveryCommands.Plan> published) {
        this.commands = Objects.requireNonNull(commands, "commands"); this.inspector = Objects.requireNonNull(inspector, "inspector");
        this.audit = Objects.requireNonNull(audit, "audit");
        this.published = Objects.requireNonNull(published, "published");
    }
    public StorageRecoveryCommands.Plan plan(String subject, CacheAuthScope scope, Runnable active,
            StorageRecoveryCommands.Selection selection, String operationId, String previousOperationId) {
        active.run();
        if (operationId != null) {
            var original = commands.guardedBy(active).find(scope, operationId).orElseThrow(StorageRecoveryService::conflict);
            if (original.phase() == StorageRecoveryCommands.Phase.RUNNING || original.phase() == StorageRecoveryCommands.Phase.COMPLETE) {
                if (!original.manifest().selection().equals(selection)) throw conflict();
                return original;
            }
        }
        var manifest = inspector.inspect(scope, selection);
        Runnable current = () -> { active.run(); inspector.requireCurrent(scope, manifest); };
        return audit.dispatch(ControlOperations.STORAGE_RECOVERY_PLAN, new AuditContext(subject, scope.clusterId()),
                () -> commands.guardedBy(current).plan(scope, manifest, operationId, previousOperationId));
    }
    public StorageRecoveryCommands.Plan confirm(String subject, CacheAuthScope scope, Runnable active,
            String operationId, String planHash, long controlRevision, boolean explicitlyConfirmed) {
        active.run();
        var original = commands.guardedBy(active).find(scope, operationId).orElseThrow(StorageRecoveryService::conflict);
        if (!explicitlyConfirmed || !original.originalScope().equals(scope) || !original.planHash().equals(planHash)
                || original.controlRevision() != controlRevision) throw conflict();
        if (original.phase() != StorageRecoveryCommands.Phase.PLANNED) {
            return publish(original);
        }
        Runnable current = () -> { active.run(); inspector.requireCurrent(scope, original.manifest()); };
        var result = audit.dispatch(ControlOperations.STORAGE_RECOVERY_CONFIRM, new AuditContext(subject, operationId),
                () -> commands.guardedBy(current).confirm(original));
        return publish(result);
    }
    private StorageRecoveryCommands.Plan publish(StorageRecoveryCommands.Plan original) {
        var result = Objects.requireNonNull(published.apply(original), "published recovery result");
        if (!result.operationId().equals(original.operationId()) || !result.originalScope().equals(original.originalScope())
                || !Objects.equals(result.previousOperationId(), original.previousOperationId())
                || !result.manifest().equals(original.manifest()) || !result.planHash().equals(original.planHash())
                || result.controlRevision() != original.controlRevision() || result.phase().ordinal() < original.phase().ordinal())
            throw new IllegalStateException("recovery publication changed its original confirmation");
        return result;
    }
    public Optional<StorageRecoveryCommands.Plan> status(CacheAuthScope scope, Runnable active, String originalId) {
        active.run();
        return commands.guardedBy(active).find(scope, originalId);
    }
    private static TapstateException conflict() { return new TapstateException(ControlError.STORAGE_SCOPE_CONFLICT, Map.of(), null); }
}
