package io.tapstate.spi.store;

import java.util.Optional;

/** CONFIG-only publication of the exact confirmed shared scope, separate from its CACHE replacement transaction. */
public interface SharedSrsRecoveryPreparations {
    SharedSrsRecoveryPreparations guardedBy(Runnable requireActive);
    SharedSrsRecoveryPreparation reserve(StorageRecoveryCommands.Plan confirmed);
    Optional<SharedSrsRecoveryPreparation> pending(CacheAuthScope original,String operationId);
    StorageRecoveryCommands.Plan complete(SharedSrsRecoveryReceipt applied);
    StorageRecoveryCommands.Plan replan(SharedSrsRecoveryReceipt cancelled,StorageRecoveryCommands.Manifest verified);
    Optional<SharedSrsRecoveryPreparation> reconfirmation(CacheAuthScope original,String operationId);
    SharedSrsRecoveryPreparation reserve(StorageRecoveryCommands.Plan confirmed,SharedSrsRecoveryReceipt cancelled);
}
