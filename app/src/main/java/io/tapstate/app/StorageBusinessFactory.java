package io.tapstate.app;

/** Registers a candidate runtime's resources before their controlled startup. */
@FunctionalInterface
interface StorageBusinessFactory {
    StorageRuntime.Business create(StorageRuntime.Bound original);
}
