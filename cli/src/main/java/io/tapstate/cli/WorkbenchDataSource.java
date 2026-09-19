package io.tapstate.cli;

/** Loads one immutable workbench snapshot without rendering or mutating terminal state. */
@FunctionalInterface
interface WorkbenchDataSource {

    WorkbenchSnapshot load(
            long contextGeneration,
            long requestSequence,
            RefreshRequest.CancellationToken cancellationToken) throws Exception;
}
