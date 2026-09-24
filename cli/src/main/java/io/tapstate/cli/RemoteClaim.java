package io.tapstate.cli;

/**
 * One workload claim as read back from the server. This mirrors the server's shape independently
 * (rule R6: the CLI carries no shared control type).
 *
 * @param resourceId          what the claim is over
 * @param ownerNodeId         the stable node holding it, which is what makes it readable after the
 *                            process that took it is gone
 * @param ownerBootId         the boot of that node which took it
 * @param claimGeneration     rises when ownership changes hands, or null when the server did not say
 * @param executionGeneration rises when the owner starts a new run, or null when the server did not say
 * @param leased              whether the store still considered it owned, or null when the server did
 *                            not say -- which is not the same as saying it is not
 */
record RemoteClaim(
        String resourceId,
        String ownerNodeId,
        String ownerBootId,
        Long claimGeneration,
        Long executionGeneration,
        Boolean leased) {
}
