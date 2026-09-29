package io.tapstate.control.core;

/**
 * One member as the engine currently sees it, before anything is decided about what it means.
 *
 * <p>Two identities, and they answer different questions. {@code nodeId} is the one that survives a
 * restart -- it is what a claim names as an owner and what an operator calls that node. {@code memberUuid}
 * and {@code bootId} belong to this run of that process: they are how a restarted node is told apart from
 * the one it replaced, which is exactly what a reader chasing "did it come back" needs.
 *
 * <p>Every field may be null on a member that carries no Tapstate identity -- a plain engine member, or
 * one started before identity was configured. Null says the member did not say, which is not the same as
 * an empty string, and a reader that prints one for the other reports a node with no name.
 */
public record LiveClusterMember(
        String nodeId, String memberUuid, String bootId, String hzAddress, String controlUrl) {
}
