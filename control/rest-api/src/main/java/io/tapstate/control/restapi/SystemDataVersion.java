package io.tapstate.control.restapi;

/**
 * What schema version the system data this process is running against is at.
 *
 * <p>An interface rather than a value because the answer lives on the other side of a boundary this
 * ring may not cross: the number comes off the store, and reaching a store is the adapter ring's job.
 * The assembly root, which is allowed to see both, supplies one of these.
 *
 * <p>A run with no store configured has no implementation of this, and the version it reports is
 * absent rather than zero — a client has to be able to tell "there is no store" from "the store has
 * never been migrated", and zero is the second of those.
 */
@FunctionalInterface
public interface SystemDataVersion {

    /** The version the store this process opened is at. */
    int current();
}
