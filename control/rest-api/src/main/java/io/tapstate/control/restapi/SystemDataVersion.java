package io.tapstate.control.restapi;

/**
 * What schema version the system data this process is running against is at.
 *
 * <p>An interface rather than a value because the answer lives on the other side of a boundary this
 * ring may not cross: the number comes off the store, and reaching a store is the adapter ring's job.
 * The assembly root, which is allowed to see both, supplies one of these.
 *
 * <p>A run with no verified configuration store reports absence rather than zero. A provider may
 * become known after same-process setup; it must not invent a version before the store is opened.
 */
@FunctionalInterface
public interface SystemDataVersion {

    /** The opened configuration store's schema version, or null when no such store is available yet. */
    Integer current();
}
