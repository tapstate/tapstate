package io.tapstate.control.core;

/** The selected server authentication context, never deserialized from operation input. */
@FunctionalInterface
public interface StorageControlCaller {
    StorageControlAccess current();
}
