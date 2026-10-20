package io.tapstate.spi.store;

/** Formal storage checks using the same normalized settings as the permanent storage client. */
public interface StorageConnectionProbe {
    StorageBinding.Connection normalize(StorageBinding.Connection candidate);
    /** Actual transactional CRUD and index privileges; returns a physical-database digest when server identity can be established. */
    java.util.Optional<String> test(StorageBinding.Connection normalized);
}
