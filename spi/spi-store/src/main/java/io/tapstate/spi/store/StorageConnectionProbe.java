package io.tapstate.spi.store;

/** Formal storage checks using the same normalized settings as the permanent storage client. */
public interface StorageConnectionProbe {
    StorageBinding.Connection normalize(StorageBinding.Connection candidate);
    /** Checks local syntax and policy only, without DNS, clients or connector initialization. */
    default StorageBinding.Connection validate(StorageBinding.Connection candidate) { return candidate; }
    /** Actual transactional CRUD and index privileges; temporary probe work is always aborted and closed. */
    void test(StorageBinding.Connection normalized);
}
