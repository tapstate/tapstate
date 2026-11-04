package io.tapstate.spi.store;

/** Formal storage checks using the same normalized settings as the permanent storage client. */
public interface StorageConnectionProbe {
    record CacheTestResult(boolean empty, String suggestedDatabase) {
        public static CacheTestResult emptyDatabase() { return new CacheTestResult(true, null); }
        public static CacheTestResult occupied(String suggestedDatabase) {
            return new CacheTestResult(false, suggestedDatabase);
        }
    }

    StorageBinding.Connection normalize(StorageBinding.Connection candidate);
    /** Checks local syntax and policy only, without DNS, clients or connector initialization. */
    default StorageBinding.Connection validate(StorageBinding.Connection candidate) { return candidate; }
    /** Actual transactional CRUD and index privileges; temporary probe work is always aborted and closed. */
    void test(StorageBinding.Connection normalized);

    /** Checks a fresh CACHE candidate; the suggestion is present only when its name was verified empty. */
    default CacheTestResult testCache(StorageBinding.Connection normalized) {
        test(normalized);
        return CacheTestResult.emptyDatabase();
    }
}
