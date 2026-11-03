package io.tapstate.adapters.mongostore;

/** Supplies the cached keyring for reads and a freshly verified keyring for each Source write. */
interface SourceConfigCipherProvider {

    SourceConfigCipher current();

    SourceConfigCipher refresh();

    /** New bounded operations must not fall back to an unbounded legacy key read. */
    default SourceConfigCipher refresh(MongoReadBudget budget) {
        throw new io.tapstate.core.common.TapstateException(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY,
                java.util.Map.of(), null);
    }

    default SourceConfigCipher currentForWrite() {
        return refresh();
    }

    default boolean requiresWriteFence() {
        return false;
    }

    default boolean fenceWrite(SourceConfigWriteScope scope, String keyId) {
        return true;
    }

    static SourceConfigCipherProvider fixed(SourceConfigCipher cipher) {
        return new SourceConfigCipherProvider() {
            @Override
            public SourceConfigCipher current() {
                return cipher;
            }

            @Override
            public SourceConfigCipher refresh() {
                return cipher;
            }

            @Override public SourceConfigCipher refresh(MongoReadBudget budget) {
                budget.requireRemaining(); return cipher;
            }
        };
    }
}
