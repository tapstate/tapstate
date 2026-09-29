package io.tapstate.adapters.mongostore;

/** Supplies the cached keyring for reads and a freshly verified keyring for each Source write. */
interface SourceConfigCipherProvider {

    SourceConfigCipher current();

    SourceConfigCipher refresh();

    default SourceConfigCipher currentForWrite() {
        return refresh();
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
        };
    }
}
