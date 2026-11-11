package io.tapstate.app;

import io.tapstate.spi.store.ContentHash;
import org.bson.Document;

import java.nio.charset.StandardCharsets;

/** Fingerprint of the persisted keyring used by restart witnesses. */
final class KeyringFingerprint {
    private KeyringFingerprint() {}

    static String of(Document keyring) {
        Document stable = new Document(keyring);
        // Protected writes may advance this fence without replacing the persisted key.
        stable.remove("sourceWriteFence");
        return ContentHash.of(stable.toJson().getBytes(StandardCharsets.UTF_8));
    }
}
