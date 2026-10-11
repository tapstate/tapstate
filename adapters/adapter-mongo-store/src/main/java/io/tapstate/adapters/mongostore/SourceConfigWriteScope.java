package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;

import java.util.Objects;

/** Adapter-private transaction scope; no driver type crosses the keyring handle's public signature. */
final class SourceConfigWriteScope {
    private final ClientSession session;

    SourceConfigWriteScope(ClientSession session) {
        this.session = Objects.requireNonNull(session, "session");
    }

    boolean fence(SourceConfigKeyringStore store, String keyId) {
        return store.fenceActiveWriter(session, keyId);
    }
}
