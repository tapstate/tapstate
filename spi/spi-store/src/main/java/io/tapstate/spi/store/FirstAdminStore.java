package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** Configuration-only first-administrator publication; transport authorization and audit run before this port. */
public interface FirstAdminStore {
    enum Phase { PENDING, COMPLETE }

    /** A small durable outcome, never a password, capability, or replacement for ordinary login. */
    record Status(StorageSetupScope scope, long bindingVersion, Phase phase, String username) {
        public Status {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(phase, "phase");
            if (bindingVersion < 1 || (phase == Phase.PENDING ? username != null : username == null || username.isBlank())) {
                throw new IllegalArgumentException("first administrator status has inconsistent fields");
            }
        }
    }

    /** Compose the original management authorization, expiry and readiness guard on every transaction attempt. */
    FirstAdminStore guardedBy(Runnable requireActive);

    /** Missing required completion or User facts are corruption, never permission to initialize again. */
    Optional<Status> load();

    /**
     * Insert the sole first admin and permanent completion with the authoring revision in one transaction.
     * A response lost after commit is resolved with load; resubmission never overwrites the account.
     */
    Status create(StorageSetupScope expected, User administrator);
}
