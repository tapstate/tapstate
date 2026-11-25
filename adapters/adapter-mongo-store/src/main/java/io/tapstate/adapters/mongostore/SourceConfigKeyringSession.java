package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.WorkloadClaim;

import java.time.Duration;

/** Node-session lifecycle port that keeps keyring rotation membership current. */
public interface SourceConfigKeyringSession {

    void acknowledge(WorkloadClaim session, Duration ttl);

    void release(WorkloadClaim session);
}
