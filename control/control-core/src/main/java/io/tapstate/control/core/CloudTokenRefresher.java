package io.tapstate.control.core;

import java.util.Optional;

/** Provider boundary for renewing an expiring Cloud user credential. */
@FunctionalInterface
public interface CloudTokenRefresher {

    Optional<CloudUserToken> refresh(CloudUserToken current);
}
