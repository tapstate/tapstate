package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;

import java.util.Map;

/** Future step-up/grant/audit gate for one Source config; production is deny-all in this release. */
@FunctionalInterface
public interface SourceConfigRevealAuthorizer {

    /** Must complete every authorization and pre-decryption audit obligation before returning. */
    void requireAuthorized(String principal, String sourceId, String oneTimeGrant);

    /** The only production policy shipped now: refuse before the artifact store can be read. */
    static SourceConfigRevealAuthorizer denyAll() {
        return (principal, sourceId, oneTimeGrant) -> {
            throw new TapstateException(ControlError.SOURCE_CONFIG_REVEAL_UNAVAILABLE, Map.of(), null);
        };
    }
}
