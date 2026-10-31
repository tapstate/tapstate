package io.tapstate.control.core;

import java.util.Map;

/** Authenticated alert operations bound to the original admitted business context. */
@FunctionalInterface
public interface AlertControlDispatcher {
    Map<String, Object> dispatch(String operation, CredentialAuthentication caller, Map<String, Object> input);
}
