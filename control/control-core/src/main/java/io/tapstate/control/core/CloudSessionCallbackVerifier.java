package io.tapstate.control.core;

import java.util.List;
import java.util.Map;

/** Trusted SDK binding for authenticated, scope-bound Cloud revocation callbacks; a jti is not proof. */
@FunctionalInterface
public interface CloudSessionCallbackVerifier {
    boolean verify(String issuer, String organizationId, String clusterId, String method, String path,
            Map<String, List<String>> headers, byte[] body);
}
