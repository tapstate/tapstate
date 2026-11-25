package io.tapstate.control.core;

/** Trusted SDK binding for authenticated, scope-bound Cloud revocation callbacks; a jti is not proof. */
@FunctionalInterface
public interface CloudSessionCallbackVerifier {
    boolean verify(String issuer, String organizationId, String clusterId, String method,
            String timestamp, String nonce, String data, String signature);
}
