package io.tapstate.control.core;

/** Optional diagnostic events contain only closed classifications, never authentication inputs. */
public interface CloudAuthenticationObserver {

    CloudAuthenticationObserver NONE = new CloudAuthenticationObserver() { };

    String REQUEST_ID_CONTEXT_KEY = "cloud_request_id";
    String ERROR_CODE_CONTEXT_KEY = "cloud_error_code";

    enum Stage { CODE_EXCHANGE, JWT_VERIFICATION, SESSION_CREATE }

    enum SessionRejection { DEPLOYMENT_MISMATCH, JWT_EXPIRED, ADMIN_SCOPE, DUPLICATE_OR_REVOKED_JTI }

    default void entering(Stage stage) { }

    default void sessionRejected(SessionRejection reason) { }
}
