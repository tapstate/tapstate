package io.tapstate.control.restapi;

import io.tapstate.control.core.StorageControlAccess;
import jakarta.servlet.http.HttpServletRequest;

/** Supplied only by the OP assembly that owns the actual dedicated server connector. */
public interface InstallationHttpAccess {
    default java.util.Map<String,Object> initializationProgress() { return java.util.Map.of(); }
    default String normalLoginUrl() { return null; }
    default boolean automaticAuthorization() { return false; }
    boolean isManagementRequest(HttpServletRequest request);
    void requireManagementRequest(HttpServletRequest request);
    StorageControlAccess caller(HttpServletRequest request);
    void createAdministrator(HttpServletRequest request, String username, String password);
}
