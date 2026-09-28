package io.tapstate.control.core;

/** Provider boundary for the one-time browser code to Cloud user-token exchange. */
@FunctionalInterface
public interface CloudCodeExchanger {

    CloudUserToken exchange(String exchangeCode, String clusterId);
}
