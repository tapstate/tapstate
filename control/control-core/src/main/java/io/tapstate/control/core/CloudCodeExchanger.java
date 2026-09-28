package io.tapstate.control.core;

/** Provider boundary for one-time code redemption. The returned JWT is untrusted until SDK verification. */
@FunctionalInterface
public interface CloudCodeExchanger {

    String exchange(String exchangeCode, String clusterId);
}
