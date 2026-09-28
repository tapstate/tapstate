package io.tapstate.control.core;

/** Provider boundary for one-time code redemption. The returned JWT is untrusted until online validation. */
@FunctionalInterface
public interface CloudCodeExchanger {

    String exchange(String exchangeCode, String clusterId);
}
