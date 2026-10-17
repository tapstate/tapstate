package io.tapstate.control.core;

/** Provider boundary for one-time code redemption. The returned JWT is untrusted until SDK verification. */
@FunctionalInterface
public interface CloudCodeExchanger {

    String exchange(String exchangeCode, String clusterId);

    /** Mutable address metadata is read separately from the one-time login exchange. */
    default java.util.Optional<CloudEgressAddress> currentEgressAddress(String clusterId) {
        return java.util.Optional.empty();
    }

    /** SDK-backed providers may also return context bound to this same redeemed code. */
    default CloudCodeExchangeResult exchangeWithContext(String exchangeCode, String clusterId) {
        return new CloudCodeExchangeResult(exchange(exchangeCode, clusterId), null);
    }
}
