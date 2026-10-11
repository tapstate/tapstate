package io.tapstate.spi.store;

import java.util.Objects;

/** A complete native member identity and the actual address under which it joined. */
public record ClusterNativeMember(ClusterExecutionMember identity, String memberAddress) {
    public ClusterNativeMember {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(memberAddress, "memberAddress");
        if (memberAddress.isBlank()) {
            throw new IllegalArgumentException("a native member requires its actual address");
        }
    }
}
