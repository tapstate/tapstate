package io.tapstate.spi.store;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/** The whole actual native data pool sampled by a node; it carries no admission verdict or deadline. */
public record ClusterNativeObservation(ClusterExecutionProfile profile,
        Map<String, ClusterNativeMember> membersByNode) {
    public ClusterNativeObservation {
        Objects.requireNonNull(profile, "profile");
        membersByNode = Map.copyOf(Objects.requireNonNull(membersByNode, "membersByNode"));
        var uuids = new HashSet<String>();
        var addresses = new HashSet<String>();
        if (membersByNode.isEmpty() || membersByNode.entrySet().stream().anyMatch(entry ->
                !entry.getKey().equals(entry.getValue().identity().nodeId())
                        || !uuids.add(entry.getValue().identity().memberUuid())
                        || !addresses.add(entry.getValue().memberAddress()))) {
            throw new IllegalArgumentException("a native observation requires unique identities and addresses for its complete pool");
        }
    }
}
