package io.tapstate.control.restapi;

import io.tapstate.control.core.ClusterTopologyService;
import io.tapstate.control.core.LiveClusterMember;
import io.tapstate.control.core.LiveClusterMembers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * A two-member cluster for the HTTP contract cases. The engine is faked because what these cases are
 * about is the wire: which fields cross it, and who is allowed to ask. Whether the engine's own member
 * list is read correctly is a question for the layer that reads it, over a real member.
 */
@Configuration
class ClusterTopologyTestConfiguration {

    static final LiveClusterMember FIRST = new LiveClusterMember(
            "node-a", "8b0a1e6e-0000-4000-8000-00000000000a", "boot-a1",
            "[127.0.0.1]:5701", "https://node-a.example:8443");

    static final LiveClusterMember SECOND = new LiveClusterMember(
            "node-b", "8b0a1e6e-0000-4000-8000-00000000000b", "boot-b1",
            "[127.0.0.1]:5702", "https://node-b.example:8443");

    @Bean
    ClusterTopologyService clusterTopologyService() {
        LiveClusterMembers members = () -> List.of(SECOND, FIRST);
        return new ClusterTopologyService(members, null, "01J5AUTHFIXTURE");
    }
}
