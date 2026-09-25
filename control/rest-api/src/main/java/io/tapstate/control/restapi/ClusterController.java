package io.tapstate.control.restapi;

import io.tapstate.control.core.ClusterTopologyService;
import io.tapstate.control.core.ClusterTopologyView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The topology verb projected onto HTTP: {@code GET /api/cluster/members}.
 *
 * <p>Cluster membership is sensitive and must never be readable anonymously, which is why it is a
 * registered operation rather than a probe: being registered is what routes it behind the authentication
 * interceptor with everything else. The handler carries no guard of its own -- one written here would be
 * a second answer to a question the interceptor already answers for every verb, and the two would drift.
 *
 * <p>A thin pass-through, like the other reads. Nothing is mutated, so nothing is audited and no caller
 * principal is named; every node answers the same, because the service joins what the engine sees with
 * what the cluster has committed and both are equally available from any member.
 */
@RestController
class ClusterController {

    private final ClusterTopologyService topology;

    ClusterController(ClusterTopologyService topology) {
        this.topology = topology;
    }

    @Verb("cluster.members")
    @GetMapping("/cluster/members")
    ClusterTopologyView members() {
        return topology.topology();
    }
}
