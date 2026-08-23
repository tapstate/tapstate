package io.tapstate.control.restapi;

import io.tapstate.control.core.ClusterIdentityView;
import io.tapstate.control.core.ClusterIdentityService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

/** Anonymous stable identity discovery performed before a client presents a cached credential. */
@Controller
class IssuerDiscoveryController {

    private final ObjectProvider<ClusterIdentityService> identities;

    IssuerDiscoveryController(ObjectProvider<ClusterIdentityService> identities) {
        this.identities = identities;
    }

    @GetMapping(AuthWire.DISCOVERY_PATH)
    ResponseEntity<IssuerDiscoveryResponse> discover() {
        ClusterIdentityView identity = identities.getObject().identityView();
        return ResponseEntity.ok(new IssuerDiscoveryResponse(
                identity.issuer(),
                identity.clusterId(),
                "tapstate/v1",
                List.of("password", "machine_token")));
    }
}
