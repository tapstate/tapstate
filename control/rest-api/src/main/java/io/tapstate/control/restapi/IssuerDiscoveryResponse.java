package io.tapstate.control.restapi;

import java.util.List;

/** Anonymous stable cluster identity returned before a client sends any cached credential. */
record IssuerDiscoveryResponse(String issuer, String clusterId, String apiVersion, List<String> authModes) {

    IssuerDiscoveryResponse {
        authModes = List.copyOf(authModes);
    }
}
