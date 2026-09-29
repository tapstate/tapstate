package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** The control endpoint this member advertises to the rest of its cluster. */
@ConfigurationProperties(prefix = "tapstate.control")
class ControlEndpointProperties {

    private String advertiseUrl;

    String getAdvertiseUrl() {
        return advertiseUrl;
    }

    void setAdvertiseUrl(String advertiseUrl) {
        this.advertiseUrl = advertiseUrl;
    }
}
