package io.tapstate.control.restapi;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Serves the host dependency manifest produced during server assembly. */
@RestController
class PdkVersionController {
    @Verb("system.pdk-version")
    @GetMapping(value = "/system/pdk-version", produces = MediaType.APPLICATION_JSON_VALUE)
    Resource pdkVersion() {
        return new ClassPathResource("META-INF/tapstate/pdk-version.json");
    }
}
