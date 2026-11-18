package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectorCenterService;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

/** HTTP projection of the connector catalog and local installation operations. */
@RestController
final class ConnectorCenterController {
    private final ObjectProvider<ConnectorCenterService> services;

    ConnectorCenterController(ObjectProvider<ConnectorCenterService> services) {
        this.services = services;
    }

    @Verb("connector-center.list")
    @GetMapping("/connector-center")
    Map<String, Object> list() {
        return service().catalog();
    }

    @Verb("connector-center.refresh")
    @PostMapping("/connector-center:refresh")
    Map<String, Object> refresh() {
        return service().refresh(AuthenticatedCaller.subject());
    }

    @Verb("connector-center.download")
    @PostMapping("/connector-center/{id}:download")
    Map<String, Object> download(@PathVariable("id") String id) {
        MalformedRequest.requireText(id, "a connector id is required");
        return service().download(id, AuthenticatedCaller.subject());
    }

    @Verb("connector-center.operation")
    @GetMapping("/connector-center/operations/{operationId}")
    Map<String, Object> operation(@PathVariable("operationId") String operationId) {
        MalformedRequest.requireText(operationId, "an installation operation id is required");
        return service().operation(operationId);
    }

    @Verb("connector-center.upload")
    @PostMapping(value = "/connector-center:upload", consumes = {"application/java-archive", "application/octet-stream"})
    Map<String, Object> upload(HttpServletRequest request) {
        var service = service();
        String principal = AuthenticatedCaller.subject();
        try {
            return service.upload(request.getInputStream(), request.getContentLengthLong(), principal);
        } catch (IOException unreadable) {
            throw MalformedRequest.rejecting("the connector artifact could not be read", null);
        }
    }

    private ConnectorCenterService service() {
        var service = services.getIfAvailable();
        if (service == null) throw new TapstateException(ControlError.UNREACHABLE, Map.of(), null);
        return service;
    }
}
