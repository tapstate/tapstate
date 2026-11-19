package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectorCenterService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ConnectorUsageQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestParam;
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
    private ConnectorUsageQuery usage;

    @Autowired(required = false)
    void connectorUsage(ConnectorUsageQuery usage) { this.usage = usage; }

    ConnectorCenterController(ObjectProvider<ConnectorCenterService> services) {
        this.services = services;
    }

    @Verb("connector-center.list")
    @GetMapping("/connector-center")
    Map<String, Object> list() {
        return service().catalog();
    }

    @Verb("connector-center.list")
    @GetMapping("/connector-center/{id}/versions/{sha256}/usages")
    ConnectorUsageQuery.Page usages(@PathVariable("id") String id, @PathVariable("sha256") String sha256,
            @RequestParam(value = "search", required = false) String search,
            @RequestParam(value = "page", defaultValue = "1") int page,
            @RequestParam(value = "pageSize", defaultValue = "10") int pageSize) {
        MalformedRequest.requireText(id, "a connector id is required");
        if (!sha256.matches("[a-f0-9]{64}") || page < 1 || pageSize < 1 || pageSize > 100)
            throw MalformedRequest.rejecting("a lowercase SHA-256, positive page and pageSize between 1 and 100 are required", null);
        service();
        return usage == null ? new ConnectorUsageQuery.Page(page, pageSize, 0, java.util.List.of())
                : usage.usages(id, sha256, search, page, pageSize);
    }

    @Verb("connector-center.refresh")
    @PostMapping("/connector-center:refresh")
    Map<String, Object> refresh() {
        return service().refresh(AuthenticatedCaller.subject());
    }

    @Verb("connector-center.download")
    @PostMapping("/connector-center/{id}:download")
    Map<String, Object> download(@PathVariable("id") String id,
            @RequestParam(name = "sha256", required = false) String expectedSha256) {
        MalformedRequest.requireText(id, "a connector id is required");
        if (expectedSha256 != null && !expectedSha256.matches("[a-f0-9]{64}"))
            throw MalformedRequest.rejecting("a lowercase SHA-256 is required", null);
        return service().download(id, expectedSha256, AuthenticatedCaller.subject());
    }

    @Verb("connector-center.operation")
    @GetMapping("/connector-center/operations/{operationId}")
    Map<String, Object> operation(@PathVariable("operationId") String operationId) {
        MalformedRequest.requireText(operationId, "an installation operation id is required");
        return service().operation(operationId);
    }

    @Verb("connector-center.upload")
    @PostMapping(value = "/connector-center:upload", consumes = {"application/java-archive", "application/octet-stream"})
    Map<String, Object> upload(HttpServletRequest request,
            @RequestParam(name = "connectorId", required = false) String expectedConnectorId) {
        if (expectedConnectorId != null) MalformedRequest.requireText(expectedConnectorId, "a connector id is required");
        var service = service();
        String principal = AuthenticatedCaller.subject();
        try {
            return service.upload(request.getInputStream(), request.getContentLengthLong(), expectedConnectorId, principal);
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
