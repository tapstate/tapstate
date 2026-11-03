package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectionTestReport;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.SchemaReport;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The connection-plane verbs projected onto HTTP: the two synchronous control-to-runtime operations the
 * whitelist opens — the connection test and the schema discovery. Each decodes the connection it targets,
 * drives it through its control-core service — which runs the runtime probe under the audit gate and
 * persists the result / the discovered model as the connection's latest — and returns the surface report.
 * A thin projection with no business logic of its own; the operations are audited to the authenticated
 * caller, read from the guard rather than trusted from the request body. The services speak control-ring
 * types, so this face never reaches into the storage ports.
 *
 * <p>Their read peers project {@code GET /api/connections/{id}/test-result} and
 * {@code GET /api/connections/{id}/schema}: the connection's latest persisted result / discovered model,
 * or a 404 when it has never been tested / discovered.
 */
@RestController
class ConnectionController {

    private final ConnectionTestService testService;
    private final ConnectionTestResultQueryService resultQueryService;
    private final SchemaDiscoveryService schemaDiscoveryService;
    private final SchemaQueryService schemaQueryService;

    ConnectionController(
            ConnectionTestService testService,
            ConnectionTestResultQueryService resultQueryService,
            SchemaDiscoveryService schemaDiscoveryService,
            SchemaQueryService schemaQueryService) {
        this.testService = testService;
        this.resultQueryService = resultQueryService;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.schemaQueryService = schemaQueryService;
    }

    @Verb("connection.test")
    @PostMapping("/connections:test")
    ConnectionTestReport test(@RequestBody(required = false) ConnectionTestRequest request) {
        // Refuse a missing / blank-field body at the boundary as a coded 400, rather than letting a null trip
        // the connection-config invariant guard deeper down (a 500).
        ConnectionTestRequest body =
                MalformedRequest.require(request, "the request must carry a connection to test");
        MalformedRequest.requireText(body.id(), "a connection `id` is required");
        MalformedRequest.requireText(body.connectorId(), "a `connectorId` is required");
        try { return testService.test(body.id(), body.connectorId(), body.settings(), AuthenticatedCaller.subject()); }
        catch (io.tapstate.core.common.TapstateException failure) {
            if ("connector.instances-busy".equals(failure.code().code())) throw new UnavailableCodedException(failure);
            throw failure;
        }
    }

    @Verb("connection.test-result")
    @GetMapping("/connections/{id}/test-result")
    ResponseEntity<ConnectionTestReport> testResult(@PathVariable("id") String id) {
        // ResponseEntity.of maps a stored result to 200 and a never-tested connection to 404, with no error
        // logic here. The path variable is named explicitly: this build compiles without -parameters, so an
        // inferred name would not resolve at runtime.
        return ResponseEntity.of(resultQueryService.find(id));
    }

    @Verb("connection.discover-schema")
    @PostMapping("/connections:discover-schema")
    SchemaReport discoverSchema(@RequestBody(required = false) ConnectionDiscoverSchemaRequest request) {
        // Refuse a missing / blank-field body at the boundary as a coded 400, rather than letting a null trip
        // the connection-config invariant guard deeper down (a 500).
        ConnectionDiscoverSchemaRequest body =
                MalformedRequest.require(request, "the request must carry a connection to discover");
        MalformedRequest.requireText(body.id(), "a connection `id` is required");
        MalformedRequest.requireText(body.connectorId(), "a `connectorId` is required");
        return schemaDiscoveryService.discover(
                body.id(), body.connectorId(), body.settings(), AuthenticatedCaller.subject());
    }

    @Verb("connection.schema")
    @GetMapping("/connections/{id}/schema")
    ResponseEntity<SchemaReport> schema(@PathVariable("id") String id) {
        // ResponseEntity.of maps a stored model to 200 and a never-discovered connection to 404; the path
        // variable is named explicitly for the same -parameters reason as above.
        return ResponseEntity.of(schemaQueryService.find(id));
    }
}
