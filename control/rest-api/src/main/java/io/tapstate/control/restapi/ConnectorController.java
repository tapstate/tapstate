package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ConnectorDetail;
import io.tapstate.control.core.ConnectorIcon;
import io.tapstate.control.core.ConnectorRegisterService;
import io.tapstate.control.core.ConnectorRegistrationReport;
import io.tapstate.core.common.TapstateException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.Base64;

/**
 * The connector register verb projected onto HTTP: it decodes the uploaded artifact from the request
 * body, drives it through the control-core register service — which ingests it into the distribution
 * store under the audit gate — and returns the surface report. A thin projection with no business logic
 * of its own; the operation is audited to the authenticated caller, read from the guard rather than
 * trusted from the request body. The service speaks control-ring types, so this face never reaches into
 * the storage ports.
 *
 * <p>The artifact arrives base64-encoded in the JSON body, since the CLI shares no filesystem with the
 * server. A missing body / field or non-base64 artifact is refused at the boundary as a coded 400; a bad
 * artifact that decodes but does not load, declares no connector or identity, or collides with a
 * registered id surfaces the register service's coded connector-domain refusal.
 *
 * <p>The read peer lists only connectors registered in this deployment, so every authoring candidate has
 * a stored normalized row and spec source. Registration becomes visible without a restart because the
 * catalog view re-reads derived catalog state for each call.
 */
@RestController
class ConnectorController {

    private final ConnectorRegisterService registerService;
    private final ConnectorCatalogView catalogView;

    ConnectorController(ConnectorRegisterService registerService, ConnectorCatalogView catalogView) {
        this.registerService = registerService;
        this.catalogView = catalogView;
    }

    @Verb("connector.register")
    @PostMapping("/connectors:register")
    ConnectorRegistrationReport register(@RequestBody(required = false) ConnectorRegisterRequest request) {
        // Refuse a missing / blank-field body at the boundary as a coded 400, rather than letting a null or
        // undecodable artifact trip a bare guard deeper down (a 500).
        ConnectorRegisterRequest body =
                MalformedRequest.require(request, "the request must carry a connector artifact to register");
        MalformedRequest.requireText(body.artifact(), "a base64-encoded `artifact` is required");
        byte[] artifact = decode(body.artifact());
        try {
            return registerService.register(artifact, AuthenticatedCaller.subject());
        } catch (TapstateException e) {
            // A connector-domain failure at register is the uploaded artifact's fault (client input): a 400
            // with the coded body. The same code raised on the resolve path (connection test / discovery) is
            // a server-side condition and keeps its 500 — so the status is decided here, where the context is
            // known, not globally by code. A non-connector coded error (e.g. a blocked audit) keeps its status.
            if (e.code().code().startsWith("connector.")) {
                throw new BadRequestCodedException(e);
            }
            throw e;
        }
    }

    @Verb("connector.list")
    @GetMapping("/connectors")
    ConnectorCatalogList list() {
        return new ConnectorCatalogList(catalogView.summaries());
    }

    @Verb("connector.get")
    @GetMapping("/connectors/{id}")
    ConnectorDetail get(@PathVariable("id") String id) {
        return catalogView.detail(id);
    }

    @Verb("connector.icon")
    @GetMapping("/connectors/{id}/icon")
    ResponseEntity<byte[]> icon(@PathVariable("id") String id) {
        return catalogView.icon(id)
                .map(ConnectorController::iconResponse)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private static ResponseEntity<byte[]> iconResponse(ConnectorIcon icon) {
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(icon.mediaType()))
                .body(icon.bytes());
    }

    /** Decodes the base64 artifact, refusing a non-base64 body field at the boundary as a coded 400. */
    private static byte[] decode(String base64) {
        try {
            return Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw MalformedRequest.rejecting("the `artifact` is not valid base64", e);
        }
    }
}
