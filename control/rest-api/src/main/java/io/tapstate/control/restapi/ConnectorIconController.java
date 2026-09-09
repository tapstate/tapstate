package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ConnectorIcon;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * Anonymous image projection for the connector picker. Only the icon bytes of a connector already
 * registered in the catalog are exposed; the authenticated connector REST surface remains unchanged.
 */
@Controller
class ConnectorIconController {

    private final ConnectorCatalogView catalogView;

    ConnectorIconController(ConnectorCatalogView catalogView) {
        this.catalogView = catalogView;
    }

    @GetMapping("/connector-icons/{id}")
    ResponseEntity<byte[]> icon(@PathVariable("id") String id) {
        return catalogView.icon(id)
                .map(ConnectorIconController::iconResponse)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private static ResponseEntity<byte[]> iconResponse(ConnectorIcon icon) {
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(icon.mediaType()))
                .body(icon.bytes());
    }
}
