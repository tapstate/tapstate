package io.tapstate.control.restapi;

import io.tapstate.control.core.SourceInput;
import io.tapstate.control.core.SourceError;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceView;
import io.tapstate.core.common.TapstateException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/** Structured JSON projection of Source CRUD onto the control service. */
@RestController
class SourceController {

    private static final String QUOTED_HASH = "\"[0-9a-f]{64}\"";

    private final SourceProjectionService sources;

    SourceController(SourceProjectionService sources) {
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Verb("source.create")
    @PostMapping("/sources")
    ResponseEntity<SourceView> create(@RequestBody SourceInput input) {
        SourceView created = sources.create(AuthenticatedCaller.subject(), input);
        return ResponseEntity.created(URI.create("/api/sources/" + created.id()))
                .eTag(created.contentHash())
                .body(created);
    }

    @Verb("source.list")
    @GetMapping("/sources")
    SourceList list() {
        return new SourceList(sources.list());
    }

    @Verb("source.get")
    @GetMapping("/sources/{id}")
    ResponseEntity<SourceView> get(@PathVariable("id") String id) {
        SourceView view = sources.get(id);
        return ResponseEntity.ok().eTag(view.contentHash()).body(view);
    }

    @Verb("source.update")
    @PutMapping("/sources/{id}")
    ResponseEntity<SourceView> replace(
            @PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @RequestBody SourceInput input) {
        SourceView replaced = sources.replace(
                AuthenticatedCaller.subject(), id, expectedHash(id, ifMatch), input);
        return ResponseEntity.ok().eTag(replaced.contentHash()).body(replaced);
    }

    @Verb("source.delete")
    @DeleteMapping("/sources/{id}")
    ResponseEntity<Void> delete(
            @PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch) {
        sources.delete(AuthenticatedCaller.subject(), id, expectedHash(id, ifMatch));
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    private static String expectedHash(String id, String ifMatch) {
        if (ifMatch == null || !ifMatch.matches(QUOTED_HASH)) {
            throw new TapstateException(SourceError.PRECONDITION_REQUIRED, Map.of("id", id), null);
        }
        return ifMatch.substring(1, ifMatch.length() - 1);
    }
}
