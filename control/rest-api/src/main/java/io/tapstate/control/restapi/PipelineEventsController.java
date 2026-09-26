package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.PipelineEventsQuery;
import io.tapstate.control.core.PipelineEventsQueryService;
import io.tapstate.core.common.TapstateException;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;

/** Authenticated HTTP projection of one bounded event-history page. */
@RestController
class PipelineEventsController {

    private final PipelineEventsQueryService events;

    PipelineEventsController(PipelineEventsQueryService events) {
        this.events = events;
    }

    @Verb("pipeline.events")
    @GetMapping("/pipelines/{id}/events")
    ResponseEntity<PipelineEventsResponse> events(@PathVariable("id") String id,
            @RequestParam(name = "from", required = false) String from,
            @RequestParam(name = "to", required = false) String to,
            @RequestParam(name = "limit", required = false) String limit,
            @RequestParam(name = "cursor", required = false) String cursor) {
        PipelineEventsQuery query = new PipelineEventsQuery(id, instant(from, "from"),
                instant(to, "to"), limit(limit), cursor);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(PipelineEventsResponse.of(events.query(query)));
    }

    private static Instant instant(String value, String name) {
        if (value == null || value.isBlank()) {
            throw malformed(name + " is required");
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException invalid) {
            throw malformed(name + " must be an RFC3339 timestamp with a UTC offset");
        }
    }

    private static int limit(String value) {
        if (value == null) {
            return PipelineEventsQuery.DEFAULT_LIMIT;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException invalid) {
            throw malformed("limit must be an integer");
        }
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }
}
